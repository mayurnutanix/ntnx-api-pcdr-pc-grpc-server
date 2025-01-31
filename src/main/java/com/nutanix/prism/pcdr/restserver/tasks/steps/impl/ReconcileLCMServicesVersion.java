package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.prism.base.zk.ICloseNullEnabledZkConnection;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.PcBackupSpecsProto.ServiceVersionSpecs;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.util.AlertUtil;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.IDFUtil;
import com.nutanix.prism.util.CompressionUtils;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;

@Service
@Slf4j
public class ReconcileLCMServicesVersion implements PcdrStepsHandler {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Autowired
  private EntityDBProxyImpl entityDBProxy;

  @Autowired
  private InstanceServiceFactory instanceServiceFactory;

  @Autowired
  private ICloseNullEnabledZkConnection zkClient;

  @Autowired
  private AlertUtil alertUtil;

  @Autowired
  private PCDRYamlAdapterImpl pcdrYamlAdapter;

  @Autowired
  private RestoreDataService restoreDataService;

  @Value("${prism.pcdr.service.version.path:/home/nutanix/config/prism/service_version_mapping.json}")
  private String serviceVersionConfigMappingPath;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask, ErgonTypes.Task currentSubtask)
      throws PCResilienceException, ErgonException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Reconciliation of LCM upgraded services version on recovered PC started.", currentStep);

    // ServiceVersionSpec proto list to be persisted in IDF version and mismatch info
    List<ServiceVersionSpecs.ServiceVersionSpec> updatedServiceVersionSpecList = new ArrayList<>();
    // services which are upgraded via LCM before recovery
    List<String> upgradedLCMServices = new ArrayList<>();
    StringBuilder alertMessageBuilder = new StringBuilder(PORTFOLIO_SERVICE_ALERT_MSG);
    String alertMessage = null;
    try {
      // fetch the service version spec proto from pc_backup_specs stored as bart of backup
      List<ServiceVersionSpecs.ServiceVersionSpec> serviceVersionSpecsList = PCUtil.fetchServiceVersionsListFromPCBackupSpecs(
          entityDBProxy, instanceServiceFactory.getClusterUuidFromZeusConfig());

      if (!ObjectUtils.isEmpty(serviceVersionSpecsList)) {
        // fetch the static service mapping config json bundled with pc
        JsonNode serviceMappingJsonNode = PCUtil.fetchServiceMappingJsonConfig
                                                    (serviceVersionConfigMappingPath);
        if (serviceMappingJsonNode != null && serviceMappingJsonNode.get(SERVICE_INFO) != null) {
          JsonNode serviceInfoJson = serviceMappingJsonNode.get(SERVICE_INFO);

          // set the static service_mapping_config json to map of genesis service name and version.
          Map<String, String> serviceMappingMap = StreamSupport.stream(serviceInfoJson.spliterator(), false).filter(
              data -> data.has(SERVICE_NAME) && data.has(SERVICE_VERSION)).collect(
              Collectors.toMap(data -> data.get(SERVICE_NAME).asText(), data -> data.get(SERVICE_VERSION).asText()));

          // compare the services current version and one before recovery in pc_backup_specs.
          // store the serviceVersionSpec with additional param version_mismatch in idf and
          // generate version mismatch alert on PC.
          for (ServiceVersionSpecs.ServiceVersionSpec serviceVersionSpec : serviceVersionSpecsList) {
            if (serviceMappingMap.containsKey(serviceVersionSpec.getServiceName()) &&
                  pcdrYamlAdapter.getPortfolioServicesToReconcileAfterRestore().contains(serviceVersionSpec.getServiceName())) {
              String version = serviceMappingMap.get(serviceVersionSpec.getServiceName());
              boolean versionMismatch = false;
              if (!StringUtils.equals(version, serviceVersionSpec.getServiceVersion())) {
                versionMismatch = true;
                upgradedLCMServices.add(serviceVersionSpec.getServiceName());
                // generate alert message
                alertMessageBuilder.append(
                    getAlertMsgSpec(serviceVersionSpec.getServiceName(), serviceVersionSpec.getServiceVersion(),
                                    version));
              }
              // ServiceVersion proto builder with field version mismatch
              ServiceVersionSpecs.ServiceVersionSpec updatedSpecProto = ServiceVersionSpecs.ServiceVersionSpec.
                  newBuilder().setServiceName(serviceVersionSpec.getServiceName()).
                  setServiceVersion(serviceVersionSpec.getServiceVersion())
                      .setVersionMismatch(versionMismatch)
                      .build();
              updatedServiceVersionSpecList.add(updatedSpecProto);
            }
          }
          alertMessage = alertMessageBuilder.toString();
        }
        else {
          log.info("ServiceInfo Mapping config Json is empty or not in proper format: {}", serviceMappingJsonNode);
        }
      }

      if (!ObjectUtils.isEmpty(updatedServiceVersionSpecList)) {
        // persist lcm services version data in IDF
        persistInRestoreServiceMetadata(updatedServiceVersionSpecList);
        if (!ObjectUtils.isEmpty(upgradedLCMServices)) {
          // raise alert on PC for version mismatch
          alertUtil.raiseAlertForLCMVersionMismatch(alertMessage);

          List<String> disabledServices =  pcdrYamlAdapter.getServicesToDisableOnStartupGenesis();
          disabledServices.addAll(upgradedLCMServices);
          // update services in zknode /appliance/logical/services_enablement_status
          // so that it does not start with services start
          pcdrYamlAdapter.setServicesToDisableOnStartupGenesis(disabledServices);
          restoreDataService.disableConfiguredServicesOnGenesisStartup(SERVICES_ENABLEMENT_PATH);
        }
      }
      log.info("STEP {}: Successfully reconciled LCM Services data.", currentStep);
    }
    catch (PCResilienceException e) {
      // Failed to fetch Services version specs or static config data
      log.warn("Failed to fetch backup specs data or version config mapping :", e);
    }
    catch (Exception e) {
      // If exception occurs while reconciling LCM services, just log the error and move ahead.
      // Don't throw any exception for this failure
      log.warn("Failed to reconcile LCM based services version :", e);
    }
    currentSubtask = ergonServiceHelper.updateTask(
              currentSubtask, 7,
              TaskConstants.RECONCILE_LCM_SERVICES_OVERRIDE);
    return currentSubtask;

  }

  /**
   * The method persist portfolio services version info on recovered PC . This entity will
   * be consumed later to show the version mismatch information on PE connected with recovered PC.
   *
   * @param serviceVersionSpecsList - Service version specs of services.
   */
  private void persistInRestoreServiceMetadata(List<ServiceVersionSpecs.ServiceVersionSpec> serviceVersionSpecsList) {
    InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArgForRestoreServiceVersion =
        InsightsInterfaceProto.BatchUpdateEntitiesArg.newBuilder();
    List<InsightsInterfaceProto.UpdateEntityArg> updateEntityArgList = new ArrayList<>();
    String pcUUID =
          instanceServiceFactory.getClusterUuidFromZeusConfig();
    for(ServiceVersionSpecs.ServiceVersionSpec serviceVersionSpec : serviceVersionSpecsList){
      Map<String, Object> attributesValueMap = new HashMap<>();
     ServiceVersionSpecs.ServiceVersionSpec serviceVersionSpecsProto =
            ServiceVersionSpecs.ServiceVersionSpec.newBuilder().setServiceVersion(serviceVersionSpec.getServiceVersion())
                    .setServiceName(serviceVersionSpec.getServiceName()).build();
       attributesValueMap.put(
          Constants.ZPROTOBUF,
          CompressionUtils.compress(serviceVersionSpecsProto.toByteString()));

       InsightsInterfaceProto.UpdateEntityArg.Builder updateEntityArgBuilder =
          IDFUtil.constructUpdateEntityArgBuilder(
              Constants.PC_RESTORE_SERVICE_VERSION_METADATA,
              PCUtil.getObjectStoreEndpointUuid(
                  pcUUID,serviceVersionSpec.getServiceName()),
              // Entity Id being set as random uuid for service name.
              attributesValueMap);
      updateEntityArgList.add(updateEntityArgBuilder.build());
    }
    batchUpdateEntitiesArgForRestoreServiceVersion.addAllEntityList(updateEntityArgList);
    try {
      IDFUtil.getErrorListAfterBatchUpdateRPC(batchUpdateEntitiesArgForRestoreServiceVersion,entityDBProxy);
    } catch (InsightsInterfaceException e) {
      log.error("Failed to update service version metadata after restore :", e);
    }
  }

  /**
   * The method creates the alert msg to be shown on PC UI
   *
   * @param serviceName     - LCM based service with version mismatch.
   * @param requiredVersion - Required version of service on restored PC.
   * @param currentVersion  - Current version on restored PC.
   * @return - returns alert msg string in format.
   */
  private String getAlertMsgSpec(String serviceName, String requiredVersion, String currentVersion) {
    return String.format("%s (Required: %s, Installed: %s) \n", serviceName, requiredVersion, currentVersion);
  }
}