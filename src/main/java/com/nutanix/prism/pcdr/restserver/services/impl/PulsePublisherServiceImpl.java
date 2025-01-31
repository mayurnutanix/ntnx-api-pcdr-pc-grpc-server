package com.nutanix.prism.pcdr.restserver.services.impl;

import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.insights.ifc.InsightsInterfaceProto.UpdateEntityArg;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.EntityCalculationException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.services.api.BackupEntitiesCalculator;
import com.nutanix.prism.pcdr.restserver.services.api.PulsePublisherService;
import com.nutanix.prism.pcdr.restserver.util.FlagHelperUtil;
import com.nutanix.prism.pcdr.restserver.util.PCRetryHelper;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.restserver.util.QueryUtil;
import com.nutanix.prism.pcdr.util.IDFUtil;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.*;

import static com.nutanix.prism.pcdr.constants.Constants.CLUSTER_VERSION;
import static com.nutanix.prism.pcdr.constants.Constants.NUM_NODES;
import static com.nutanix.prism.pcdr.messages.Messages.PAUSE_BACKUP_EMPTY_MESSAGE;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;

/**
 * Service to handle regular pulse updates and pulse updates related to workflow
 */
@Slf4j
@Service
public class PulsePublisherServiceImpl implements PulsePublisherService {

  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private PCRetryHelper pcRetryHelper;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private BackupEntitiesCalculator backupEntitiesCalculator;
  @Autowired
  private FlagHelperUtil flagHelperUtil;

  /**
   * Send metric data to pulse. The metric data is updated in all the
   * Prism Element replica entities.
   * And add the object store endpoint data in case it was missed while adding
   * the replica.
   */
  public void sendMetricDataToPulse() {
    List<String> replicaPEUuids = null;
    List<PcObjectStoreEndpoint> pcObjectStoreEndpoints =
        Collections.emptyList();
    try {
      replicaPEUuids = pcRetryHelper
          .fetchExistingClusterUuidInPCBackupConfig();
      pcObjectStoreEndpoints =
          pcRetryHelper.fetchExistingObjectStoreEndpointInPCBackupConfig();
    }
    catch (PCResilienceException e) {
      log.error("Unable to fetch cluster data for sending data to pulse");
      return;
    }
    log.debug("Sending data to IDF for pulse");
    String pcClusterUuid = "";
    try {
      List<InsightsInterfaceProto.MetricData> metricDataFromIDF =
          getMetricDataFromIDF();
      pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
      entityDBProxy
          .putMetricData(getPutMetricDataArg(pcClusterUuid, metricDataFromIDF));
    } catch (InsightsInterfaceException e) {
      String warnMessage = String.format(
          "Unable to send pulse metrics data to the " +
          "insights DB for PCDR clusterUuid - %s", pcClusterUuid);
      log.warn(warnMessage, e);
    } catch (Exception e) {
      log.warn("Unexpected error encountered while updating pulse metrics " +
                "for PC pulse metrics.", e);
    }
    try {
      updatePeMetadataInPulse(replicaPEUuids);
      updateObjectStoreEndpointInPulse(pcObjectStoreEndpoints);
    }
    catch (Exception e) {
      String warnMessage = "Unable to send pulse metrics data to the" +
                           " insights DB for PCDR.";
      log.warn(warnMessage, e);
    }
  }

  /**
   * List of object store endpoints to update the endpoint address and
   * endpoint flavour.
   * @param objectStoreEndpoints - object store endpoints which are required
   *                             to be updated in pulse configuration.
   * @throws InsightsInterfaceException - can throw insights interface
   * exception while making batch update request.
   */
  void updateObjectStoreEndpointInPulse(
      List<PcObjectStoreEndpoint> objectStoreEndpoints)
      throws InsightsInterfaceException {
    if (ObjectUtils.isEmpty(objectStoreEndpoints)) {
      log.info("No Object store endpoint is added as part of backup yet.");
      return;
    }
    InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder
        batchUpdateEntitiesArg =
        InsightsInterfaceProto.BatchUpdateEntitiesArg.newBuilder();
    List<UpdateEntityArg> updateEntityArgList =
        getObjectStoreUpdateEntityArgList(objectStoreEndpoints);
    batchUpdateEntitiesArg.addAllEntityList(updateEntityArgList);
    List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
        batchUpdateEntitiesArg,
        entityDBProxy);
    if (CollectionUtils.isNotEmpty(errorList)) {
      log.warn(errorList.toString());
    }
  }

  /**
   * Given replicaPEuuids, update the version, number of nodes of the PE in
   * the IDF pulse table.
   * @param replicaPEUuids - replica PE uuids
   * @throws InsightsInterfaceException - can be thrown by IDF
   */
  void updatePeMetadataInPulse(List<String> replicaPEUuids)
      throws InsightsInterfaceException {
    if (CollectionUtils.isEmpty(replicaPEUuids)) {
      log.info("No Prism Element replica is added as part of backup yet.");
      return;
    }
    // Map of replicaPEUuid against a pair of <version, num_node>
    Map<String, Pair<String, Long>> peVersionNodeCountMap = new HashMap<>();
    InsightsInterfaceProto.GetEntitiesWithMetricsRet ret =
        PCUtil.getClusterInfoFromIDf(replicaPEUuids, entityDBProxy);
    ret.getGroupResultsListList()
       .forEach(
           queryGroupResult ->
               queryGroupResult
                   .getLookupQueryResultsList()
                   .forEach(
                       entity -> {
                         String peVersion =
                             IDFUtil.getAttributeValueInEntityMetric(
                                        CLUSTER_VERSION,
                                        entity.getEntityWithMetrics())
                                    .getStrValue();
                         long peNumNodes =
                             IDFUtil.getAttributeValueInEntityMetric(
                                        NUM_NODES,
                                        entity.getEntityWithMetrics())
                                    .getInt64Value();
                         peVersionNodeCountMap.put(
                             entity.getEntityWithMetrics().getEntityGuid()
                                   .getEntityId(),
                             Pair.of(peVersion, peNumNodes));
                       }));
    InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder
        batchUpdateEntitiesArg =
        InsightsInterfaceProto.BatchUpdateEntitiesArg.newBuilder();
    for (String replicaPEUuid : replicaPEUuids) {
      Map<String, Object> attributesValueMap = new HashMap<>();
      Pair<String, Long> versionNodeCountPair =
          peVersionNodeCountMap.get(replicaPEUuid);
      attributesValueMap.put(PULSE_METRICS_VERSION,
                             versionNodeCountPair.getLeft());
      attributesValueMap.put(PULSE_METRICS_NUM_NODES,
                             versionNodeCountPair.getRight());
      UpdateEntityArg.Builder updateEntityArgBuilder =
          IDFUtil.constructUpdateEntityArgBuilder(Constants.PULSE_DATA_TABLE,
                                                  replicaPEUuid,
                                                  attributesValueMap);
      updateEntityArgBuilder.setFullUpdate(false);
      batchUpdateEntitiesArg.addEntityList(updateEntityArgBuilder);
    }
    List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
        batchUpdateEntitiesArg,
        entityDBProxy);
    if (CollectionUtils.isNotEmpty(errorList)) {
      log.warn(errorList.toString());
    }
  }

  /**
   * Get the count of PE clusters managed by the PC
   *
   * @return Integer count of PE clusters managed by the PC
   */
  long getPeClustersManagedCount() {
    long peCount = 0;
    InsightsInterfaceProto.GetEntitiesWithMetricsRet result = null;
    try {
      result = entityDBProxy
          .getEntitiesWithMetrics(InsightsInterfaceProto
                                      .GetEntitiesWithMetricsArg
                                      .newBuilder().setQuery(QueryUtil.constructPEClusterCountQuery())
                                      .build());
      if (result.getGroupResultsListCount() > 0) {
        peCount = result.getGroupResultsList(0).getTotalEntityCount();
      }
    }
    catch (InsightsInterfaceException e) {
      log.warn("Querying PE data from IDF failed for pulse ", e);
    }
    log.debug("Successfully retrieved PE cluster List from IDF for Pulse "
              + peCount);
    return peCount;
  }

  /**
   * Get the count of VMs managed by registered PE clusters on a PC
   *
   * @return Integer count of VMs
   */
  long getVmsManagedCount() {
    InsightsInterfaceProto.GetEntitiesWithMetricsArg.Builder argBuilder =
        InsightsInterfaceProto.GetEntitiesWithMetricsArg.newBuilder();

    final InsightsInterfaceProto.Query.Builder query =
        InsightsInterfaceProto.Query.newBuilder();
    // Set the name of the query.
    query.setQueryName("prism:pc_dr_pulse_data");
    // Add the entity type for the query.
    query.addEntityList(InsightsInterfaceProto.EntityGuid.newBuilder()
                                                         .setEntityTypeName(Constants.VM_TABLE));
    // Flag to fetch only the count of entities
    query.setFlags(2);
    long vmsCount = 0L;
    try {
      InsightsInterfaceProto.GetEntitiesWithMetricsRet result =
          entityDBProxy
              .getEntitiesWithMetrics(argBuilder.setQuery(query.build())
                                                .build());

      if (result.getGroupResultsListCount() > 0) {
        vmsCount = result.getGroupResultsList(0).getTotalEntityCount();
      }
      log.debug("Successfully retrieved VMs managed count: " + vmsCount);
      return vmsCount;
    }
    catch (InsightsInterfaceException e) {
      log.warn("Exception received while fetching the VMs count for pulse " +
               "data update ", e);
    }
    return vmsCount;
  }

  /**
   * Constructs the PutMetricDataArg for metric data sent to pulse
   *
   * @return PutMetricDataArg for the metric data
   */
  private InsightsInterfaceProto.PutMetricDataArg getPutMetricDataArg(
      String clusterUuid, List<InsightsInterfaceProto.MetricData>
      metricDataList) {
    InsightsInterfaceProto.PutMetricDataArg.Builder putMetricDataArgBuilder =
        InsightsInterfaceProto.PutMetricDataArg.newBuilder();
    InsightsInterfaceProto.EntityGuid.Builder entityGuidBuilder =
        InsightsInterfaceProto.EntityGuid.newBuilder();
    entityGuidBuilder.setEntityTypeName(Constants.PULSE_DATA_TABLE)
                     .setEntityId(clusterUuid);

    InsightsInterfaceProto.EntityWithMetric.Builder entityWithMetricBuilder =
        InsightsInterfaceProto.EntityWithMetric.newBuilder();
    entityWithMetricBuilder.setEntityGuid(entityGuidBuilder.build());
    entityWithMetricBuilder.addAllMetricDataList(metricDataList);

    putMetricDataArgBuilder
        .addEntityWithMetricList(entityWithMetricBuilder.build());

    return putMetricDataArgBuilder.build();
  }

  /**
   * Creates a list of metric data consisting of total_pe_clusters_managed_by_pc
   * and total_vms_managed_by_pc data to send it to pulse
   *
   * @return List of metric data
   */
  private List<InsightsInterfaceProto.MetricData> getMetricDataFromIDF() {
    LinkedHashMap<String, Long> entityMap = new LinkedHashMap<>();
    entityMap.put(PULSE_METRICS_TOTAL_VMS_MANAGED_BY_PC, getVmsManagedCount());
    entityMap.put(PULSE_METRICS_TOTAL_PE_CLUSTERS_MANAGED_BY_PC,
                  getPeClustersManagedCount());
    if (flagHelperUtil.isBackupLimitFeatEnabled()) {
      try {
        if (backupEntitiesCalculator.getBackupEntityList().isEmpty()) {
          backupEntitiesCalculator.updateBackupEntityList();
        }
        long entitiesToBackupCount =
            backupEntitiesCalculator.getTotalBackupEntityCount();
        entityMap.put(PULSE_METRICS_TOTAL_BACKUP_ENTITIES_COUNT,
                      entitiesToBackupCount);
      } catch (EntityCalculationException e) {
        log.error("Error encountered while fetching total entities to backup " +
                  "count.", e);
      }
    }

    long timeValue = Instant.now().toEpochMilli() * 1000;
    List<InsightsInterfaceProto.MetricData> metricDataList = new ArrayList<>();

    for (Map.Entry<String, Long> keyValue : entityMap.entrySet()) {
      InsightsInterfaceProto.DataValue dataValue =
          InsightsInterfaceProto.DataValue.newBuilder()
                                          .setInt64Value(keyValue.getValue()).build();
      InsightsInterfaceProto.TimeValuePair timeValuePair =
          InsightsInterfaceProto.TimeValuePair.newBuilder().setValue(dataValue)
                                              .setTimestampUsecs(timeValue).build();
      InsightsInterfaceProto.MetricData metricData =
          InsightsInterfaceProto.MetricData.newBuilder()
                                           .setName(keyValue.getKey()).addValueList(timeValuePair).build();
      metricDataList.add(metricData);
    }
    return metricDataList;
  }

  /**
   * Add the replica cluster uuids into IDF for pulse tracking during
   * add-replica
   *
   * @param peClusterUuidSet - Set of pe cluster uuid
   */
  @Async("adonisServiceThreadPool")
  public void insertReplicaData(Set<String> peClusterUuidSet,
                                List<PcObjectStoreEndpoint> objectStoreEndpoints) {
    try {
      log.debug("Sending data to IDF for pulse");
      InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder
          batchUpdateEntitiesArg =
          InsightsInterfaceProto.BatchUpdateEntitiesArg.newBuilder();
      List<UpdateEntityArg> updateEntityArgList =
          new ArrayList<>();

      long currentEpochTime = Instant.now().toEpochMilli() * 1000;
      for (String clusterUuid : peClusterUuidSet) {
        Map<String, Object> attributesValueMap = new HashMap<>();
        attributesValueMap.put(PULSE_METRICS_BACKUP_START_TIMESTAMP,
                               currentEpochTime);

        UpdateEntityArg.Builder updateEntityArgBuilder =
            IDFUtil.constructUpdateEntityArgBuilder(Constants.PULSE_DATA_TABLE,
                                                    clusterUuid,
                                                    attributesValueMap);
        updateEntityArgList.add(updateEntityArgBuilder.build());
      }
      String pcUUID = instanceServiceFactory.getClusterUuidFromZeusConfig();
      for (PcObjectStoreEndpoint pcObjectStoreEndpoint: objectStoreEndpoints) {
        Map<String, Object> attributesValueMap = new HashMap<>();
        attributesValueMap.put(PULSE_METRICS_BACKUP_START_TIMESTAMP,
                               currentEpochTime);
        attributesValueMap.put(PULSE_ENDPOINT_ADDRESS_ATTRIBUTE,
                               pcObjectStoreEndpoint.getEndpointAddress());
        attributesValueMap.put(PULSE_PC_ENDPOINT_FLAVOUR_ATTRIBUTE,
                               pcObjectStoreEndpoint.getEndpointFlavour()
                                                    .fromEnum());
        String entityId = PCUtil.getObjectStoreEndpointUuid(
            pcUUID, pcObjectStoreEndpoint.getEndpointAddress());
        UpdateEntityArg.Builder updateEntityArgBuilder =
            IDFUtil.constructUpdateEntityArgBuilder(Constants.PULSE_DATA_TABLE,
                                                    entityId,
                                                    attributesValueMap);
        updateEntityArgList.add(updateEntityArgBuilder.build());
      }

      batchUpdateEntitiesArg.addAllEntityList(updateEntityArgList);

      List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
          batchUpdateEntitiesArg,
          entityDBProxy);
      if (CollectionUtils.isNotEmpty(errorList)) {
        log.warn(errorList.toString());
      }
    }
    catch (Exception e) {
      log.warn("Unable to update idf table for pulse during add replica ", e);
    }
  }

  /**
   * The method to create object store update entity args from endpoint
   * address and endpoint flavour
   * @param objectStoreEndpoints - list containing object store endpoints
   *                             details.
   * @return - returns list of update entity args.
   */
  private List<UpdateEntityArg> getObjectStoreUpdateEntityArgList(
      List<PcObjectStoreEndpoint> objectStoreEndpoints) {
    List<UpdateEntityArg> updateEntityArgList = new ArrayList<>();
    String pcUUID = instanceServiceFactory.getClusterUuidFromZeusConfig();
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint: objectStoreEndpoints) {
      Map<String, Object> attributesValueMap = new HashMap<>();
      attributesValueMap.put(PULSE_ENDPOINT_ADDRESS_ATTRIBUTE,
                             pcObjectStoreEndpoint.getEndpointAddress());
      attributesValueMap.put(PULSE_PC_ENDPOINT_FLAVOUR_ATTRIBUTE,
                             pcObjectStoreEndpoint.getEndpointFlavour()
                                                  .fromEnum());
      String entityId = PCUtil.getObjectStoreEndpointUuid(
          pcUUID, pcObjectStoreEndpoint.getEndpointAddress());
      UpdateEntityArg.Builder updateEntityArgBuilder =
          IDFUtil.constructUpdateEntityArgBuilder(Constants.PULSE_DATA_TABLE,
                                                  entityId,
                                                  attributesValueMap);
      updateEntityArgBuilder.setFullUpdate(false);
      updateEntityArgList.add(updateEntityArgBuilder.build());
    }
    return updateEntityArgList;
  }

  /**
   * Remove the replica entity id from IDF for pulse tracking during
   * remove-replica
   *
   * @param replicaEntityId - replica entity Id present in IDF.
   */
  @Async("adonisServiceThreadPool")
  public void removeReplicaData(String replicaEntityId) {
    InsightsInterfaceProto.DeleteEntityArg.Builder deleteEntityArgBuilder =
        InsightsInterfaceProto.DeleteEntityArg.newBuilder();

    try {
      InsightsInterfaceProto.EntityGuid.Builder entityGuidBuilder =
          InsightsInterfaceProto.EntityGuid.newBuilder();
      // Set entityType and entityId
      entityGuidBuilder.setEntityTypeName(Constants.PULSE_DATA_TABLE);
      entityGuidBuilder.setEntityId(replicaEntityId);
      deleteEntityArgBuilder.setEntityGuid(entityGuidBuilder.build());
      InsightsInterfaceProto.DeleteEntityRet deleteEntityRet =
          entityDBProxy
              .deleteEntity(deleteEntityArgBuilder.build());
      if (deleteEntityRet == null || !deleteEntityRet.hasEntity()) {
        String message = "Unable to delete the entity from pulse table" +
                         " during removing replica for " + replicaEntityId;
        log.warn(message);
      }
    }
    catch (Exception e) {
      log.warn("Unable to update idf table for pulse during remove replica ", e);
    }
  }

  /**
   * Method responsible for updating the pulse metrics when the backup is
   * paused.
   *
   * @param pcBackupConfigs - list of pc backup configs.
   */
  @Async("adonisServiceThreadPool")
  public void updatePauseBackupStatus(List<PcBackupConfig> pcBackupConfigs) {
    try {
      log.debug("Sending data to IDF for pulse backup pause status update.");
      if (pcBackupConfigs.isEmpty()) {
        log.info("PE info list is empty, not proceeding with pulse update.");
        return;
      }
      InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder
          batchUpdateEntitiesArg =
          InsightsInterfaceProto.BatchUpdateEntitiesArg.newBuilder();
      // Get the current epoch time to tell when the backup was paused.
      long currentEpochTime = Instant.now().toEpochMilli() * 1000;
      String pcUUID = instanceServiceFactory.getClusterUuidFromZeusConfig();
      for (PcBackupConfig pcBackupConfig : pcBackupConfigs) {
        Map<String, Object> attributesValueMap = new HashMap<>();
        // Backup paused message to "" indicating backup is not paused
        // (default value).
        String backupPausedMessage = PAUSE_BACKUP_EMPTY_MESSAGE;
        // Backup paused timestamp to 0 indicating backup is not paused
        // (default value).
        long backupPausedTimestamp = 0;
        if (pcBackupConfig.getPauseBackup()) {
          backupPausedMessage = pcBackupConfig.getPauseBackupMessage();
          backupPausedTimestamp = currentEpochTime;
        }

        attributesValueMap.put(PULSE_METRIC_BACKUP_PAUSED_TIMESTAMP,
                               backupPausedTimestamp);
        attributesValueMap.put(PULSE_METRICS_BACKUP_PAUSED_MESSAGE,
                               backupPausedMessage);
        if (!StringUtils.isEmpty(pcBackupConfig.getClusterUuid())) {
          UpdateEntityArg.Builder updateEntityArgBuilder =
              IDFUtil.constructUpdateEntityArgBuilder(
                  Constants.PULSE_DATA_TABLE,
                  pcBackupConfig.getClusterUuid(),
                  attributesValueMap);

          updateEntityArgBuilder.setFullUpdate(false);
          batchUpdateEntitiesArg.addEntityList(updateEntityArgBuilder);
        } else {
          String endpointAddress =
              pcBackupConfig.getObjectStoreEndpoint().getEndpointAddress();
          if (StringUtils.isEmpty(endpointAddress)) {
            log.error("Invalid PC backup config provided. Endpoint Address " +
                      "and cluster uuid both are empty.");
          } else {
            String entityId = PCUtil.getObjectStoreEndpointUuid(
                pcUUID, endpointAddress);
            UpdateEntityArg.Builder updateEntityArgBuilder =
                IDFUtil.constructUpdateEntityArgBuilder(
                    Constants.PULSE_DATA_TABLE,
                    entityId,
                    attributesValueMap);
            updateEntityArgBuilder.setFullUpdate(false);
            batchUpdateEntitiesArg.addEntityList(updateEntityArgBuilder);
          }
        }
      }

      List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
          batchUpdateEntitiesArg,
          entityDBProxy);
      if (CollectionUtils.isNotEmpty(errorList)) {
        log.warn(errorList.toString());
      }
    } catch (Exception e) {
      log.warn("Unable to update idf table for pulse during pause backup " +
               "status change.", e);
    }
  }
}
