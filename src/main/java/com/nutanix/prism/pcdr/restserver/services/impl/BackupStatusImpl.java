package com.nutanix.prism.pcdr.restserver.services.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto.BatchUpdateEntitiesArg;
import com.nutanix.insights.ifc.InsightsInterfaceProto.GetEntitiesWithMetricsRet;
import com.nutanix.insights.ifc.InsightsInterfaceProto.UpdateEntityArg;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.EntityCalculationException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.messages.Messages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.services.api.BackupEntitiesCalculator;
import com.nutanix.prism.pcdr.restserver.services.api.BackupStatus;
import com.nutanix.prism.pcdr.restserver.util.AlertUtil;
import com.nutanix.prism.pcdr.restserver.util.PCRetryHelper;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.IDFUtil;
import com.nutanix.prism.util.CompressionUtils;
import dp1.pri.prism.v4.protectpc.EligibleCluster;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.CLUSTER_UUID_PC_BACKUP_CONFIG;
import static com.nutanix.prism.pcdr.constants.Constants.ZPROTOBUF;
import static com.nutanix.prism.pcdr.messages.Messages.PAUSE_BACKUP_EMPTY_MESSAGE;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;

@Slf4j
@Component
public class BackupStatusImpl implements BackupStatus {

  private final EntityDBProxyImpl entityDbProxy;

  private final AlertUtil alertUtil;

  private final BackupEntitiesCalculator backupEntitiesCalculator;

  private final PulsePublisherServiceImpl pulsePublisherService;

  private final InstanceServiceFactory instanceServiceFactory;
  private final PCRetryHelper pcRetryHelper;

  @Autowired
  public BackupStatusImpl(EntityDBProxyImpl entityDbProxy,
                          AlertUtil alertUtil,
                          BackupEntitiesCalculator backupEntitiesCalculator,
                          PulsePublisherServiceImpl pulsePublisherService,
                          InstanceServiceFactory instanceServiceFactory,
                          PCRetryHelper pcRetryHelper) {
    this.entityDbProxy = entityDbProxy;
    this.alertUtil = alertUtil;
    this.backupEntitiesCalculator = backupEntitiesCalculator;
    this.pulsePublisherService = pulsePublisherService;
    this.instanceServiceFactory = instanceServiceFactory;
    this.pcRetryHelper = pcRetryHelper;
  }

  /**
   * Method to fetch PcBackupConfig proto decompress it and return it as a list
   * @return - List of PcBackupConfig proto
   * @throws PCResilienceException - can throw backup exception while fetching it.
   */
  public List<PcBackupConfig> fetchPcBackupConfigProtoListForPEReplicas()
      throws PCResilienceException {
    GetEntitiesWithMetricsRet result =
        PCUtil.fetchPCBackupConfigWithSpecificAttrSetFromIDF(
            entityDbProxy, CLUSTER_UUID_PC_BACKUP_CONFIG);
    if (ObjectUtils.isEmpty(result)) {
      log.warn("The received result from pc_backup_config is either empty " +
                "or null - {}", result);
      return Collections.emptyList();
    }
    return getPcBackupConfigProtoList(result);
  }

  /**
   * From the given entity with metrics ret in IDF retrieve PC backup config
   * proto list.
   *
   * @param queryResult - entity with metrics result
   * @return - returns a list of pc_backup_config proto
   */
  private List<PcBackupConfig> getPcBackupConfigProtoList(
      GetEntitiesWithMetricsRet queryResult) {
    List<PcBackupConfig> pcBackupConfigList = new ArrayList<>();
    queryResult.getGroupResultsListList()
          .forEach(queryGroupResult -> queryGroupResult
              .getRawResultsList()
              .forEach(entityWithMetric -> {
                try {
                  final PcBackupConfig pcBackupConfig =
                      PcBackupConfig.parseFrom(CompressionUtils.decompress(
                          Objects.requireNonNull(
                              IDFUtil.getAttributeValueInEntityMetric(
                                  ZPROTOBUF, entityWithMetric).getBytesValue()
                                                )));
                  pcBackupConfigList.add(pcBackupConfig);
                }
                catch (InvalidProtocolBufferException e) {
                  String error = "Unable to convert bytes value to " +
                                 "PcBackupConfig proto.";
                  log.error(error, e);
                }
              }));
    return pcBackupConfigList;
  }

  /**
   * This method is responsible for fetching all the entities present as part
   * of pc_backup_config, and returning a list of PcBackupConfig proto.
   *
   * @return - returns list of pc backup config proto
   * @throws PCResilienceException - can throw PCbackupException
   */
  public List<PcBackupConfig> fetchPcBackupConfigProtoList()
      throws PCResilienceException {
    GetEntitiesWithMetricsRet result =
        PCUtil.fetchPCBackupConfig(entityDbProxy);
    return getPcBackupConfigProtoList(result);
  }

  /**
   * Function to set backup sync for specific PE list. It pauses or resumes
   * the backup sync for the specific list of replica PE's
   * While calling this method take care of acquiring appropriate lock.
   *
   * @param replicaUuids - List of replica PE on which backup needs to be
   *                       paused/resumed
   * @param pauseBackup - Boolean to represent pause or resume of backup.
   */
  public void setBackupStatusForReplicaPEs(List<String> replicaUuids,
                                          boolean pauseBackup,
                                          String pauseBackupMessage)
      throws InsightsInterfaceException, PCResilienceException {
    if (replicaUuids == null || replicaUuids.isEmpty()) {
      // Return incase there is no update required from replicaPEUuid.
      return;
    }
    BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg =
        BatchUpdateEntitiesArg.newBuilder();

    List<PcBackupConfig> latestPcBackupConfigs = new ArrayList<>();

    for (String replicaUuid: replicaUuids) {
      latestPcBackupConfigs.add(
          PcBackupConfig.newBuilder()
                        .setClusterUuid(replicaUuid)
                        .setPauseBackup(pauseBackup)
                        .setPauseBackupMessage(pauseBackupMessage)
                        .build());
    }

    // Get UpdateArg to update the Database to pause backup.
    List<UpdateEntityArg> updateEntityArgList =
        createUpdateEntityArgList(latestPcBackupConfigs);
    log.info("Setting pause backup field to {} for PE Replicas",
             pauseBackup);
    log.debug(
        "Batch updating pc backup config table with following entries: {}",
        updateEntityArgList);
    batchUpdateEntitiesArg.addAllEntityList(updateEntityArgList);
    List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
        batchUpdateEntitiesArg, entityDbProxy);
    if (!errorList.isEmpty()) {
      log.error(errorList.toString());
      throw new PCResilienceException(ErrorMessages.DATABASE_ERROR_PAUSE_BACKUP,
              ErrorCode.PCBR_INTERNAL_SERVER_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    pulsePublisherService.updatePauseBackupStatus(latestPcBackupConfigs);
  }

  /**
   * Function to set backup sync for specific PE list. It pauses or resumes
   * the backup sync for the specific list of replica PE's
   * While calling this method take care of acquiring appropriate lock.
   *
   * @param objectStoreEndpointInPCBackupConfig - List of object store replicas
   *    in which backup needs to be paused/resumed
   * @param pauseBackup - Boolean to represent pause or resume of backup.
   */
  public void setBackupStatusForObjectStoreReplicas(
          List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig,
          boolean pauseBackup,
          String pauseBackupMessage)
    throws InsightsInterfaceException, PCResilienceException {

    if (objectStoreEndpointInPCBackupConfig == null ||
        objectStoreEndpointInPCBackupConfig.isEmpty()) {
      // Return incase there is no update required from object stores endpoints.
      return;
    }
    BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg =
            BatchUpdateEntitiesArg.newBuilder();

    List<PcBackupConfig> latestPcBackupConfigs = new ArrayList<>();

    for (PcObjectStoreEndpoint pcObjectStoreEndpoint: objectStoreEndpointInPCBackupConfig) {
      // pc_backup_config uuid is create via the combination of pc_cluster_uuid
      // and endpoint_address.
      String pcBackupConfigUUID = PCUtil.getObjectStoreEndpointUuid(
        instanceServiceFactory.getClusterUuidFromZeusConfig(), pcObjectStoreEndpoint.getEndpointAddress());
      PcBackupConfig pcBackupConfig = PCUtil.getPcBackupConfigById(pcBackupConfigUUID, pcRetryHelper);

      latestPcBackupConfigs.add(
        PcBackupConfig.newBuilder(pcBackupConfig)
          .setPauseBackup(pauseBackup)
          .setPauseBackupMessage(pauseBackupMessage)
          .build());
    }

    // Get UpdateArg to update the Database to pause backup.
    List<UpdateEntityArg> updateEntityArgList =
            createUpdateEntityArgListForObjectStoreReplicas(latestPcBackupConfigs);
    log.info("Setting pause backup field to {} for ObjectStore Replicas",
            pauseBackup);
    log.debug(
            "Batch updating pc backup config table with following entries: {}",
            updateEntityArgList);
    batchUpdateEntitiesArg.addAllEntityList(updateEntityArgList);
    List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
            batchUpdateEntitiesArg, entityDbProxy);
    if (!errorList.isEmpty()) {
      log.error(errorList.toString());
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_DATABASE_WRITE_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    pulsePublisherService.updatePauseBackupStatus(latestPcBackupConfigs);
  }

  /**
   * Function to set backup Sync with retries. It pauses or un pauses the
   * backup sync. While calling this method take care of acquiring
   * appropriate lock.
   * @param pauseBackup - Boolean to represent pause of unpause of backup.
   */
  @Retryable(value = {PCResilienceException.class, InsightsInterfaceException.class},
      maxAttemptsExpression = "#{${prism.pcdr.pauseBackupMaxRetryAttempts:3}}",
      backoff =
      @Backoff(delayExpression = "#{${prism.pcdr.pauseBackupDelay:10000}}"))
  public void setBackupStatusForAllReplicaPEsWithRetries(List<String> replicaUuids,
                                                         boolean pauseBackup,
                                                         String pauseBackupMessage)
      throws InsightsInterfaceException, PCResilienceException {
    log.info("Setting up backup status to {} for replica PEs.", pauseBackup);
    setBackupStatusForReplicaPEs(replicaUuids, pauseBackup, pauseBackupMessage);
  }

  /**
   * Function to set object store backup Sync with retries. It pauses or
   * un pauses the backup sync. While calling this method take care of acquiring
   * appropriate lock.
   * @param pauseBackup - Boolean to represent pause of unpause of backup.
   */
  @Retryable(value = {PCResilienceException.class, InsightsInterfaceException.class},
          maxAttemptsExpression = "#{${prism.pcdr.pauseBackupMaxRetryAttempts:3}}",
          backoff =
          @Backoff(delayExpression = "#{${prism.pcdr.pauseBackupDelay:10000}}"))
  public void setBackupStatusForAllObjectStoreReplicasWithRetries(
          List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig,
          boolean pauseBackup,
          String pauseBackupMessage)
    throws InsightsInterfaceException, PCResilienceException {

    log.info("Setting up backup status to {} for ObjectStore replicas.",
            pauseBackup);
    setBackupStatusForObjectStoreReplicas(
            objectStoreEndpointInPCBackupConfig, pauseBackup, pauseBackupMessage);
  }

  /**
   * Function to create Update Entity Arg which is used for batch update of Pc Backup
   * config.
   * @param pcBackupConfigs - List of PcBackupConfig of replica clusters.
   * @return - UpdateEntityArg List containing PcBackupConfig.
   */
  List<UpdateEntityArg> createUpdateEntityArgList(
      List<PcBackupConfig> pcBackupConfigs){
    List<UpdateEntityArg> updateEntityArgList =
        new ArrayList<>();
    for (PcBackupConfig pcBackupConfig: pcBackupConfigs) {
      Map<String, Object> attributesValueMap = new HashMap<>();
      attributesValueMap.put(Constants.ZPROTOBUF,
                             CompressionUtils.compress(
                                 pcBackupConfig.toByteString()));
      UpdateEntityArg.Builder updateEntityArgBuilder =
          IDFUtil.constructUpdateEntityArgBuilder(
              Constants.PC_BACKUP_CONFIG,
              pcBackupConfig.getClusterUuid(), // Entity Id being set as PE cluster uuid.
              attributesValueMap);
      log.debug("Update Entity Arg builder for pe cluster Uuid:{} is {}",
                pcBackupConfig.getClusterUuid(), updateEntityArgBuilder);
      updateEntityArgList.add(updateEntityArgBuilder.build());
    }
    return updateEntityArgList;
  }

  /**
   * Function to create Update Entity Arg which is used for batch update of Pc Backup
   * config.
   * @param pcBackupConfigs - List of PcBackupConfig of replica clusters.
   * @return - UpdateEntityArg List containing PcBackupConfig.
   */
  public List<UpdateEntityArg> createUpdateEntityArgListForObjectStoreReplicas(
          List<PcBackupConfig> pcBackupConfigs){
    List<UpdateEntityArg> updateEntityArgList =
            new ArrayList<>();
    // pc_backup_config uuid is create via the combination of pc_cluster_uuid
    // and endpoint_address.
    String pcUUID =
            instanceServiceFactory.getClusterUuidFromZeusConfig();
    for (PcBackupConfig pcBackupConfig: pcBackupConfigs) {
      String pcBackupConfigUUID = PCUtil.getObjectStoreEndpointUuid(
          pcUUID, pcBackupConfig.getObjectStoreEndpoint().getEndpointAddress());
      Map<String, Object> attributesValueMap = new HashMap<>();
      attributesValueMap.put(Constants.ZPROTOBUF,
              CompressionUtils.compress(
                      pcBackupConfig.toByteString()));
      UpdateEntityArg.Builder updateEntityArgBuilder =
              IDFUtil.constructUpdateEntityArgBuilder(
                      Constants.PC_BACKUP_CONFIG,
                      pcBackupConfigUUID,
                      attributesValueMap);
      log.debug("Update Entity Arg builder for ObjectStore replica Uuid:{} is {}",
                 pcBackupConfigUUID, updateEntityArgBuilder);
      updateEntityArgList.add(updateEntityArgBuilder.build());
    }
    return updateEntityArgList;
  }
  /**
   * Check based on number of entities and upgrade flow whether the backup is
   * supposed to be paused or not. Execute the same and return the final
   * status.
   * @throws InsightsInterfaceException - can throw InsightsInterfaceException
   * @throws PCResilienceException - can throw PCResilienceException
   */
  @Override
  public void updateReplicaPEsPauseBackupStatus()
      throws InsightsInterfaceException, PCResilienceException {
    List<PcBackupConfig> replicaPePcBackupConfigs =
        fetchPcBackupConfigProtoListForPEReplicas();
    List<String> replicaPEUuids =
        replicaPePcBackupConfigs.stream().map(PcBackupConfig::getClusterUuid)
                       .collect(Collectors.toList());
    try {
      backupEntitiesCalculator.updateBackupEntityList();
      List<EligibleCluster> backupLimitExceededClusters =
          backupEntitiesCalculator.getBackupLimitExceededPes(replicaPEUuids);

      List<String> backupPausedReplicaUuids = backupLimitExceededClusters
          .stream().map(EligibleCluster::getClusterUuid)
          .collect(Collectors.toList());

      List<String> backupPausedReplicaNames = backupLimitExceededClusters
          .stream().map(EligibleCluster::getClusterName)
          .collect(Collectors.toList());

      List<String> backupResumedReplicaUuids =
          replicaPEUuids.stream()
                        .filter(replicaPEUuid ->
                                    !backupPausedReplicaUuids
                                        .contains(replicaPEUuid))
                        .collect(Collectors.toList());

      log.info("List of clusters on which backup limit exceeded the capacity " +
               "of clusters: {}", backupPausedReplicaUuids);

      if (!backupPausedReplicaUuids.isEmpty()) {
        updateReplicaPEsBackupStatus(backupPausedReplicaUuids,
                                     true,
                                     replicaPePcBackupConfigs);
        alertUtil.raiseAlertAsBackupLimitExceeded(backupPausedReplicaNames);
      }

      if (!backupResumedReplicaUuids.isEmpty()) {
        updateReplicaPEsBackupStatus(backupResumedReplicaUuids,
                                     false,
                                     replicaPePcBackupConfigs);
      }
    } catch (EntityCalculationException e) {
      log.warn("Unable to get backup entity count and appropriately update " +
               "the status.");
    }
  }

  /**
   * Check if backup status is required to be updated for the replica PEs, if
   * the status is incorrect in the db and requires update, then this method
   * updates the status.
   *
   * @param backupReplicaUuids - replica PE uuids which are required to be
   *                             updated.
   * @param currentPauseBackupStatus - whether the backup status is required
   *                                   to be paused or resumed.
   * @throws InsightsInterfaceException - can throw Insights interface exception
   * @throws PCResilienceException - can throw pc backup exception
   */
  private void updateReplicaPEsBackupStatus(
      List<String> backupReplicaUuids,
      boolean currentPauseBackupStatus,
      List<PcBackupConfig> pcBackupConfigs)
      throws InsightsInterfaceException, PCResilienceException {
    String currentPauseBackupMessage =
        getPauseBackupMessage(currentPauseBackupStatus);

    List<String> inconsistentBackupStatusPeUuidList = new ArrayList<>();

    for (String replicaUuid: backupReplicaUuids) {
      // Get the pcBackupConfig for the given replicaUuid.
      Optional<PcBackupConfig> pcBackupConfigMatched =
          pcBackupConfigs.stream().filter(
              pcBackupConfig ->
                  pcBackupConfig.getClusterUuid().equals(replicaUuid))
                               .findFirst();

      if (pcBackupConfigMatched.isPresent()) {
        // Check if the new backup config required and already existing backup
        // config are same or different.
        boolean isBackupConfigChanged =
            !(pcBackupConfigMatched.get().getPauseBackup() ==
                  currentPauseBackupStatus &&
              pcBackupConfigMatched.get().getPauseBackupMessage()
                                   .equals(currentPauseBackupMessage));
        if (isBackupConfigChanged) {
          // Add the replica Uuid which is required to be updated.
          inconsistentBackupStatusPeUuidList.add(replicaUuid);
        }
      } else {
        log.warn("PcBackupConfig with replicaUuid {} not found.",
                 replicaUuid);
      }
    }

    // If the pe uuid list is not empty update the status of pause backup.
    if (!inconsistentBackupStatusPeUuidList.isEmpty()) {
      setBackupStatusForReplicaPEs(inconsistentBackupStatusPeUuidList,
                                   currentPauseBackupStatus,
                                   currentPauseBackupMessage);
      log.info("Successfully {} backup of Prism Central on clusters " +
               "with Uuid: {}", currentPauseBackupStatus ? PAUSED : RESUMED,
               inconsistentBackupStatusPeUuidList);
    }

    List<String> consistentBackupStatusPeUuidList =
        backupReplicaUuids.stream().filter(
            replicaPEUuid ->
                !inconsistentBackupStatusPeUuidList.contains(replicaPEUuid))
                          .collect(Collectors.toList());
    if (!consistentBackupStatusPeUuidList.isEmpty()) {
      log.info("Backup {} on clusters with uuid: {}",
               currentPauseBackupStatus ?
               PAUSED_MESSAGE : IN_PROGRESS_MESSAGE,
               consistentBackupStatusPeUuidList);
    }
  }

  /**
   * Get the pause backup message depending on whether the backup is paused
   * or resumed.
   *
   * @param pauseBackupStatus - whether the backup is paused or resumed.
   * @return - returns whether backup limit exceeded is responsible for pause
   * or the backup is not paused.
   */
  private String getPauseBackupMessage(boolean pauseBackupStatus) {
    String pauseBackupMessage;
    if (pauseBackupStatus) {
      // If backup is required to be paused then set pauseBackupStatus to true.
      pauseBackupMessage =
          Messages.PauseBackupMessagesEnum.BACKUP_LIMIT_EXCEEDED.name();
    } else {
      // If backup is required to be resumed then set pauseBackupStatus to
      // false.
      pauseBackupMessage = PAUSE_BACKUP_EMPTY_MESSAGE;
    }
    return pauseBackupMessage;
  }
}
