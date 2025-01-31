package com.nutanix.prism.pcdr.restserver.schedulers;

import com.amazonaws.services.s3.AmazonS3URI;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nutanix.api.utils.json.JsonUtils;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.insights.ifc.InsightsInterfaceProto.BatchUpdateEntitiesArg;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig;
import com.nutanix.prism.exception.MantleException;
import com.nutanix.prism.pcdr.PcBackupMetadataProto;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.dto.ObjectStoreCredentialsBackupEntity;
import com.nutanix.prism.pcdr.restserver.services.api.*;
import com.nutanix.prism.pcdr.restserver.util.CertFileUtil;
import com.nutanix.prism.pcdr.restserver.util.FlagHelperUtil;
import com.nutanix.prism.pcdr.restserver.util.PCRetryHelper;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.restserver.zk.PCUpgradeWatcher;
import com.nutanix.prism.pcdr.util.*;
import com.nutanix.prism.pcdr.zklock.DistributedLock;
import dp1.pri.prism.v4.protectpc.PcEndpointCredentials;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.CERTS_COPY_FAILED_ZK_NODE;
import static com.nutanix.prism.pcdr.constants.Constants.PAUSE_BACKUP_SCHEDULER_ZK_NODE;

@Slf4j
@Service
public class BackupScheduler {

  @Autowired
  private PCBackupService pcBackupService;
  @Autowired
  private PCVMDataService pcvmDataService;
  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private PCRetryHelper pcRetryHelper;
  @Autowired
  private ObjectStoreBackupService objectStoreBackupService;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Value("${prism.pcdr.backup.schedule.highFrequency:1800000}")
  private long highFrequencyMisec;
  @Value("${prism.pcdr.backup.schedule.maxAllowedRemainingTime:300000}")
  private long maxAllowedRemainingTimeMisec;

  @Value("${prism.pcdr.backup.schedule.updateClusterEligibility:true}")
  private boolean updateClusterEligibility;
  // This flag is responsible for enabling/disabling the backupLimit feat.
  @Autowired
  private FlagHelperUtil flagHelperUtil;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;
  @Autowired
  private BackupStatus backupStatus;
  @Autowired
  private PCUpgradeWatcher pcUpgradeWatcher;
  @Autowired
  private MantleUtils mantleUtils;
  @Autowired
  private ClusterEligibilityUpdateHelper clusterEligibilityUpdateHelper;
  @Autowired
  CertFileUtil certFileUtil;

  @Autowired
  @Qualifier("adonisServiceScheduledThreadPool")
  private ScheduledExecutorService adonisServiceScheduledThreadPool;

  @Value("${prism.pcdr.backup.schedule.highFrequencyDelay:120000}")
  private long initialDelay;

  @Value("${prism.pcdr.backup.schedule.highFrequency:1800000}")
  private long fixedDelay;

  @PostConstruct
  public void init() {
    createPCBackupScheduler();
  }

  private void createPCBackupScheduler() {
     log.info("Creating backup scheduler with Fixed delay.");
       // The scheduled task will run every 30 minutes default.
      adonisServiceScheduledThreadPool.scheduleWithFixedDelay(
            this::highFrequencyBackupSchedule, initialDelay, fixedDelay, TimeUnit.MILLISECONDS );
  }

  public void highFrequencyBackupSchedule() {

    // Check if the backup scheduler of explicitly paused using
    // PAUSE_BACKUP_SCHEDULER_ZK_NODE.
    //
    if (isBackupSchedulerPaused()) {
      // In-case backup scheduler is paused return.
      return;
    }
    // Check the cached and the actual state of upgrade and handles the
    // change that might have happened.
    pcUpgradeWatcher.handleUpgradeStateChange();
    // Check if upgrade is in progress or not, in case upgrade is in progress
    // do not run the scheduler.
    if (pcUpgradeWatcher.isUpgradeInProgress()) {
      log.info("Upgrade is in progress, not proceeding with backup. Backup " +
               "is paused.");
      return;
    }

    // Create a date time formatter to be used later in logs.
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    // Store the start of scheduler datetime object
    LocalDateTime from = LocalDateTime.now();

    // Get a distributed lock object for backup (lock on write_lock_backup zk
    // node.)
    DistributedLock distributedLock =
            instanceServiceFactory.getDistributedLockForBackupPCDR();
    try {
      // Acquire a non-blocking lock i.e. if the lock is not acquired and is
      // taken up by some other thread or PCVM, then do not proceed with
      // backup on this thread/node.
      if (!distributedLock.lock(false)) {
        log.info("Unable to acquire lock, backup already" +
                " in process. Skipping on this node.");
        return;
      }

      // run cluster eligibility update task if enabled
      if(updateClusterEligibility && clusterEligibilityUpdateHelper.isTimeToUpdateClusterEligibility()) {
        clusterEligibilityUpdateHelper.updateClustersEligibility();
      }

      List<String> replicaPEUuids =
            backupStatus.fetchPcBackupConfigProtoListForPEReplicas()
                        .stream()
                        .map(PcBackupConfig::getClusterUuid)
                        .collect(Collectors.toList());
      String pcClusterUuid =
          instanceServiceFactory.getClusterUuidFromZeusConfig();

      ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntity =
          PCUtil.fetchExistingObjectStoreEndpointInPCBackupConfigWithCredentials(entityDBProxy,
              instanceServiceFactory.getClusterUuidFromZeusConfig());;

      // Populate new fields, bucket name, region if not populated.
      // case when PC upgraded from not having objects to version supporting objects
      // this will also update objectStoreCredentialsBackupEntity
      updateIDFEntityWithNewFields(objectStoreCredentialsBackupEntity);

      List<PcObjectStoreEndpoint> objectStoreEndpoints = objectStoreCredentialsBackupEntity.getObjectStoreEndpoints();
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap = pcvmDataService.
          getObjectStoreEndpointDtoMapFromObjectStoreCredentialsBackupEntity(objectStoreCredentialsBackupEntity);
      List<ObjectStoreEndPointDto> objectStoreEndPointDtoList = objectStoreEndPointDtoMap.values().stream().collect(Collectors.toList());

      maybeCopyCertFilesToFailedNodes(objectStoreEndPointDtoList);

      // fetch secret credentials from mantle service
      Map<String, ClusterExternalStateProto.ObjectStoreCredentials> objectStoreCredentialsProtoMap = fetchCredentialsFromMantle(
          objectStoreCredentialsBackupEntity.getCredentialsKeyIdMap());
      updateObjectStoreEndPointListWithCredentials(objectStoreEndpoints, objectStoreCredentialsProtoMap);
      log.debug("Replica PE Uuids are: {}", replicaPEUuids);
      log.debug("Object store endpoints are: {}", objectStoreEndpoints);
      if (!replicaPEUuids.isEmpty() || !objectStoreEndpoints.isEmpty()) {
        if (isThresholdForBackupCrossed(from)) {
          // If backup limit feat is enabled only then proceed with the
          // backup limit check as part of updateAndReturnBackupPauseStatus
          // else skip the check.
          // First check whether the entities count allows backup to continue
          // or not. If backup is supposed to be paused then return directly.
          if (flagHelperUtil.isBackupLimitFeatEnabled()) {
            backupStatus.updateReplicaPEsPauseBackupStatus();
          }
          // If entity count allows for the backup to continue then proceed
          // further ahead.
          log.debug("High frequency backup started at time: {}", dtf.format(from));
          BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg =
              BatchUpdateEntitiesArg.newBuilder();
          pcBackupService.addUpdateEntitiesArgForPCBackupSpecs(
              batchUpdateEntitiesArg);
          pcBackupService.addUpdateEntitiesArgForPCZkData(
              batchUpdateEntitiesArg);
          makeBatchRPC(batchUpdateEntitiesArg);
          pcBackupService.deleteStaleZkNodeValuesFromIDF(batchUpdateEntitiesArg);
          // Update metadata only when specs and zk nodes are updated correctly.
          BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArgForMetadata =
              BatchUpdateEntitiesArg.newBuilder();
          pcBackupService.addUpdateEntitiesArgForPCBackupMetadata(
              batchUpdateEntitiesArgForMetadata, replicaPEUuids,
              objectStoreEndpoints, objectStoreEndPointDtoMap, true, true);
          makeBatchRPC(batchUpdateEntitiesArgForMetadata);
          LocalDateTime to = LocalDateTime.now();

          log.debug("High frequency backup ended at time: {}", dtf.format(to));
          if (!replicaPEUuids.isEmpty()) {
            log.info("Initiating resetBackupSyncStates for replica PE Uuids " +
                     "associated with PC");
            pcBackupService.resetBackupSyncStates();
          }

          if (!objectStoreEndpoints.isEmpty()) {
            log.debug("Initiating backup to S3 bucket for object store " +
                     "endpoints.");
            // Handle the backup state for objectstore endpoints. Following
            // method will ensure these things asynchronously in that order -
            // 1. Publishing the seed data in IDF.
            // 2. Handle the pause backup status based on seed data availability.
            // 3. Write object metadata string to S3 if not already present.
            objectStoreBackupService.initiateBackupToObjectStore(
                objectStoreEndPointDtoMap, pcClusterUuid,
                    entityDBProxy, objectStoreEndpoints);
          }
          Duration duration = Duration.between(from, to);
          log.info("Time taken for backup: {} seconds", duration.getSeconds());
        }
        else {
          log.info("Skipping backup, unable to cross the threshold of " +
                   "remaining allowed time in milli seconds {}",
                   maxAllowedRemainingTimeMisec);
        }
      } else {
        makeDbTableEntitiesConsistent();
        log.info("There is no entry in pc_backup_config table. Nothing to " +
                 "backup yet.");
      }
    }
    catch (InsightsInterfaceException e) {
      log.error("Aborting backing up data. Error encountered while making a " +
                "batch RPC call for backing up data. Error: ", e);
    }
    catch (PCResilienceException e) {
      log.error("Aborting backing up data. Unable to add backup, the " +
                "following error encountered: ", e);
    }
    catch (Throwable t) {
      log.error("Aborting backing up data. Unable to add backup, the " +
                "following error encountered: ", t);
      throw t;
    }
    finally {
      // Unlocking
      distributedLock.unlock();
    }
  }

  private void updateIDFEntityWithNewFields(ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntity)
      throws PCResilienceException, InsightsInterfaceException {
    List<PcObjectStoreEndpoint> pcObjectStoreEndpointList = objectStoreCredentialsBackupEntity.getObjectStoreEndpoints();

    BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg =
        BatchUpdateEntitiesArg.newBuilder();

    List<PcBackupConfig> latestPcBackupConfigs = new ArrayList<>();

    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : pcObjectStoreEndpointList) {
      // if true then Object store will be AWS S3
      if (ObjectUtils.isEmpty(pcObjectStoreEndpoint.getBucket())) {
        AmazonS3URI uri = new AmazonS3URI(pcObjectStoreEndpoint.getEndpointAddress());
        pcObjectStoreEndpoint.setRegion(uri.getRegion());
        pcObjectStoreEndpoint.setBucket(uri.getBucket());
        pcObjectStoreEndpoint.setSkipCertificateValidation(false);

        // update pc backup config table
        String pcBackupConfigUUID = PCUtil.getObjectStoreEndpointUuid(
            instanceServiceFactory.getClusterUuidFromZeusConfig(), pcObjectStoreEndpoint.getEndpointAddress());
        PcBackupConfig pcBackupConfig = PCUtil.getPcBackupConfigById(pcBackupConfigUUID, pcRetryHelper);
        PcBackupConfig.ObjectStoreEndpoint objectStoreEndpoint
            = PcBackupConfig.ObjectStoreEndpoint.newBuilder(pcBackupConfig.getObjectStoreEndpoint())
            .setBucketName(uri.getBucket()).setRegion(uri.getRegion())
            .setSkipCertificateValidation(false).setPathStyleEnabled(false).build();
        latestPcBackupConfigs.add(
            PcBackupConfig.newBuilder(pcBackupConfig)
                .setObjectStoreEndpoint(objectStoreEndpoint)
                .build());

        List<InsightsInterfaceProto.UpdateEntityArg> updateEntityArgList =
            backupStatus.createUpdateEntityArgListForObjectStoreReplicas(latestPcBackupConfigs);
        log.debug("Batch updating pc backup config table with following entries: {}", updateEntityArgList);
        batchUpdateEntitiesArg.addAllEntityList(updateEntityArgList);
        List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
            batchUpdateEntitiesArg, entityDBProxy);
        if (!errorList.isEmpty()) {
          log.error(errorList.toString());
          throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_DATABASE_WRITE_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }

    }

  }

  protected void updateObjectStoreEndPointListWithCredentials(List<PcObjectStoreEndpoint> objectStoreEndpoints,
                                                              final Map<String, ClusterExternalStateProto.ObjectStoreCredentials> objectStoreCredentialsProtoMap) {
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : objectStoreEndpoints) {
      if (objectStoreCredentialsProtoMap.containsKey(pcObjectStoreEndpoint.getEndpointAddress())) {
        ClusterExternalStateProto.ObjectStoreCredentials credentials = objectStoreCredentialsProtoMap.get(pcObjectStoreEndpoint.getEndpointAddress());
        String accesskey = credentials.getAccessKey();
        String secretAccessKey = credentials.getSecretAccessKey();
        pcObjectStoreEndpoint.getEndpointCredentials().setAccessKey(accesskey);
        pcObjectStoreEndpoint.getEndpointCredentials().setSecretAccessKey(secretAccessKey);
      } else {
          // empty credentials object is added to prevent npe
          // while fetching access key and secret access key from endpoint credentials to get s3 client later
          if (ObjectUtils.isEmpty(pcObjectStoreEndpoint.getEndpointCredentials())) {
            pcObjectStoreEndpoint.setEndpointCredentials(new PcEndpointCredentials());
          }
      }
    }
  }

  /**
   * Fetch saved credentials from mantle service
   * @param credentialsKeyIdMap  {entityId,credentialKeyId}
   * @return objectStoreCredentialsMap - {entityId,objectCredentialProto}
   */
  protected Map<String, ClusterExternalStateProto.ObjectStoreCredentials> fetchCredentialsFromMantle
      (Map<String, String> credentialsKeyIdMap) {
    Map<String, ClusterExternalStateProto.ObjectStoreCredentials> objectStoreCredentialsProtoMap = new HashMap<>();
    credentialsKeyIdMap.forEach((endPointAddress, credentialKeyId) -> {
      if(StringUtils.isNotEmpty(credentialKeyId)) {
        try {
          ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto = ClusterExternalStateProto.
              ObjectStoreCredentials.getDefaultInstance();
          objectStoreCredentialsProto = mantleUtils.
              fetchSecret(credentialKeyId, objectStoreCredentialsProto);
          log.info("Successfully fetched the secret credentials for objectstore endpointAddress: {}",
                   endPointAddress);
          objectStoreCredentialsProtoMap.put(endPointAddress, objectStoreCredentialsProto);
        }
        catch (MantleException e) {
          log.error("Failed to fetch secret credentials for objectstore endPointAddress: {} with {}", endPointAddress,e);
        }
      }
    });
    return objectStoreCredentialsProtoMap;
  }

  protected void maybeCopyCertFilesToFailedNodes(List<ObjectStoreEndPointDto> objectStoreEndPointDtoList) throws PCResilienceException{

    List<String> backupTargetIds;
    try {
      Stat stat = zookeeperServiceHelper.exists(CERTS_COPY_FAILED_ZK_NODE, false);
      if (ObjectUtils.isEmpty(stat)) {
        log.info("The node {} doesnt exist, no need to copy cert files", CERTS_COPY_FAILED_ZK_NODE);
        return;
      } else {
        backupTargetIds = zookeeperServiceHelper.getChildren(CERTS_COPY_FAILED_ZK_NODE);
      }
    } catch (Exception e){
      String error = String.format("Hit Zookeeper Exception while trying to list the nodes under %s : %s", CERTS_COPY_FAILED_ZK_NODE, e);
      throw new PCResilienceException(error, ErrorCode.PCBR_INTERNAL_SERVER_ERROR,
              HttpStatus.INTERNAL_SERVER_ERROR);
    }
    log.debug("Backup Target Ids whose certs need to be copied {}", backupTargetIds);
    List<String> ipList;
    List<ObjectStoreEndPointDto> singleObjectStoreEndPointDtoList;
    ObjectMapper objectMapper = JsonUtils.getObjectMapper();
    byte[] data;
    if (!backupTargetIds.isEmpty()) {
      for (String backupTargetId : backupTargetIds){
        final Stat stat = new Stat();
        try {
          data = zookeeperServiceHelper.getData(CERTS_COPY_FAILED_ZK_NODE+"/"+backupTargetId, stat);
        } catch (KeeperException.NoNodeException e) {
          log.error("Failed to read from Zknode {}", CERTS_COPY_FAILED_ZK_NODE);
          throw new PCResilienceException("Failed to read from Zknode",
                  ErrorCode.PCBR_INTERNAL_SERVER_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
        } catch (Exception e){
          String error = String.format("Hit Zookeeper Exception while trying to read from Zookeeper : %s", CERTS_COPY_FAILED_ZK_NODE);
          log.error(error, e);
          throw new PCResilienceException(error, ErrorCode.PCBR_INTERNAL_SERVER_ERROR,
                  HttpStatus.INTERNAL_SERVER_ERROR);
        }

        try {
          ipList = objectMapper.readValue(data, ArrayList.class);
        } catch (IOException e) {
          throw new PCResilienceException("Failed to read Zk node data using object mapper",
                  ErrorCode.PCBR_INTERNAL_SERVER_ERROR,
                  HttpStatus.INTERNAL_SERVER_ERROR);
        }
        singleObjectStoreEndPointDtoList = objectStoreEndPointDtoList.stream().filter(objectStoreEndPointDto -> objectStoreEndPointDto.getBackupUuid().equals(backupTargetId)).collect(Collectors.toList());
        if(!singleObjectStoreEndPointDtoList.isEmpty() && !ipList.isEmpty()){
          ObjectStoreEndPointDto objectStoreEndPointDto = singleObjectStoreEndPointDtoList.get(0);
          certFileUtil.createCertFileOnPCVMs(objectStoreEndPointDto, ipList, true);
        }
      }
    }
  }

  /**
   * Function to update pc_backup_metadata according to pc_backup_config, so
   * that there is no inconsistency between the two tables.
   * @throws PCResilienceException - can throw backup exception.
   */
  private void makeDbTableEntitiesConsistent() throws PCResilienceException {
    PcBackupMetadataProto.PCVMBackupTargets pcvmBackupTargets;
    try {
      pcvmBackupTargets = PcdrProtoUtil.fetchBackupTargetFromPcBackupMetadata(
          instanceServiceFactory.getClusterUuidFromZeusConfig(), entityDBProxy);
      log.debug("Fetched replica UUID's from pc_backup_metadata. {}",
                pcvmBackupTargets);
    } catch (Exception e) {
      log.error("Unable to fetch pc_backup_metadata from IDF due to exception: ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }
    if (pcvmBackupTargets == null) {
      log.debug("No entity found in pc_backup_metadata table as well.");
    } else {
      log.warn("Deleting entity from pc_backup_metadata table because " +
               "pc_backup_config table is empty. This could have happened" +
               " because of partial update in add-replica or " +
               "remove-replica");
      pcBackupService.deletePcBackupMetadataProtoEntity();
    }
  }

  /**
   * Makes a Batch RPC call based on the update entities arg provided.
   *
   * @param batchUpdateEntitiesArg - batch update arg
   * @throws InsightsInterfaceException - can throw insights interface exception
   * @throws PCResilienceException - can throw pc backup exception.
   */
  private void makeBatchRPC(BatchUpdateEntitiesArg.Builder
                                batchUpdateEntitiesArg) throws InsightsInterfaceException,
                                                               PCResilienceException {
    List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
        batchUpdateEntitiesArg, entityDBProxy);
    if (CollectionUtils.isNotEmpty(errorList)) {
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_DATABASE_WRITE_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Checks whether the threshold of time for backup is crossed or not. The
   * scheduler will only run when the threshold is crossed.
   *
   * @param from - the time when this backup started
   * @return - returns boolean representing whether there is a requirement to
   * proceed through backup
   * @throws InsightsInterfaceException - can throw
   * InsightsInterfaceException By IDF.
   */
  boolean isThresholdForBackupCrossed(LocalDateTime from)
      throws InsightsInterfaceException {
    long fromTimestampMisec = Timestamp.valueOf(from).getTime();
    String pcClusterUuid = instanceServiceFactory
        .getClusterUuidFromZeusConfig();
    InsightsInterfaceProto.Entity entity =
        IDFUtil.getEntityWithEntityId(entityDBProxy,
                                      Constants.PC_BACKUP_SPECS_TABLE,
                                      pcClusterUuid);
    boolean proceedWithBackup = (entity == null);
    if (!proceedWithBackup) {
      long timePastSinceLastBackupMisec =
          fromTimestampMisec - (entity.getModifiedTimestampUsecs() / 1000);
      long remainingTimeMisec =
          (highFrequencyMisec - timePastSinceLastBackupMisec);
      log.debug("Remaining time for the scheduled backup in milliSec: {}",
                remainingTimeMisec);
      proceedWithBackup = remainingTimeMisec < maxAllowedRemainingTimeMisec;
    }
    else {
      log.debug("Entity returned is null, backing up data from scratch.");
    }
    return proceedWithBackup;
  }

  /**
   * Check if the PAUSE_BACKUP_SCHEDULER_ZK_NODE is present or not, if it's
   * present that means the backup scheduler should not proceed any further
   * and backup scheduler is paused.
   *
   * @return true if backup scheduler is paused else false
   */
  private boolean isBackupSchedulerPaused() {
    // If Restore is running do not perform backup.
    // (PAUSE_BACKUP_SCHEDULER_ZK_NODE is created at the time of restore and
    // is deleted when the restore is finished.)
    try {
      // Check if the PAUSE_BACKUP_SCHEDULER_ZK_NODE is present or not.
      Stat stat = zookeeperServiceHelper.exists(PAUSE_BACKUP_SCHEDULER_ZK_NODE, false);
      // If zk node is not present then stat is null else it is not null.
      if (stat != null) {
        log.info("{} zk node exists with stats {}. Not proceeding with backup" +
                 " cause the backup is paused.", PAUSE_BACKUP_SCHEDULER_ZK_NODE, stat);
        return true;
      }
    } catch (Exception e) {
      log.error("Error while checking whether {} zk node exists or not. Not " +
                "performing the backup", PAUSE_BACKUP_SCHEDULER_ZK_NODE, e);
      // In-case we are not able to read the zk node we will keep
      // backup paused because it can hinder the recovery process if it
      // happens at the time of recovery.
      return true;
    }
    // In-case the node is not found return false.
    return false;
  }
}
