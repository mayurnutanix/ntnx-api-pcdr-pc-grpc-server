package com.nutanix.prism.pcdr.restserver.zk;

import com.google.common.annotations.VisibleForTesting;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.messages.Messages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.dto.ObjectStoreCredentialsBackupEntity;
import com.nutanix.prism.pcdr.restserver.services.api.BackupStatus;
import com.nutanix.prism.pcdr.restserver.util.FlagHelperUtil;
import com.nutanix.prism.pcdr.restserver.util.PCRetryHelper;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import com.nutanix.prism.pcdr.zklock.DistributedLock;
import com.nutanix.prism.zk.PCDRWatchedZkNode;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.IDF_BACKUP_SYNC_STATES_ZK_PATH;
import static com.nutanix.prism.pcdr.messages.Messages.PAUSE_BACKUP_EMPTY_MESSAGE;
import static com.nutanix.prism.pcdr.messages.Messages.UPGRADE_PAUSE_BACKUP_MESSAGE;

/**
 * Class to watch Upgrade zk Node and pause/unpause the dr sync accordingly.
 */
@Service
@Slf4j
public class PCUpgradeWatcher implements Watcher {

  @Getter @Setter
  private List<String> childrenPaths = new ArrayList<>();

  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private BackupStatus backupStatus;
  @Autowired
  private FlagHelperUtil flagHelperUtil;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;
  @Autowired
  private PCRetryHelper pcRetryHelper;
  @Autowired
  private EntityDBProxyImpl entityDBProxy;

  private PCDRWatchedZkNode pcdrWatchedZkNode;

  public PCUpgradeWatcher() {
    pcdrWatchedZkNode = new PCDRWatchedZkNode();
  }

  @PostConstruct
  public void init() {
    refreshChildren();
  }

  @VisibleForTesting
  public void init(PCDRWatchedZkNode pcdrWatchedZkNode) {
    this.pcdrWatchedZkNode = pcdrWatchedZkNode;
  }


  /**
   * This method is executed whenever the children of the zk node
   * /appliance/logical/genesis/node_upgrade_status changes.
   *
   * If the children are 0 that means either the service has restarted or the
   * upgrade has finished. If children are > 0 that means the upgrade is in
   * progress.
   *
   * We will pause the backup in case upgrade is in progress, and this method
   * is responsible for handling it. In case backup limit feat is enabled
   * this method keeps the backup paused even after upgrade has finished if
   * the backup limit on any of the Replica PE has exceeded.
   */
  protected void handleChildrenChanged() {
    try {
      DistributedLock distributedBackupLock =
          instanceServiceFactory.getDistributedLockForBackupPCDR();
      DistributedLock distributedUpgradeLock =
          instanceServiceFactory.getDistributedLockForUpgradePCDR();
      try {
        // Acquire a non-blocking lock on upgrade. The upgrade logic can be
        // triggered on all the PCVM nodes, so we want this logic to be
        // executed in only one of the node if called simultaneously.
        if (distributedUpgradeLock.lock(false)) {
          try {
            // Acquire lock for backup, it is possible that the backup scheduler
            // and upgrade are triggered simultaneously, so we want the upgrade
            // to be executed when scheduler is not running.
            // Reason being the scheduler and upgrade watcher modifies the same
            // piece of data in IDF table.
            // Acquire a blocking lock in case of backup because we want the
            // upgrade logic to be executed for sure.
            if (distributedBackupLock.lock(true)) {
              // Check if upgrade is in progress or not, if upgrade is in
              // progress that means that we want to pause the backup hence
              // pauseBackup will be true, else it will be false.
              boolean pauseBackup = isUpgradeInProgress();

              // Get the replica PE uuids from the backupStatus object
              // (pc_backup_config table in IDF is the source of truth of
              // replica PEs)
              List<String> replicaPEUuids =
                  backupStatus.fetchPcBackupConfigProtoListForPEReplicas()
                              .stream()
                              .map(PcBackupConfig::getClusterUuid)
                              .collect(Collectors.toList());
              log.debug("Replica PE Uuids are: {}", replicaPEUuids);
              ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntity = pcRetryHelper.
                  fetchExistingObjectStoreEndpointInPCBackupConfigWithCredentials();
              List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig =
                  new ArrayList<>(objectStoreCredentialsBackupEntity.getObjectStoreEndpoints());
              // credential key id stored in mantle service as part of add replica
              Map<String, String> credentialsKeyIdMap = objectStoreCredentialsBackupEntity.getCredentialsKeyIdMap();
              log.debug("ObjectStore backup targets are: {}",
                        objectStoreEndpointInPCBackupConfig);
              // Since the backup is paused, it means that PC upgrade is in
              // progress. At the time of upgrade, setting up ResetIdfBackupSyncZkNode
              // is required so that we can call reset IDF Sync state for
              // the Replica PEs which will responsible for taking PC backup.
              if (pauseBackup && !replicaPEUuids.isEmpty()) {
                try {
                  setupResetIdfBackupSyncZkNode(replicaPEUuids);
                } catch (PCResilienceException e) {
                  log.error("Setting up reset IDF backup sync status " +
                            "failed with error: {}", e.getMessage());
                }
              }

              String pauseBackupMessage = pauseBackup ?
                                          Messages.PauseBackupMessagesEnum
                                              .UPGRADE_IN_PROGRESS
                                              .name() : PAUSE_BACKUP_EMPTY_MESSAGE;
              backupStatus.setBackupStatusForAllReplicaPEsWithRetries(
                  replicaPEUuids,
                  pauseBackup,
                  pauseBackupMessage);

              // check if we are updating the pause backup status to false and
              // the backup was paused due to right reason i.e upgrade in
              // progress in this case.
              if (pauseBackup == false &&
                  PCUtil.isValidPauseStateForObjectStoreEndpoint(
                      entityDBProxy, UPGRADE_PAUSE_BACKUP_MESSAGE)) {
                backupStatus
                    .setBackupStatusForAllObjectStoreReplicasWithRetries(
                        objectStoreEndpointInPCBackupConfig, pauseBackup,
                        pauseBackupMessage);
              }

              if (flagHelperUtil.isBackupLimitFeatEnabled() && !pauseBackup
                  && !replicaPEUuids.isEmpty()) {
                backupStatus.updateReplicaPEsPauseBackupStatus();
              }
            } else {
              log.error("Unable to acquire blocking node for pausing backup " +
                        "during upgrade.");
            }
          }
          finally {
            // Unlocking backup lock
            distributedBackupLock.unlock();
          }
        } else {
          log.warn("Unable to acquire unblocking node for pausing backup " +
                   "during upgrade. Check the other PCVMs to see if they have" +
                   " acquired it.");
        }
      } finally {
        // Unlocking upgrade lock
        distributedUpgradeLock.unlock();
      }
    }
    catch (Exception e) {
      log.error("Unable to pause backup while upgrading, the error is:{}",
                e.getMessage(), e);
    }
  }

  /**
   * This method is responsible for setting up ResetIdfBackupSyncZkNode
   * at path /appliance/logical/prism/pcdr/reset_idf_sync_states with zkData
   * as list of replica PE Uuids. This involves setting up zkNode(if does not
   * exist) with appropriate zkData OR setting appropriate zkData values in case
   * zkNode already exist.
   *
   * @param replicaPEUuids - List of replica PE Uuids
   */
  public void setupResetIdfBackupSyncZkNode(List<String> replicaPEUuids)
      throws PCResilienceException {
    String serializedReplicaPEUuids = null;
    log.info("List of Replica PE's UUIDs registered with Prism Central: {}",
             replicaPEUuids);
    try {
      Stat stat = zookeeperServiceHelper.exists(IDF_BACKUP_SYNC_STATES_ZK_PATH, false);

      // Serialize the list of PE Uuids as data needs to be stored on zkNode in Byte format
      serializedReplicaPEUuids = replicaPEUuids.isEmpty() ?
                                 Constants.NO_DATA_ZK_VALUE :
                                 String.join(",", replicaPEUuids);
      if (stat == null) {
        log.info("IDF_BACKUP_SYNC_STATES zkNode does not exists so creating the" +
                 " corresponding zknode");
        PCUtil.createAllParentZkNodes(IDF_BACKUP_SYNC_STATES_ZK_PATH, zookeeperServiceHelper);
        zookeeperServiceHelper.createZkNode(IDF_BACKUP_SYNC_STATES_ZK_PATH,
                              serializedReplicaPEUuids.getBytes(),
                              ZooDefs.Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
      } else {
        log.info("IDF_BACKUP_SYNC_STATES zkNode already exists, so updating the" +
                 " existing zkNode");
        zookeeperServiceHelper.setData(IDF_BACKUP_SYNC_STATES_ZK_PATH,
                         serializedReplicaPEUuids.getBytes(),
                         stat.getVersion());
      }
    } catch (Exception e) {
      log.error("Error encounter while setting up IDF back sync status " +
                "zkNode:", e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * If there is no children in the root zk node
   * (/appliance/logical/genesis/node_upgrade_status) that means the upgrade
   * has either finished or the service has started up.
   *
   * If there are children that means that upgrade is in progress
   *
   * @return - boolean representing whether upgrade is in progress or not
   */
  public boolean isUpgradeInProgress() {
    return !getChildrenPaths().isEmpty();
  }

  /**
   * The method is responsible to check if upgrade is in progress or not, and
   * whether the children count has changed from the cached value, if it has
   * changed then execute the upgrade handle children changed method.
   *
   * This method is responsible for updating the childrenPaths current values.
   */
  public void handleUpgradeStateChange() {
    try {
      List<String> updatedChildNodes = zookeeperServiceHelper.getChildren(
          Constants.ROOT_PATH);
      log.debug("Children nodes found for upgrade are {}", updatedChildNodes);
      if (!PCUtil.listEqualsIgnoreOrder(updatedChildNodes, getChildrenPaths())) {
        log.info("Cached node_upgrade_status children are not same, current " +
                 "cached values are {} and latest values are {}",
                 getChildrenPaths(), updatedChildNodes);
        setChildrenPaths(updatedChildNodes);
        handleChildrenChanged();
      }
    }
    catch (final KeeperException.NoNodeException e) {
      log.warn("ZK node {} does not exists", Constants.ROOT_PATH, e);
    }
    catch (Exception e) {
      log.error("Error encountered while reading children nodes from " +
                "zookeeper.", e);
    }
  }

  /**
   * Function to refreshChildren. This sets the watch on desired zk Node.
   */
  void refreshChildren() {
    try {
      setChildrenPaths(pcdrWatchedZkNode.getZookeeper()
                                        .getChildren(Constants.ROOT_PATH,
                                                     this));
      handleChildrenChanged();
    }
    catch (Exception e) {
      log.error("ZooKeeper Exception", e);
    }
    log.info("All Children list is: {}", getChildrenPaths());
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
      log.debug("Node children Changed event got triggered");
      synchronized (this) {
        refreshChildren();
      }
    }
  }
}
