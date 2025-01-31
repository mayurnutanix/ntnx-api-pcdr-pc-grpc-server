package com.nutanix.prism.pcdr.restserver.util;

import com.google.common.annotations.VisibleForTesting;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.clients.ProtectPcProxyClient;
import com.nutanix.prism.pcdr.restserver.dto.ObjectStoreCredentialsBackupEntity;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import dp1.pri.prism.v4.protectpc.PcvmRestoreFiles;
import dp1.pri.prism.v4.protectpc.RestoreFilesApiResponse;
import dp1.pri.prism.v4.protectpc.StopServicesApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.util.List;

import static com.nutanix.prism.pcdr.constants.Constants.*;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.MAX_ATTEMPT_FOR_BACKUP_CONFIG_FETCH;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.RETRY_DELAY_FOR_FETCHING_CONFIG_MS;


@Slf4j
@Service
public class PCRetryHelper {

  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;
  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private PCDRYamlAdapterImpl pcdrYamlAdapter;
  @Autowired
  private ProtectPcProxyClient protectPcProxyClient;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;

  @VisibleForTesting
  public void init(EntityDBProxyImpl entityDBProxy,
                   ZookeeperServiceHelperPc zookeeperServiceHelper) {
    this.zookeeperServiceHelper = zookeeperServiceHelper;
    this.entityDBProxy = entityDBProxy;
  }

  @Retryable(value = RestClientException.class,
      backoff = @Backoff(delay = MAX_RETRY_DELAY_MS))
  public StopServicesApiResponse stopServicesApiRetryable(String svmIp ,
      List<String> servicesToStop) throws RestClientException {
    log.info("Invoking 'genesis stop' command/API to stop {} services via " +
             "genesis at host with IP: {}", servicesToStop, svmIp);
    return protectPcProxyClient.stopServicesApi(svmIp,servicesToStop);
  }

  @Retryable(value = RestClientException.class,
          backoff = @Backoff(delay = MAX_RETRY_DELAY_MS))
  public RestoreFilesApiResponse restoreFilesApiRetryable(
          String svmIp, PcvmRestoreFiles pcvmRestoreFiles) throws RestClientException {
    log.info("Invoking 'restore-files' API at host with IP: {}", svmIp);
    return protectPcProxyClient.restoreFilesApi(svmIp, pcvmRestoreFiles);

  }

  @Retryable(
      maxAttempts = MAX_RETRY_ATTEMPTS,
      value = {PCResilienceException.class,
                      InterruptedException.class,
                      ZkClientConnectException.class},
      backoff = @Backoff(delay = RETRY_DELAY_FOR_ADMIN_CREATION_MS))
  public void createDummyAdminUserForAuth()
      throws InterruptedException, PCResilienceException, ZkClientConnectException {
    log.info("Trying creating dummy admin user for auth.");
    PCUtil.createDummyAdminUserForAuth(zookeeperServiceHelper, entityDBProxy);
  }

  @Retryable(
      maxAttempts = MAX_RETRY_ATTEMPTS,
      value = {IOException.class,
               InterruptedException.class,
               ZkClientConnectException.class},
      backoff = @Backoff(delay = RETRY_DELAY_FOR_ZK_NODE))
  public void createDisabledServicesZkNode()
      throws InterruptedException, ZkClientConnectException,
             KeeperException.NodeExistsException, IOException {
    log.info("Trying creating disabled services Zk node.");
    PCUtil.createDisabledServicesZkNode(zookeeperServiceHelper, pcdrYamlAdapter);
  }

  @Retryable(value = InsightsInterfaceException.class,
      backoff = @Backoff(delay = MAX_RETRY_DELAY_MS))
  public void deleteAdminUserForAuth()
      throws InsightsInterfaceException {
    log.info("Trying deleting admin user for auth.");
    PCUtil.deleteAdminUserForAuth(entityDBProxy);
  }

  @Retryable(value = PCResilienceException.class,
      backoff = @Backoff(delay = MAX_RETRY_DELAY_MS))
  public void clearRestoreConfigInIdf()
      throws PCResilienceException {
    log.info("Trying deleting admin user for auth.");
    PCUtil.clearRestoreConfigInIdf(entityDBProxy);
  }

  @Retryable(
      maxAttempts = MAX_ATTEMPT_FOR_BACKUP_CONFIG_FETCH,
      value = PCResilienceException.class,
      backoff = @Backoff(delay = RETRY_DELAY_FOR_FETCHING_CONFIG_MS))
  public List<String> fetchExistingClusterUuidInPCBackupConfig()
      throws PCResilienceException {
    log.debug("Trying fetching existing cluster uuid in pc_backup_config table.");
    return PCUtil.fetchExistingClusterUuidInPCBackupConfig(entityDBProxy);
  }

  @Retryable(
      maxAttempts = MAX_ATTEMPT_FOR_BACKUP_CONFIG_FETCH,
      value = PCResilienceException.class,
      backoff = @Backoff(delay = RETRY_DELAY_FOR_FETCHING_CONFIG_MS))
  public List<PcObjectStoreEndpoint> fetchExistingObjectStoreEndpointInPCBackupConfig()
          throws PCResilienceException {
    log.debug("Trying fetching existing object store endpoint list in " +
              "pc_backup_config table.");
    return PCUtil.fetchExistingObjectStoreEndpointInPCBackupConfig(entityDBProxy,
        instanceServiceFactory.getClusterUuidFromZeusConfig());
  }

  @Retryable(
      maxAttempts = MAX_ATTEMPT_FOR_BACKUP_CONFIG_FETCH,
      value = PCResilienceException.class,
      backoff = @Backoff(delay = RETRY_DELAY_FOR_FETCHING_CONFIG_MS))
  public ObjectStoreCredentialsBackupEntity fetchExistingObjectStoreEndpointInPCBackupConfigWithCredentials()
      throws PCResilienceException {
    log.debug("Trying fetching existing object store endpoint list in " +
              "pc_backup_config table with credentials");
    return PCUtil.fetchExistingObjectStoreEndpointInPCBackupConfigWithCredentials(entityDBProxy,
        instanceServiceFactory.getClusterUuidFromZeusConfig());
  }

  @Retryable(
      maxAttempts = MAX_ATTEMPT_FOR_BACKUP_CONFIG_FETCH,
      value = PCResilienceException.class,
      backoff = @Backoff(delay = RETRY_DELAY_FOR_FETCHING_CONFIG_MS))
  public ClusterExternalStateProto.PcBackupConfig
    fetchPCBackupConfigWithEntityId(String entityID) throws PCResilienceException {
    log.debug("Trying fetching existing cluster uuid in pc_backup_config table.");
    return PCUtil.fetchPCBackupConfigWithEntityId(entityDBProxy, entityID);
  }
}
