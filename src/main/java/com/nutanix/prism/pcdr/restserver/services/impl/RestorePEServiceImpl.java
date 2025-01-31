package com.nutanix.prism.pcdr.restserver.services.impl;

import com.google.common.collect.Lists;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.ClusterExternalState;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.messages.APIErrorMessages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.clients.RecoverPcProxyClient;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.services.api.RestorePEService;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.restserver.util.QueryUtil;
import com.nutanix.prism.pcdr.util.ClusterExternalStateHelper;
import com.nutanix.prism.pcdr.util.IDFUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import com.nutanix.prism.pcdr.zklock.DistributedLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Slf4j
@Service
public class RestorePEServiceImpl implements RestorePEService {

  @Value("${prism.pcdr.min.supported.version:5.20.1}")
  private String pcdrMinPeSupportedVersion;
  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private PCVMDataService pcvmDataService;
  @Autowired
  private ClusterExternalStateHelper clusterExternalStateHelper;
  @Autowired
  private RecoverPcProxyClient recoverPcProxyClient;
  @Autowired
  @Qualifier("adonisServiceThreadPool")
  private ExecutorService adonisServiceThreadPool;

  @Value("${prism.pcdr.peClusterDataCleanup.expiry.time.days:30}")
  private int peClusterDataExpiryDays;
  @Value("${prism.pcdr.peClusterDataCleanup.concurrentRequestCount:10}")
  private int concurrentPeRequestCount;

  private static final String NO_DATA_ZK_VALUE= "NoData";

  public void cleanIdfOnRegisteredPEs() throws PCResilienceException {
    Map<String, List<String>> peUuidMap = getPEListMapFromIdfCluster();

    if (!peUuidMap.get(Constants.NOT_SUPPORTED_TAG).isEmpty()) {
      log.warn("Following clusters cannot be reset for sync as they have " +
               "older PE version. Please clear cluster data states manually " +
               "using script");
      log.warn(peUuidMap.get(Constants.NOT_SUPPORTED_TAG).toString());
      //TODO: RAISE AN ALERT MAYBE.
    }
    DistributedLock distributedLock =
        instanceServiceFactory.getDistributedLockForPECleanupPCDR();

    try {
      if (!distributedLock.lock(false)) {
        log.info("Unable to acquire lock, arithmos sync cleanup already" +
                 " in process. Skipping on this node.");
        return;
      }
      List<String> peUuidsInPCDRZkNode = getPeUuidsInPCDRZkNode();
      List<String> supportedPeUuids = peUuidMap.get(Constants.SUPPORTED_TAG);
      List<String> filteredPeUuid =
          peUuidsInPCDRZkNode.stream().distinct()
                             .filter(supportedPeUuids::contains)
                             .collect(Collectors.toList());
      if (filteredPeUuid.isEmpty()) {
        log.info("No svm available to cleanup sync states, hence marking" +
                 " the sync state completed to true.");
        return;
      }
      log.info("Following is the list of PEs for which arithmos sync cleanup is to be done: "
               + filteredPeUuid.toString());

      Map<String, String> peUuidIPMap= null;
      try {
        peUuidIPMap = getPeIPFromClusterExternalState(
              filteredPeUuid);
      }
      catch (PCResilienceException e) {
        log.error("Error in fetching the registered clusters: ", e);
        throw e;
      }
      List<String> retryLaterPeUuidList = new LinkedList<>();

      for (List<String> peUuidSubset : Lists.partition(filteredPeUuid,
                                                       concurrentPeRequestCount)) {
        retryLaterPeUuidList
            .addAll(executeCleanupOnPes(peUuidSubset, peUuidIPMap));
        log.info("Arithmos sync pe cleanup for batch complete");
      }
      //Even if the list is empty, we should update zknode so that an
      //empty list stops the scheduler.
      if(!retryLaterPeUuidList.isEmpty())
        log.info("Arithmos sync cleanup was not successful for pes: "+ retryLaterPeUuidList);
      updatePeUuidsInZkNode(retryLaterPeUuidList);
    } finally {
      distributedLock.unlock();
    }
    log.info("Successfully completed arithmos sync cleanup on all SvmIP's.");
  }

  public void updatePeUuidsInZkNode(List<String> peUuids){
    String peUuidsSerialized;
    if(peUuids.isEmpty())
      peUuidsSerialized = NO_DATA_ZK_VALUE;
    else
      peUuidsSerialized = String.join(",", peUuids);
    try {
      zookeeperServiceHelper.setData(Constants.PECLEANUP_ZK_NODE,
                       peUuidsSerialized.getBytes(),
                       -1);
    } catch (final KeeperException.BadVersionException |
        KeeperException.NoNodeException | ZkClientConnectException e) {
      log.error(APIErrorMessages.ZK_UPDATE_ERROR, Constants.PECLEANUP_ZK_NODE, e);
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      log.error(APIErrorMessages.ZK_UPDATE_ERROR, Constants.PECLEANUP_ZK_NODE, e);
    }
  }

  public List<String> getPeUuidsInPCDRZkNode(){
    try {
      Stat stat = new Stat();
      LinkedList<String> peList = new LinkedList<>();
      byte[] data = zookeeperServiceHelper.getData(Constants.PECLEANUP_ZK_NODE, stat);
      long timeNow = System.currentTimeMillis();
      long timeModified = stat.getMtime();
      if (timeNow > timeModified + TimeUnit.MILLISECONDS.convert(
          peClusterDataExpiryDays, TimeUnit.DAYS)) {
        return peList;
      }
      String serializedPeList = new String(data);
      if(serializedPeList.equals(NO_DATA_ZK_VALUE))
        return peList;
      else
        return Arrays.asList(serializedPeList.split(","));
    } catch (final KeeperException.NoNodeException | ZkClientConnectException e) {
      log.error(APIErrorMessages.ZK_READ_ERROR, Constants.PECLEANUP_ZK_NODE, e);
    } catch (Exception e){
      Thread.currentThread().interrupt();
      log.error(APIErrorMessages.ZK_READ_ERROR);
    }
    return new LinkedList<>();
  }

  public void setPeUuidsInPCDRZkNode(List<String> peUuids){
    String peUuidsSerialized;
    if(peUuids.isEmpty())
      peUuidsSerialized = NO_DATA_ZK_VALUE;
    else
      peUuidsSerialized = String.join(",", peUuids);

    try {
      final Stat stat = zookeeperServiceHelper.exists(Constants.PECLEANUP_ZK_NODE, false);
      if (stat == null) {
        zookeeperServiceHelper.createZkNode(Constants.PECLEANUP_ZK_NODE,
                              peUuidsSerialized.getBytes(),
                              ZooDefs.Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
      }
      else {
        zookeeperServiceHelper.setData(Constants.PECLEANUP_ZK_NODE,
                         peUuidsSerialized.getBytes(),
                         -1);
      }
    } catch (final KeeperException.NodeExistsException
      | KeeperException.BadVersionException | KeeperException.NoNodeException
      | ZkClientConnectException e) {
      log.error(APIErrorMessages.ZK_CREATE_ERROR, Constants.PECLEANUP_ZK_NODE, e);
    } catch (Exception e) {
      Thread.currentThread().interrupt();
      log.error(APIErrorMessages.ZK_CREATE_ERROR, Constants.PECLEANUP_ZK_NODE, e);
    }
  }

  public Map<String, String> getPeIPFromClusterExternalState(
    List<String> clusterUuidList) throws PCResilienceException {
    List<String> registeredClusters = pcvmDataService.getPeUuidsOnPCClusterExternalState();
    final Map<String, ClusterExternalState> remoteClusterStates = new HashMap<>();
    for(String clusterId : registeredClusters) {
      ClusterExternalState clusterExternalState = clusterExternalStateHelper.getClusterExternalState(clusterId);
      if ( clusterExternalState != null )
       remoteClusterStates.put(clusterId, clusterExternalState);
    }
    Map<String, String> clusterIpMap = new HashMap<>();

    for (String clusterUuid : clusterUuidList) {

      String ip = remoteClusterStates
        .get(clusterUuid)
        .getClusterDetails()
        .getContactInfo()
        .getAddressList(0);

      log.debug("Found IP address: {} for cluster-uuid: {} to cleanup PE IDF",
        ip, clusterUuid);
      clusterIpMap.put(clusterUuid, ip);
    }
    return clusterIpMap;
  }

  public List<String> executeCleanupOnPes(List<String> uuidIpList,
    Map<String, String> peUuidIpMap) throws PCResilienceException {
    List<CompletableFuture<Object>> allFutures = new ArrayList<>();
    LinkedBlockingDeque<String> failedRequestBlockingQueue =
        new LinkedBlockingDeque<>();

    for (String peUuid : uuidIpList) {
      String svmIp = peUuidIpMap.get(peUuid);
      log.info("Making request for arithmos sync cleanup after restore on SVM IP {} " +
        "with cluster id: {}", svmIp, peUuid);
      // The files data needs to be base64 encoded
      allFutures.add(CompletableFuture
        .supplyAsync(
            () -> recoverPcProxyClient.resetArithmosSyncAPICallOnPE(svmIp), adonisServiceThreadPool)
        .handle((result, ex) -> {
          if (ex != null) {
            log.error(
              "Unable to make arithmos sync cleanup request to PE {} " +
                "due to ",
              svmIp, ex);
            failedRequestBlockingQueue.add(svmIp);
          }
          return null;
        }));
    }

    CompletableFuture.allOf(allFutures
      .toArray(new CompletableFuture[0])).join();
    List<String> failedRequestList = new ArrayList<>();
    failedRequestBlockingQueue.drainTo(failedRequestList);
    return failedRequestList;
  }

  /**
   * Filters the list of clusters which are eligible for PC data backup.
   *
   * @return An eligible cluster list object.
   */
  public Map<String, List<String>> getPEListMapFromIdfCluster() {
    // Get the results of PE from IDF.
    InsightsInterfaceProto.GetEntitiesWithMetricsRet result;
    Map<String, List<String>> clusterMap = new HashMap<>();

    result = this.getPEClusterUuidMetricDataFromClusterIDF();
    if(result == null){
      return clusterMap;
    }
    clusterMap.put(Constants.SUPPORTED_TAG, new ArrayList<>());
    clusterMap.put(Constants.NOT_SUPPORTED_TAG, new ArrayList<>());

    // Loop to start feeding data to eligibleClusterList from IDF result.
    for (InsightsInterfaceProto.QueryGroupResult group : result
        .getGroupResultsListList()) {
      for (InsightsInterfaceProto.EntityWithMetricAndLookup entity : group
          .getLookupQueryResultsList()) {

        final String peVersion = Objects.requireNonNull(IDFUtil
           .getAttributeValueInEntityMetric(Constants.CLUSTER_VERSION,
                                            entity.getEntityWithMetrics()),
           "Version field in Cluster should not be null.").getStrValue();

        final String clusterUuid = Objects.requireNonNull(IDFUtil
          .getAttributeValueInEntityMetric(Constants.CLUSTER_UUID,
                                           entity.getEntityWithMetrics()),
          "cluster_uuid field in Cluster should not be null.").getStrValue();

        log.debug(String.format("Checking version compatibility for PE " +
          "cluster_uuid:%s with version:%s", clusterUuid, peVersion));

        // Checking the version compatibility for PC Backup
        if (PCUtil.comparePEVersions(peVersion,
                                     pcdrMinPeSupportedVersion)) {
          log.debug(String.format("PE %s is compatible", clusterUuid));
          clusterMap.get(Constants.SUPPORTED_TAG).add(clusterUuid);
        }
        else {
          log.debug(String.format("PE %s is not compatible", clusterUuid));
          clusterMap.get(Constants.NOT_SUPPORTED_TAG).add(clusterUuid);
        }

      }
    }

    return clusterMap;
  }

  /**
   * Filters the PE entities from cluster entity_type and sorts it in
   * a sortorder given for ssd free space.
   *
   * @return - cluster entities from IDF.
   */
  private InsightsInterfaceProto.GetEntitiesWithMetricsRet
    getPEClusterUuidMetricDataFromClusterIDF() {
    InsightsInterfaceProto.GetEntitiesWithMetricsRet result = null;
    try {
      result =
          entityDBProxy.getEntitiesWithMetrics(
              InsightsInterfaceProto.GetEntitiesWithMetricsArg
                  .newBuilder()
                  .setQuery(QueryUtil.constructEligibleClusterListQuery(
                    InsightsInterfaceProto.QueryOrderBy.SortOrder.kDescending,
                    InsightsInterfaceProto.QueryOrderBy.SortKey.kLatest)
                           ).build());
    } catch (InsightsInterfaceException e) {
      log.error(String.format(Constants.QUERY_IDF_FAILED, "PE Data"), e);
      log.debug(APIErrorMessages.DEBUG_START, e);
      List<String> messageList = new ArrayList<>();
      messageList.add("Failed to get Eligible cluster list due to " +
                      "database error.");
    }
    return result;
  }

}
