package com.nutanix.prism.pcdr.restserver.services.impl;


import static com.nutanix.prism.pcdr.constants.Constants.*;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.PCDR_ENDPOINT_ADDRESS_SPLIT_REGEX;
import static com.nutanix.prism.pcdr.constants.Constants.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.nutanix.dp1.lcmroot.lifecycle.v4.resources.Entity;

import com.google.protobuf.ByteString;
import com.nutanix.dp1.lcmroot.lifecycle.v4.resources.ListEntitiesApiResponse;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto.BatchUpdateEntitiesArg;
import com.nutanix.insights.ifc.InsightsInterfaceProto.DataValue;
import com.nutanix.insights.ifc.InsightsInterfaceProto.DeleteClusterReplicationStateArg;
import com.nutanix.insights.ifc.InsightsInterfaceProto.DeleteClusterReplicationStateRet;
import com.nutanix.insights.ifc.InsightsInterfaceProto.DeleteEntityArg;
import com.nutanix.insights.ifc.InsightsInterfaceProto.EntityGuid;
import com.nutanix.insights.ifc.InsightsInterfaceProto.EntityWithMetric;
import com.nutanix.insights.ifc.InsightsInterfaceProto.EntityWithMetricAndLookup;
import com.nutanix.insights.ifc.InsightsInterfaceProto.GetEntitiesWithMetricsArg;
import com.nutanix.insights.ifc.InsightsInterfaceProto.GetEntitiesWithMetricsRet;
import com.nutanix.insights.ifc.InsightsInterfaceProto.Query;
import com.nutanix.insights.ifc.InsightsInterfaceProto.QueryGroupResult;
import com.nutanix.insights.ifc.InsightsInterfaceProto.QueryOrderBy.SortKey;
import com.nutanix.insights.ifc.InsightsInterfaceProto.QueryOrderBy.SortOrder;
import com.nutanix.insights.ifc.InsightsInterfaceProto.UpdateEntityArg;
import com.nutanix.lcmroot.java.client.ApiClient;
import com.nutanix.lcmroot.java.client.api.EntitiesApi;

import com.nutanix.prism.adapter.service.ZeusConfiguration;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig.BackupType;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig.ObjectStoreEndpoint;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig.ObjectStoreEndpoint.EndpointFlavour;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.exception.MantleException;
import com.nutanix.prism.pcdr.PcBackupMetadataProto;
import com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMBackupTargets;
import com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMBackupTargets.ObjectStoreBackupTarget;
import com.nutanix.prism.pcdr.PcBackupSpecsProto;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.*;
import com.nutanix.prism.pcdr.exceptions.*;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.factory.ObjectStoreHelperFactory;
import com.nutanix.prism.pcdr.messages.APIErrorMessages;
import com.nutanix.prism.pcdr.messages.Messages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.clients.InsightsFanoutClient;
import com.nutanix.prism.pcdr.restserver.clients.MercuryFanoutRpcClient;
import com.nutanix.prism.pcdr.restserver.converters.PrismCentralBackupConverter;
import com.nutanix.prism.pcdr.restserver.dto.ObjectStoreCredentialsBackupEntity;
import com.nutanix.prism.pcdr.restserver.services.api.RunnableWithCheckedException;
import com.nutanix.prism.pcdr.restserver.services.api.BackupEntitiesCalculator;
import com.nutanix.prism.pcdr.restserver.services.api.BackupStatus;
import com.nutanix.prism.pcdr.restserver.services.api.ObjectStoreBackupService;
import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMZkService;
import com.nutanix.prism.pcdr.restserver.services.api.PulsePublisherService;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.EnableCmspBasedServices;
import com.nutanix.prism.pcdr.restserver.util.*;
import com.nutanix.prism.pcdr.restserver.zk.PCUpgradeWatcher;
import com.nutanix.prism.pcdr.services.ClusterHelper;
import com.nutanix.prism.pcdr.services.ObjectStoreHelper;
import com.nutanix.prism.service.ErgonService;
import com.nutanix.prism.pcdr.util.*;
import com.nutanix.prism.util.CompressionUtils;
import dp1.pri.prism.v4.management.*;
import dp1.pri.prism.v4.protectpc.*;

import dp1.pri.prism.v4.protectpc.BackupTargetsInfo;
import dp1.pri.prism.v4.protectpc.EligibleCluster;
import dp1.pri.prism.v4.protectpc.EligibleClusterList;
import dp1.pri.prism.v4.protectpc.ObjectStoreEndpointInfo;
import dp1.pri.prism.v4.protectpc.PEInfo;
import dp1.pri.prism.v4.protectpc.PcEndpointCredentials;
import dp1.pri.prism.v4.protectpc.PcEndpointFlavour;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import dp1.pri.prism.v4.protectpc.RpoConfig;

import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;

import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;
import static com.nutanix.zeus.protobuf.Configuration.ConfigurationProto.PCClusterInfo.NetworkType.kDualStack;

import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonInterface.TaskListRet;
import nutanix.ergon.ErgonTypes;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Slf4j
@Service
public class PCBackupServiceImpl implements PCBackupService {

  private static BiMap<List, String> lcmGenesisMapping = HashBiMap.create();
  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private PulsePublisherService pulsePublisherService;
  @Autowired
  private PCRetryHelper pcRetryHelper;
  @Autowired
  private BackupStatus backupStatus;
  @Autowired
  private PCVMDataService pcvmDataService;
  @Autowired
  private PCVMZkService pcvmZkService;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private BackupEntitiesCalculator backupEntitiesCalculator;
  @Autowired
  ClusterHelper clusterHelper;
  @Autowired
  private PCTaskServiceHelper pcTaskServiceHelper;
  @Autowired
  private CertFileUtil certFileUtil;
  @Autowired
  private PrismCentralBackupConverter prismCentralBackupConverter;
  @Autowired
  @Qualifier("pcdr.ergon")
  private ErgonService ergonService;
  @Value("${prism.pcdr.enabled.version:6.0}")
  private String pcdrEnabledVersion;
  /** This is the AOS version that would support deploying a default CMSP
   enabled PC(2022.9). This is the AOS version that will support PCDR
   recovery as well because recovery involves deployment.
   */
  @Value("${prism.pcdr.cmsp.enabled.aos.version:6.5.3}")
  private String pcdrCmspEnabledVersion;
  @Value("${prism.pcdr.min.supported.version:5.20.1}")
  private String pcdrMinPeSupportedVersion;
  @Autowired
  private EnableCmspBasedServices enableCmspBasedServices;
  @Autowired
  private FlagHelperUtil flagHelperUtil;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;
  @Autowired
  private InsightsFanoutClient insightsFanoutClient;
  @Autowired
  private ObjectStoreBackupService objectStoreBackupService;
  @Autowired
  private PCUpgradeWatcher pcUpgradeWatcher;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private MantleUtils mantleUtils;
  @Autowired
  @Qualifier("adonisServiceThreadPool")
  private ExecutorService adonisServiceThreadPool;

  @Autowired
  private ApiClientGenerator apiClientGenerator;

  @Autowired
  private PCDRYamlAdapterImpl pcdrYamlAdapter;

  @PostConstruct
  public void init() {
    lcmGenesisMapping = createLcmGenesisVersionMapping(pcdrYamlAdapter);
  }

  public BiMap<List, String> getlcmGenesisMapping(){
    return lcmGenesisMapping;
  }

  @VisibleForTesting
  public void init(PCRetryHelper pcRetryHelper) {
    this.pcRetryHelper = pcRetryHelper;
  }

  /**
   * Filters the list of clusters which are eligible for PC data backup.
   *
   * @return An eligible cluster list object.
   */
  public EligibleClusterList getEligibleClusterList(boolean isSortingRequired)
          throws PCResilienceException {
    // Creating eligible cluster empty list to add eligible clusters from IDF.
    EligibleClusterList eligibleClusterListDTO = new EligibleClusterList();
    // Get the results of PE from IDF.
    GetEntitiesWithMetricsRet result = this.fetchPEClusterListFromIDF();
    List<EligibleCluster> eligibleClusterList = new ArrayList<>();
    String hostingPEUuid = pcvmDataService.getHostingPEUuid();
    //todo : hostingPEUuid != null check is not required as from above function it will always be returned non null
    if (hostingPEUuid != null && pcvmDataService.isHostingPEHyperV(
            hostingPEUuid)) {
      log.info("Returning eligible cluster as empty as hosting PE is HYPER-V");
      return eligibleClusterListDTO;
    }
    boolean isPcdrSupported = false;
    // If the entities for backup count will be zero if the backup limit feat
    // is not enabled.
    long entitiesToBackupCount = isSortingRequired ? getEntitiesToBackupTotalCount() : 0L;

    // Loop to start feeding data to eligibleClusterList from IDF result.
    for (QueryGroupResult group : result.getGroupResultsListList()) {
      long numNodes = group.getGroupByColumnValue().getInt64Value();
      // The rows are grouped by num_nodes
      log.debug("Traversing the group with column (num_nodes) value - {}",
              numNodes);
      for (EntityWithMetricAndLookup entity : group.getLookupQueryResultsList()) {

        final String peVersion = Objects.requireNonNull(
                        IDFUtil
                                .getAttributeValueInEntityMetric(Constants.CLUSTER_VERSION,
                                        entity.getEntityWithMetrics()),
                        "Version field in Cluster should not be null.")
                .getStrValue();
        final String clusterName = Objects.requireNonNull(
                        IDFUtil
                                .getAttributeValueInEntityMetric(Constants.CLUSTER_NAME,
                                        entity.getEntityWithMetrics()),
                        "cluster_name field in Cluster should not be null.")
                .getStrValue();
        final String clusterUuid = Objects.requireNonNull(
                        IDFUtil
                                .getAttributeValueInEntityMetric(Constants.CLUSTER_UUID,
                                        entity.getEntityWithMetrics()),
                        "cluster_uuid field in Cluster should not be null.")
                .getStrValue();

        log.debug(String.format("Checking version compatibility for PE " +
                        "cluster_name:%s and cluster_uuid:%s",
                clusterName, clusterUuid));
        if (!PCUtil.comparePEVersions(peVersion, pcdrMinPeSupportedVersion)) {
          // Check if any of the PE matches does not match the min supported
          // version, if not return empty eligibleClusterList
          eligibleClusterListDTO.setEligibleClusterList(Collections.emptyList());
          return eligibleClusterListDTO;
        }
        if (PCUtil.comparePEVersions(peVersion, pcdrCmspEnabledVersion)) {
          // Support pcdr only if there is atleast one
          // registered PE of version 6.5.3
          isPcdrSupported = true;
        }
        long computeOnlyNodeCount = PCUtil.getComputeOnlyNodeCount(entity);
        // The sorting logic is updated based on number of entities supported
        // by each PE. So, we even if the backupLimit feat is not enabled we
        // will sort the replica PEs based on number of nodes and version
        // count supported by them.
        updateEligibleClusterList(eligibleClusterList, peVersion,
                numNodes - computeOnlyNodeCount,
                clusterName, clusterUuid, hostingPEUuid,
                entitiesToBackupCount);
      }
    }

    if (!isPcdrSupported){
      // If there is no registered PE of version 6.5.3 or above, if not
      // return an empty eligibleClusterList
      eligibleClusterListDTO.setEligibleClusterList(Collections.emptyList());
      return eligibleClusterListDTO;
    }
    // Sort according to the number of backed up entity count which are
    // supported by AOS.
    eligibleClusterList.sort(Comparator.comparing(
            EligibleCluster::getBackedUpEntitiesCountSupported).reversed());
    // Set the eligibleClusterList to the eligibleClusterListDTO.
    eligibleClusterListDTO.setEligibleClusterList(eligibleClusterList);
    log.debug("Successfully created EligibleClusterList object {}.",
            eligibleClusterListDTO);

    return eligibleClusterListDTO;
  }

  /**
   * Update the Eligible cluster list with eligibleClusters. This method also
   * filters out the PEs which exceeds the already existing entities present
   * in on the IDF required for backup.
   *
   * @param eligibleClusterList   - list to be updated
   * @param peVersion             - version of the PE
   * @param numCvmNodes           - number of nodes in PE
   * @param clusterName           - name of the PE
   * @param clusterUuid           - uuid of the PE
   * @param hostingPEUuid         - boolean telling if it is hosting PE or not
   * @param entitiesToBackupCount - number of entities to backup
   */
  private void  updateEligibleClusterList(List<EligibleCluster> eligibleClusterList,
                                          String peVersion,
                                          long numCvmNodes,
                                          String clusterName,
                                          String clusterUuid,
                                          String hostingPEUuid,
                                          long entitiesToBackupCount) {
    // Checking the version compatibility for PC Backup
    if (PCUtil.comparePEVersions(peVersion, pcdrEnabledVersion)) {
      long backupEntitiesCountSupported =
              backupEntitiesCalculator.getMaxBackupEntitiesSupportedForAOS(
                      peVersion, numCvmNodes);
      // entitiesToBackupCount will be 0 in-case the backup limit feat is not
      // enabled. And all the PEs in the list will be compatible.
      // If backupEntitiesCountSupported by a PE is less than the already
      // existing entity count which are going to be backed up that means
      // that PE is not eligible for backup.
      if (entitiesToBackupCount > backupEntitiesCountSupported) {
        log.warn(String.format("Current PE is incompatible due to backup " +
                        "count of entities exceeding the supported" +
                        " possible count, details: " +
                        "cluster_name:%s and cluster_uuid:%s and " +
                        "supportedCount:%d", clusterName,
                clusterUuid, backupEntitiesCountSupported));
      }
      else {
        log.debug(String.format("Current PE is compatible, details: " +
                        "cluster_name:%s and cluster_uuid:%s",
                clusterName, clusterUuid));
        EligibleCluster eligibleCluster = new EligibleCluster();
        eligibleCluster.setClusterName(clusterName);
        eligibleCluster.setClusterUuid(clusterUuid);
        eligibleCluster.setIsHostingPe(clusterUuid.equals(hostingPEUuid));
        eligibleCluster.setBackedUpEntitiesCountSupported(
                backupEntitiesCountSupported);
        eligibleClusterList.add(eligibleCluster);
      }
    }
  }

  /**
   * Filters the PE entities from cluster entity_type and sorts it in
   * a sortorder given for ssd free space.
   *
   * @return - cluster entities from IDF.
   */
  public GetEntitiesWithMetricsRet fetchPEClusterListFromIDF()
          throws PCResilienceException {
    GetEntitiesWithMetricsRet result;
    try {
      result =
              entityDBProxy.getEntitiesWithMetrics(
                      GetEntitiesWithMetricsArg
                              .newBuilder().setQuery(
                                      QueryUtil.constructEligibleClusterListQuery(
                                              SortOrder.kDescending, SortKey.kLatest)
                              ).build());
    }
    catch (InsightsInterfaceException e) {
      log.error(String.format(Constants.QUERY_IDF_FAILED, "PE Data"), e);
      log.debug(APIErrorMessages.DEBUG_START, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_DATABASE_READ_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return result;
  }

  /**
   * Fetch all the entries of pc_backup_specs from IDF.
   *
   * @return - returns GetEntitiesWithMetricsRet containing pc_backup_specs
   * entries.
   */
  public GetEntitiesWithMetricsRet fetchPCBackupSpecsFromIDF() throws PCResilienceException {
    GetEntitiesWithMetricsRet result;
    try {
      result =
              entityDBProxy.getEntitiesWithMetrics(
                      GetEntitiesWithMetricsArg
                              .newBuilder()
                              .setQuery(QueryUtil.constructPcBackupSpecsProtoQuery())
                              .build());
    }
    catch (InsightsInterfaceException e) {
      log.error(String.format(Constants.QUERY_IDF_FAILED,
              Constants.PC_BACKUP_SPECS_TABLE), e);
      log.debug(APIErrorMessages.DEBUG_START, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_DATABASE_READ_BACKUP_SPECS_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return result;
  }

  /**
   * Incase it is not able to fetch the entities count the method returns 0
   *
   * @return - returns the total entities required to be backed up count on PE.
   */
  private long getEntitiesToBackupTotalCount() {
    long entitiesToBackupCount = 0;
    // If backup limit feat is not enabled then there is no need to fetch the
    // number of entities present in the IDF, we can directly return 0 and
    // based on 0 entities rest of the checks will take place.
    if (!flagHelperUtil.isBackupLimitFeatEnabled()) {
      return entitiesToBackupCount;
    }
    try {
      // If backupEntityList is empty then update it
      if (backupEntitiesCalculator.getBackupEntityList().isEmpty()) {
        backupEntitiesCalculator.updateBackupEntityList();
      }
      entitiesToBackupCount =
              backupEntitiesCalculator.getTotalBackupEntityCount();
    }
    catch (EntityCalculationException e) {
      log.warn("Unable to count backup entities, showing all the PEs allowed " +
              "for backup.", e);
    }
    return entitiesToBackupCount;
  }

  /**
   * Fetch all the entries of pc_zk_data from IDF.
   *
   * @return - returns GetEntitiesWithMetricsRet containing pc_zk_data
   * entries.
   */
  public GetEntitiesWithMetricsRet fetchPCZkDataFromIDF()
          throws PCResilienceException {
    GetEntitiesWithMetricsRet result;
    try {
      result =
              entityDBProxy.getEntitiesWithMetrics(
                      GetEntitiesWithMetricsArg
                              .newBuilder()
                              .setQuery(QueryUtil.constructPCZkDataQuery())
                              .build());
    }
    catch (InsightsInterfaceException e) {
      log.error(String.format(Constants.QUERY_IDF_FAILED,
              PC_ZK_DATA, e));
      log.debug(APIErrorMessages.DEBUG_START, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_DATABASE_READ_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return result;
  }

  /**
   * Get entityIds given GetEntitiesWithMetricsRet.
   * <p>
   * IDF returns GetEntitiesWithMetricsRet for read query. Get Entities IDs
   * from the query response.
   *
   * @param getEntitiesWithMetricsRet - Response from IDF read query.
   * @return - List of String contains entityIds in GetEntitiesWithMetricsRet
   */
  private List<String> getEntityIdsFromGetEntitiesWithMetrics(
          GetEntitiesWithMetricsRet getEntitiesWithMetricsRet) {
    List<QueryGroupResult> queryGroupResultList =
            getEntitiesWithMetricsRet.getGroupResultsListList();
    List<String> entityUuidList = new ArrayList<>();
    for (QueryGroupResult queryGroupResult : queryGroupResultList) {
      List<EntityWithMetric> entityWithMetrics =
              queryGroupResult.getRawResultsList();
      for (EntityWithMetric entityWithMetric : entityWithMetrics) {
        EntityGuid entityGuid = entityWithMetric.getEntityGuid();
        String entityId = entityGuid.getEntityId();
        entityUuidList.add(entityId);
        entityGuid.getEntityTypeName();
      }
    }
    return entityUuidList;
  }

  /**
   * Modify the peClusterUuidSet to contain only the entries which are not
   * present in IDF table pc_backup_config, so that there is no duplicacy of
   * data while feeding it to IDF.
   *
   * @param peClusterUuidSet            - Set of PE cluster UUid given input in API
   *                                    request.
   * @param clusterUuidInPCBackupConfig - List of clusterUuid present in
   *                                    pc_backup_config table in IDF.
   */
  private void modifyWithoutPCBackupConfigEntries(
          final Set<String> peClusterUuidSet,
          List<String> clusterUuidInPCBackupConfig) {
    // Removing the clusters from peClusterUuidSet which are already present
    // in pc_backup_config table.
    peClusterUuidSet.removeIf(clusterUuidInPCBackupConfig::contains);
    log.debug("PE cluster uuid set after removing pc backup config uuids - " +
            "{}", peClusterUuidSet);
  }

  /**
   * Modify the objectStoreEndpointList to contain only the entries which are not
   * present in IDF table pc_backup_config, so that there is no duplicate
   * data while feeding it to IDF.
   *
   * @param objectStoreEndpointList         - List of ObjectStoreEndpoint given input in API
   *                                        request.
   * @param objectStoreEndpointInPCBackupConfig - List of object store
   *                                            endpoint present in
   *                                            pc_backup_config table in IDF.
   */
  protected void modifyWithoutPCBackupConfigObjectStoreEntries(
          final List<PcObjectStoreEndpoint> objectStoreEndpointList,
          List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig) {

    // For Nutanix Objects backup entity added with IP and FQDN/different FQDN will be considered different
    // and will have two different backup configs, even if both IP and FQDN points to same Nutanix Objects.

    // Removing the object store endpoints  from objectStoreEndpointList which
    // are already present in pc_backup_config table.
    objectStoreEndpointList.removeIf(p2 -> objectStoreEndpointInPCBackupConfig.stream().anyMatch
            (p1 -> p1.getEndpointAddress().equals(p2.getEndpointAddress())));

  }

  /**
   * Function to add the replica PE or object store endpoint to the
   * pc_backup_config table
   * Subtasks -
   * 1. Checks whether all the clusterUuid given are part of eligibleClusters
   * in case of PE cluster uuids.
   * 2. Filters out the cluster uuid and object store endpoints which are
   * already present in pc_backup_config.
   * 3. Checks whether the total number of entries are not more than
   * MAX_ALLOWED_REPLICA_PE for cluster uuid and MAX_ALLOWED_REPLICA_OBJECT_STORE
   * for object store endpoint value.
   * 4. If everything goes right, then update the pc_backup_config table.
   * Order of addition of entities -
   * 1. pc_zk_data+pc_backup_specs
   * 2. pc_backup_metadata
   * 3. pc_backup_config
   *
   * @param peClusterUuidSet - peClusterUuid set given as input in API request.
   */
  @Override
  public void addReplicas(final Set<String> peClusterUuidSet,
                          List<PcObjectStoreEndpoint> objectStoreEndpointList,
                          ByteString taskId)
          throws PCResilienceException, ErgonException {

    Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap = pcvmDataService.
            getObjectStoreEndpointDtoMapFromPcObjectStoreEndpointList(objectStoreEndpointList);

    ZeusConfiguration zeusConfig = instanceServiceFactory.getZeusConfig();
    String networkType = clusterHelper.getNetworkType(zeusConfig);
    if (networkType.equals(kDualStack.toString())) {
      log.error(ErrorMessages.BACKUP_NOT_SUPPORTED_ON_DUAL_STACK_PC);
      throw new PCResilienceException(ErrorMessages.BACKUP_NOT_SUPPORTED_ON_DUAL_STACK_PC, ErrorCode.PCBR_BACKUP_NOT_SUPPORTED_ON_DUAL_STACK_PC,
              HttpStatus.BAD_REQUEST);
    }

    if (pcUpgradeWatcher.isUpgradeInProgress()) {
      log.warn("Upgrade is in progress, not proceeding with addReplicas");
      throw new PCResilienceException(ErrorMessages.ADD_REPLICA_UPGRADE_IN_PROGRESS_ERROR,
              ErrorCode.PCBR_UPGRADE_IN_PROGRESS,HttpStatus.SERVICE_UNAVAILABLE);
    }

    try{
      TaskListRet taskListRet = ergonServiceHelper
              .fetchTaskListRet(
                      Constants.TaskOperationType.kPCRestore.name(), false, PCDR_TASK_COMPONENT_TYPE);
      if (taskListRet.getTaskUuidListCount() > 0) {
        log.info("Restore task already in progress. not proceeding with addReplicas");
        throw new PCResilienceException(ErrorMessages.ADD_REPLICA_RESTORE_ALREADY_IN_PROGRESS_ERROR,
                ErrorCode.PCBR_RESTORE_IN_PROGRESS, HttpStatus.SERVICE_UNAVAILABLE);
      }
    }
    catch (ErgonException e) {
      log.error("Unable to fetch task from Ergon",e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    // STEP - 1
    // Verifying whether the clusters sent in the request are part of
    // eligible clusters or not. If any one of them is not a part raise an
    // exception.
    checkIfPesEligibleForBackup(peClusterUuidSet, false);

    pcTaskServiceHelper.updateTask(taskId, 10);


    // STEP - 2: Modify the peClusterUuidSet and objectStoreEndpointList.
    // If nothing is left after checking from pc_backup_config
    // throw an exception.
    // Getting all the elements present in pc_backup_config from IDF.
    List<String> clusterUuidInPCBackupConfig = getClusterUuidInBackupConfig(peClusterUuidSet);

    ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntityInPCBackupConfig =
            getObjectStoreCredentialsBackupEntityInPCBackupConfig(objectStoreEndpointList);
    List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig =
            objectStoreCredentialsBackupEntityInPCBackupConfig.getObjectStoreEndpoints();

    log.info("Object store endpoints from backup config  - {}",
            objectStoreEndpointInPCBackupConfig);
    String objectStoreEndpoints = objectStoreEndpointList.toString();
    objectStoreEndpoints = DataMaskUtil.maskSecrets(objectStoreEndpoints);


    log.info("Received object store endpoints - {}", objectStoreEndpoints);

    // If nothing is left after checking from pc_backup_config
    // throw an exception.
    if (peClusterUuidSet.isEmpty() && objectStoreEndpointList.isEmpty()) {
      String errorMessage = "Nothing to add in pc_backup_config table. " +
              "The ClusterUuids/ObjectStoreEndpoints sent are " +
              "already present in the database";
      log.warn(errorMessage);
      log.debug("PC backup config contains following cluster uuid : " +
              clusterUuidInPCBackupConfig.toString());
      throw new PCResilienceException(ErrorMessages.ADD_REPLICA_BACKUP_TARGET_ALREADY_PRESENT_ERROR,
              ErrorCode.PCBR_ALREADY_PROTECTED,HttpStatus.BAD_REQUEST);
    }

    // STEP -3
    // Checking the total count of replicas does not exceed maximum allowed
    // cluster value.
    checkAllowedReplicasCount(
            peClusterUuidSet, objectStoreEndpointList,
            clusterUuidInPCBackupConfig, objectStoreEndpointInPCBackupConfig);

    pcTaskServiceHelper.updateTask(taskId, 20);

    // Check bucket read write access , bucket and object configuration for
    // provided object store endpoints.
    if(!objectStoreEndpointList.isEmpty())
      checkBucketAccessAndConfiguration(objectStoreEndpointList, objectStoreEndPointDtoMap);

    pcTaskServiceHelper.updateTask(taskId, 30);

    //fetch mantle secret id for credentials provided in object store endpoints
    Map<String,String> credentialsKeyIdMap = new HashMap<>();

    log.info("Check 1");

    credentialsKeyIdMap = writeObjectStoreCredentialsInMantle(objectStoreEndpointList);

    log.info("Check 2");
    // Copy Certs on all PCVM
    certFileUtil.createCertFileOnAllPCVM(new ArrayList<>(objectStoreEndPointDtoMap.values()), false);

    log.info("Check 3");
    // STEP -4 Update all the update entities arg for batch update entities.
    try {
      BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg =
              BatchUpdateEntitiesArg.newBuilder();
      log.info("Check 4");
      // Add updateEntityArg in updateEntityArgList.
      addUpdateEntitiesArgForPCBackupSpecs(batchUpdateEntitiesArg);
      log.info("Check 5");
      // Add update args ZK.
      addUpdateEntitiesArgForPCZkData(batchUpdateEntitiesArg);
      log.info("Check 6");
      // If this fails then there is no need to rollover the updates.
      if (!makeBatchUpdateRPC(batchUpdateEntitiesArg)) {
        log.info("Check 7");
        // This will not be thrown to the user.
        throw new PCResilienceException(
                ErrorMessages.getInsightsServerWriteErrorMessage("prism central backup specs and some other meta"),
                ErrorCode.PCBR_DATABASE_WRITE_BACKUP_DATA_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
      }

      pcTaskServiceHelper.updateTask(taskId, 40);

      // After successfully updating pc_zk_data table, remove stale zk nodes
      deleteStaleZkNodeValuesFromIDF(batchUpdateEntitiesArg);

      // Independently update pc_backup_metadata table, so that if this step fail
      // We don't have to roll over any change and proceed further.
      List<String> replicaPEsUuid = new ArrayList<>(peClusterUuidSet);
      replicaPEsUuid.addAll(clusterUuidInPCBackupConfig);
      List<PcObjectStoreEndpoint> objectStoreReplicas =
              new ArrayList<>(objectStoreEndpointList);
      objectStoreReplicas.addAll(objectStoreEndpointInPCBackupConfig);
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoAllMap =
              new HashMap<>(objectStoreEndPointDtoMap);
      // have certificate path in dto populate
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMapInPcBackupConfig =
              pcvmDataService.getObjectStoreEndpointDtoMapFromObjectStoreCredentialsBackupEntity(objectStoreCredentialsBackupEntityInPCBackupConfig);
      objectStoreEndPointDtoAllMap.putAll(objectStoreEndPointDtoMapInPcBackupConfig);
      BatchUpdateEntitiesArg.Builder  batchUpdateEntitiesArgForPcBackupMetadataProto =
              BatchUpdateEntitiesArg.newBuilder();
      // We need to necessarily add the hosting PE info when there is no
      // entry in pc_backup_config.
      boolean canSkipHostingPEInfo = !(clusterUuidInPCBackupConfig.isEmpty() &&
              objectStoreEndpointInPCBackupConfig.isEmpty());
      // Not updating sync time as part of the API, it will be updated as
      // part of scheduler.
      boolean updateSyncTimestamp = false;
      addUpdateEntitiesArgForPCBackupMetadata(
              batchUpdateEntitiesArgForPcBackupMetadataProto,
              replicaPEsUuid,
              objectStoreReplicas,
              objectStoreEndPointDtoAllMap,
              canSkipHostingPEInfo,
              updateSyncTimestamp);
      // If this fails then there is no need to rollover the updates.
      if (!makeBatchUpdateRPC(batchUpdateEntitiesArgForPcBackupMetadataProto)) {
        // This will not be thrown to the user.
        throw new PCResilienceException(ErrorMessages.getInsightsServerWriteErrorMessage("prism central backup"),
                ErrorCode.PCBR_DATABASE_WRITE_BACKUP_DATA_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
      }

      pcTaskServiceHelper.updateTask(taskId, 60);

      // STEP - 4
      updateBackupConfigWithRollback(peClusterUuidSet, objectStoreEndpointList, objectStoreEndPointDtoMap,
              credentialsKeyIdMap);
      if (!objectStoreEndpointList.isEmpty()) {
        // Handle the backup state for objectstore endpoints. Following
        // method will ensure these things asynchronously in that order -
        // 1. Publishing the seed data in IDF.
        // 2. Handle the pause backup status based on seed data availability.
        // 3. Write object metadata string to S3 if not already present.
        final String pcClusterUuid =
                instanceServiceFactory.getClusterUuidFromZeusConfig();
        objectStoreBackupService.initiateBackupToObjectStoreAsync(
                objectStoreEndPointDtoMap, pcClusterUuid,
                entityDBProxy, objectStoreEndpointList);
      }

      pcTaskServiceHelper.updateTask(taskId, 80);


      publishPulseData(peClusterUuidSet, objectStoreEndpointList);
    }
    catch (InsightsInterfaceException e) {
      log.error("Unable to process batchUpdate on the update args " +
              "provided in the request. Error Message: " + e.getMessage());
      log.debug("Error for batchUpdate while adding replicas: ", e);
      throw new PCResilienceException(ErrorMessages.getInsightsServerWriteErrorMessage("prism central backup"),
              ErrorCode.PCBR_DATABASE_WRITE_BACKUP_DATA_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public void createBackupTarget(ByteString taskId, BackupTargets backupTargets) throws ErgonException, PCResilienceException {
    log.info("Processing the Async request for Creating BackupTarget");
    Set<String> clusterUuidSet = new HashSet<>();
    List<PcObjectStoreEndpoint> pcObjectStoreEndpointList = new ArrayList<>();
    if (!ObjectUtils.isEmpty(backupTargets.getClusterUuidList())){
      clusterUuidSet = new HashSet<>(backupTargets.getClusterUuidList());
    }
    if (!ObjectUtils.isEmpty(backupTargets.getObjectStoreEndpointList())){
      pcObjectStoreEndpointList = backupTargets.getObjectStoreEndpointList();
    }
    ergonServiceHelper.updateTask(taskId, 0, ErgonTypes.Task.Status.kRunning);

    addReplicas(clusterUuidSet, pcObjectStoreEndpointList, taskId);

    ergonServiceHelper.updateTask(taskId, 100, ErgonTypes.Task.Status.kSucceeded);
  }


  @Override
  public void updateBackupTarget(ByteString taskId, BackupTargets backupTargets, String extId) throws ErgonException, PCResilienceException {

    log.info("Processing the Async request for Updating BackupTarget");
    ergonServiceHelper.updateTask(taskId, 0, ErgonTypes.Task.Status.kRunning);

    //Check if PC upgrade in progress
    if (pcUpgradeWatcher.isUpgradeInProgress()) {
      log.warn("Upgrade is in progress, not proceeding with updateRpo");
      throw new PCResilienceException(ErrorMessages.UPDATE_BACKUP_TARGET_RPO_UPGRADE_IN_PROGRESS_ERROR,
              ErrorCode.PCBR_UPGRADE_IN_PROGRESS, HttpStatus.BAD_REQUEST);
    }

    PcBackupConfig pcBackupConfig = PcBackupConfig.getDefaultInstance();
    String bucketName;
    // Fetch pc backup configs by entityId.
    // This method throws PC backup Exception if entity does not exists.
    try {
      pcBackupConfig = PCUtil.getPcBackupConfigById(extId, pcRetryHelper);
      ObjectStoreHelper helper = ObjectStoreHelperFactory.getObjectStoreHelper(pcBackupConfig.getObjectStoreEndpoint().getEndpointFlavour().name());
      bucketName = helper.getBucketNameByEndpoint(pcBackupConfig.getObjectStoreEndpoint().getEndpointAddress());
    } catch (PCResilienceException e) {
      throw e;
    }
    PcBackupConfig.ObjectStoreEndpoint objectStoreEndpoint = pcBackupConfig.getObjectStoreEndpoint();
    PcEndpointCredentials pcEndpointCredentials = backupTargets.
            getObjectStoreEndpointList().get(0).getEndpointCredentials();
    ObjectStoreEndPointDto objectStoreEndPointDto;
    String clusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    objectStoreEndPointDto = S3ObjectStoreUtil.getS3EndpointDTOFromObjectStoreEndpoint(objectStoreEndpoint, extId, clusterUuid, entityDBProxy);

    ergonServiceHelper.updateTask(taskId, 30, ErgonTypes.Task.Status.kRunning);

    String certPath = StringUtils.EMPTY;
    String certificate = StringUtils.EMPTY;
    ByteString encodedCert = null;

    if (!ObjectUtils.isEmpty(pcEndpointCredentials) && !ObjectUtils.isEmpty(pcEndpointCredentials.getCertificate())){
      if (!ObjectUtils.isEmpty(objectStoreEndPointDto.getCertificatePath())){
        certPath = CertificatesUtility.updateCertificatePath(objectStoreEndPointDto.getCertificatePath());
      } else {
        certPath = PCBR_OBJECTSTORE_CERTS_PATH +
                PCUtil.getObjectStoreEndpointUuid(instanceServiceFactory.getClusterUuidFromZeusConfig(),
                        objectStoreEndpoint.getEndpointAddress()) + PCBR_OBJECTSTORE_CERTS_EXTENSION;
      }
      certificate = CertificatesUtility.normalizeAndValidateCertificateContent(pcEndpointCredentials.getCertificate());
      objectStoreEndPointDto.setCertificatePath(certPath);
      objectStoreEndPointDto.setCertificateContent(certificate);
    }

    log.debug("Checking access for bucket {} with new access credentials",bucketName);
    //Check bucket access against new credentials or certs
    checkBucketAccess(bucketName,objectStoreEndpoint,objectStoreEndPointDto,pcEndpointCredentials);

    if (!ObjectUtils.isEmpty(pcEndpointCredentials) && !ObjectUtils.isEmpty(pcEndpointCredentials.getCertificate())) {
      certFileUtil.createCertFileOnAllPCVM(Arrays.asList(objectStoreEndPointDto), false);
      encodedCert = ByteString.copyFrom(Base64.getEncoder().encode(
              objectStoreEndPointDto.getCertificateContent().getBytes()));
    }

    PCVMBackupTargets pcvmBackupTargets = pcvmDataService.fetchPcvmBackupTargets();
    log.info("Fetched data from backup_metadata {} {}",bucketName,pcvmBackupTargets.getBackupTargetsList());

    String newKeyId = "";
    // Don't write secret for NC2 environment
    if (!ObjectUtils.isEmpty(pcEndpointCredentials)) {
      newKeyId = writeSecret(bucketName,pcEndpointCredentials);
      log.debug("New Credential KeyId updated");
    }

    pcTaskServiceHelper.updateTask(taskId, 60);

    RpoConfig rpoConfig = new RpoConfig();
    rpoConfig.setRpoSeconds(backupTargets.getObjectStoreEndpointList().get(0).getRpoSeconds());

    //Update rpo configuration in  pc_backup_config and pc_backup_metadata
    updateRpoAndCertsInPcBackupMetadata(bucketName, pcBackupConfig, rpoConfig, pcvmBackupTargets, false, 0, StringUtils.EMPTY, certPath, encodedCert);
    updateRpoAndCredentialsInPcBackupConfigWithRollback(extId, newKeyId, bucketName, pcBackupConfig, rpoConfig, pcvmBackupTargets, certPath, encodedCert);

    // If credentials are valid, erase old credentials from mantle
    if(pcBackupConfig.getObjectStoreEndpoint().hasCredentialsKeyId()) {
      String oldCredentialKeyId = pcBackupConfig.getObjectStoreEndpoint().getCredentialsKeyId();
      log.debug("Deleting old keyId {}",oldCredentialKeyId);
      deleteSecret(oldCredentialKeyId, bucketName);
    }

    ergonServiceHelper.updateTask(taskId, 100, ErgonTypes.Task.Status.kSucceeded);
  }

  /**
   * This method is responsible for checking the replica count do not exceeds
   * the allowed replica count.
   *
   * @param peClusterUuidSet - The set of PE cluster uuid.
   * @param objectStoreEndpointList - object store endpoint list
   * @param clusterUuidInPCBackupConfig - cluster uuids present in
   *                                    pc_backup_config
   * @param objectStoreEndpointInPCBackupConfig - object store endpoint
   *                                            present in pc_backup_config
   * @throws PCResilienceException - can throw PC backup exception.
   */
  private void checkAllowedReplicasCount(Set<String> peClusterUuidSet, List<PcObjectStoreEndpoint> objectStoreEndpointList, List<String> clusterUuidInPCBackupConfig, List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig) throws PCResilienceException {
    if (peClusterUuidSet.size() + clusterUuidInPCBackupConfig.size() >
            Constants.MAX_ALLOWED_REPLICA_PE) {

      // Raise an exception if the number of replicas requested to be added is
      // more than what's allowed.
      Integer allowedEntries = (Constants.MAX_ALLOWED_REPLICA_PE -
              clusterUuidInPCBackupConfig.size());
      String errorMessage = ErrorMessages.getAllowedPeReplicaCountErrorMessage(clusterUuidInPCBackupConfig.size(), allowedEntries);
      log.error(errorMessage);
      Map<String,String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_ALREADY_PRESENT_ENTITIES,
              String.valueOf(clusterUuidInPCBackupConfig.size()));
      errorArguments.put(ErrorCodeArgumentMapper.ARG_POSSIBLE_ENTITIES, String.valueOf(allowedEntries));
      throw new PCResilienceException(errorMessage, ErrorCode.PCBR_MAX_BACKUP_TARGET_PE_ENTITIES_REACHED,
              HttpStatus.BAD_REQUEST,errorArguments);
    }
    if (objectStoreEndpointList.size() +
            objectStoreEndpointInPCBackupConfig.size() > Constants.MAX_ALLOWED_REPLICA_OBJECT_STORE) {
      Integer allowedEntries = (Constants.MAX_ALLOWED_REPLICA_OBJECT_STORE -
              objectStoreEndpointInPCBackupConfig.size());
      String errorMessage = ErrorMessages.getAllowedObjectStoreReplicaCountErrorMessage(
              objectStoreEndpointInPCBackupConfig.size(), allowedEntries);
      log.error(errorMessage);
      Map<String,String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_ALREADY_PRESENT_ENTITIES,
              String.valueOf(objectStoreEndpointInPCBackupConfig.size()));
      errorArguments.put(ErrorCodeArgumentMapper.ARG_POSSIBLE_ENTITIES, String.valueOf(allowedEntries));
      throw new PCResilienceException(errorMessage, ErrorCode.PCBR_MAX_BACKUP_TARGET_OBJECT_ENTITIES_REACHED,
              HttpStatus.BAD_REQUEST,errorArguments);
    }
  }

  @Override
  public void generateEndpointAddress(PcObjectStoreEndpoint pcObjectStoreEndpoint,
                                      String pcClusterUuid) throws PCResilienceException {

    ObjectStoreHelper objectStoreHelper = ObjectStoreHelperFactory.getObjectStoreHelper(
            pcObjectStoreEndpoint.getEndpointFlavour()
                    .toString());
    ObjectStoreEndPointDto objectStoreEndPointDto =
            pcvmDataService.getBasicObjectStoreEndpointDtoFromPcObjectStore(pcObjectStoreEndpoint);
    String endPointAddress = objectStoreHelper.
            validateAndGenerateEndPointAddress(objectStoreEndPointDto);
    pcObjectStoreEndpoint.setEndpointAddress(endPointAddress);
    pcObjectStoreEndpoint.setEndpointAddress(
            S3ServiceUtil.getEndpointAddressWithBaseObjectKey(
                    pcObjectStoreEndpoint.getEndpointAddress()
                    ,pcClusterUuid));
  }

  /**
   * Check if the PE cluster uuids are eligible for backup or not
   * @param peClusterUuidSet - set of pe cluster uuid
   * @throws PCResilienceException - can throw pc backup exception in case pes
   * are not eligible.
   */
  private void checkIfPesEligibleForBackup(Set<String> peClusterUuidSet, boolean isSortingRequired) throws PCResilienceException {
    Set<String> eligibleClusterUuidSet = new HashSet<>();
    // Only fetch eligible cluster list when we receive pe cluster uuid.
    if (!peClusterUuidSet.isEmpty()) {
      eligibleClusterUuidSet = getEligibleClusterList(isSortingRequired)
              .getEligibleClusterList()
              .stream()
              .map(EligibleCluster::getClusterUuid)
              .collect(Collectors.toSet());
    }

    // looping over peClusterUuidSet, max possible values are 3.
    // In case only objectstore endpoints are provided, eligible cluster list
    // checks are going to be skipped as peClusterUuidSet is empty.
    for (String clusterUuid : peClusterUuidSet) {
      if (!eligibleClusterUuidSet.contains(clusterUuid)) {
        log.error(String.format("%s is not part of eligible Cluster PE for " +
                        "backup. Unable to proceed further.",
                clusterUuid));
        Map<String,String> errorArguments = new HashMap<>();
        errorArguments.put(ErrorCodeArgumentMapper.ARG_CLUSTER_EXT_ID, clusterUuid);
        throw new PCResilienceException(ErrorMessages.ADD_REPLICA_NON_ELIGIBLE_CLUSTER_ERROR,
                ErrorCode.PCBR_INELIGIBLE_BACKUP_TARGET, HttpStatus.BAD_REQUEST,errorArguments);
      }
    }
  }

  /**
   * Updates the pc_backup_config entity, and in case of failure rollbacks
   * the change.
   *
   * @param peClusterUuidSet - peClusterUuid set which is required to be
   *                         updated.
   * @param objectStoreEndpointList - list of object store endpoints which is
   *                               required to be updated.
   * @throws PCResilienceException - Can throw backup exception.
   */
  void updateBackupConfigWithRollback(Set<String> peClusterUuidSet, List<PcObjectStoreEndpoint> objectStoreEndpointList,
                                      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
                                      Map<String,String> credentialsKeyIdMap)
          throws PCResilienceException {
    // Adding the clusters which were not present in pc_backup_config.
    BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArgForBackupConfig =
            BatchUpdateEntitiesArg.newBuilder();
    addUpdateEntityArgsForPCBackupConfig(
            peClusterUuidSet, objectStoreEndpointList, objectStoreEndPointDtoMap, credentialsKeyIdMap,
            batchUpdateEntitiesArgForBackupConfig);
    // If this fails we will need to roll over the updates.
    if (!makeBatchUpdateRPC(batchUpdateEntitiesArgForBackupConfig)) {
      // If batch-update fails then start the roll over.
      for (String peClusterUuid : peClusterUuidSet) {
        // No exception should occur here because pc_backup_metadata is
        // already added, so remove replica will work.
        removeReplica(peClusterUuid);
      }
      for (PcObjectStoreEndpoint objectStoreEndpoint : objectStoreEndpointList) {
        // pc_backup_config uuid is create via the combination of pc_cluster_uuid
        // and endpoint_address.
        String pcUUID =
                instanceServiceFactory.getClusterUuidFromZeusConfig();
        String pcBackupConfigUUID = PCUtil.getObjectStoreEndpointUuid(
                pcUUID, objectStoreEndpoint.getEndpointAddress());
        removeReplica(pcBackupConfigUUID);
      }
      // Rollback the secret credentials stored in mantle service if
      // add replica fails.
      if(!credentialsKeyIdMap.isEmpty()){
        rollbackSecretsFromMantleService(credentialsKeyIdMap);
      }
      throw new PCResilienceException(ErrorMessages.getInsightsServerWriteErrorMessage("prism central backup"),
              ErrorCode.PCBR_DATABASE_WRITE_BACKUP_DATA_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);

    }
  }

  /**
   * This function makes the batch rpc call and returns whether
   * it was a success or not.
   *
   * @param batchUpdateEntitiesArg - batchUpdate arguments which are
   *                               required to be updated in IDF
   */
  public boolean makeBatchUpdateRPC(
          BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg) {
    try {
      List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
              batchUpdateEntitiesArg, entityDBProxy);
      if (!errorList.isEmpty()) {
        log.error(errorList.toString());
        return false;
      }
      return true;
    }
    catch (InsightsInterfaceException e) {
      log.error("Error encountered while making a" +
              " batch update call for backup.", e);
    }
    return false;
  }

  private void publishPulseData(
          Set<String> peClusterUuidSet,
          List<PcObjectStoreEndpoint> pcObjectStoreEndpoints) {
    try {
      pulsePublisherService.insertReplicaData(peClusterUuidSet,
              pcObjectStoreEndpoints);
    }
    catch (Exception e) {
      log.warn("Unable to send pulse data due to ", e);
    }
  }

  @Override
  public void removeReplica(String backupTargetId) throws PCResilienceException {
    removeReplica(backupTargetId, null);
  }

  /**
   * Function to remove the replica for the given
   * Subtasks -
   * 1. Remove the pc_backup_config entry for the given backup target ID.
   * 2. If deletion is successful then update the pc_backup_metadata table.
   * 3. If the pc_backup_config contains 0 entries then remove all entries
   * from pc_backup_specs and pc_zk_data.
   * <p>
   * Order of removal of entities -
   * 1. pc_backup_config - delete
   * 2. pc_backup_metadata - update
   * 3. (if there are no replicas) delete pc_zk_data + pc_backup_specs +
   * pc_backup_metadata
   *
   * @param backupTargetID - backupTargetID(can be object store entityId or pe
   *                      cluster uuid) given as input in API request.
   */
  @Override
  public void removeReplica(String backupTargetID, ByteString taskId)
          throws PCResilienceException {
    try{
      pcTaskServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kRunning, 0);
      String endpointAddressToRemoveFromS3 = "";
      // PC backup config is the source of truth for replica. If pc backup
      // metadata is not consistent with pc backup config, then backup
      // scheduler is responsible for making it consistent.

      if (pcUpgradeWatcher.isUpgradeInProgress()) {
        log.warn("Upgrade is in progress, not proceeding with removeReplicas");
        throw new PCResilienceException(ErrorMessages.REMOVE_REPLICA_UPGRADE_IN_PROGRESS_ERROR,
                ErrorCode.PCBR_UPGRADE_IN_PROGRESS,HttpStatus.SERVICE_UNAVAILABLE);
      }

      try{
        TaskListRet taskListRet = ergonServiceHelper
                .fetchTaskListRet(
                        Constants.TaskOperationType.kPCRestore.name(), false, PCDR_TASK_COMPONENT_TYPE);
        if (taskListRet.getTaskUuidListCount() > 0) {
          log.info("Restore task already in progress, not proceeding with removeReplicas");
          //Todo: verify if it is service unavailable or bad request
          throw new PCResilienceException(ErrorMessages.REMOVE_REPLICA_RESTORE_ALREADY_IN_PROGRESS_ERROR,
                  ErrorCode.PCBR_RESTORE_IN_PROGRESS,HttpStatus.SERVICE_UNAVAILABLE);
        }
      }
      catch (ErgonException e) {
        log.error("Unable to fetch task from Ergon",e);
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }

      PcBackupConfig.BackupType backupTargetType =
              getBackupType(backupTargetID);

      // 0. Check if current backupTargetID is in existing cluster
      // uuids or Object store endpoint in pc_backup_config table in IDF.
      // Copying the list to ensure that the lists are mutable and not
      // dependent on the methods.
      List<String> currentClusterUuidsInConfig =
              new ArrayList<>(
                      pcRetryHelper.fetchExistingClusterUuidInPCBackupConfig());

      // Get proper ObjectStoreEdnPointDto and PcObjectStoreEndpoint from pcBackupConfig
      ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntity =
              PCUtil.fetchExistingObjectStoreEndpointInPCBackupConfigWithCredentials(entityDBProxy,
                      instanceServiceFactory.getClusterUuidFromZeusConfig());

      List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig = objectStoreCredentialsBackupEntity.getObjectStoreEndpoints();

      if (backupTargetType == BackupType.kPE) {
        currentClusterUuidsInConfig.removeIf(uuid -> uuid.equals(backupTargetID));
      }
      else if (backupTargetType == BackupType.kOBJECTSTORE) {
        Iterator<PcObjectStoreEndpoint> itr =
                objectStoreEndpointInPCBackupConfig.iterator();
        String pcUUID =
                instanceServiceFactory.getClusterUuidFromZeusConfig();
        while (itr.hasNext()) {
          PcObjectStoreEndpoint objectStoreEndpoint = itr.next();
          String pcBackupConfigUUID = PCUtil.getObjectStoreEndpointUuid(
                  pcUUID, objectStoreEndpoint.getEndpointAddress());
          if (pcBackupConfigUUID.equals(backupTargetID)) {
            endpointAddressToRemoveFromS3 =
                    objectStoreEndpoint.getEndpointAddress();
            itr.remove();
          }
        }
      }

      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap = pcvmDataService.
              getObjectStoreEndpointDtoMapFromObjectStoreCredentialsBackupEntity(objectStoreCredentialsBackupEntity);

      // 1. Remove the pc_backup_config with given peClusterUuid
      removeBackupEntity(backupTargetID, currentClusterUuidsInConfig,
              objectStoreEndpointInPCBackupConfig, objectStoreEndPointDtoMap);


      if (currentClusterUuidsInConfig.isEmpty() &&
              objectStoreEndpointInPCBackupConfig.isEmpty()) {

        // Delete pc_backup_specs entries.
        deletePcBackupSpecsProtoEntities();

        // delete all pc_zk_data entry
        deletePcZkDataEntities();

        // delete pc_backup_metadata entry
        deletePcBackupMetadataProtoEntity();

      }
      pcTaskServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kRunning, 40);
      // TODO: Gaurav to add comment for this logic.
      if (backupTargetType == BackupType.kOBJECTSTORE &&
              objectStoreEndpointInPCBackupConfig.isEmpty() &&
              !StringUtils.isEmpty(endpointAddressToRemoveFromS3)) {
        objectStoreBackupService.deletePcSeedDataFromInsightsDb(
                instanceServiceFactory.getClusterUuidFromZeusConfig());
      }

      if (backupTargetType == BackupType.kPE) {
        updateIdfSyncStateZkNode(backupTargetID);
      }
      pulsePublisherService.removeReplicaData(backupTargetID);
      log.info(String.format("Replica with Uuid: %s has been removed",
              backupTargetID));
      pcTaskServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kSucceeded, 100);

      // 3. if peClusterUuid/ objectstore endpoint was the last entry in
      // pc_backup_config, delete all entries from pc_backup_specs and
      // pc_zk_data table too
    } catch (PCResilienceException e) {
      log.error("Error while removing replica ", e);
      throw e;
    } catch (ErgonException e) {
      log.error("Ergon exception occurred while removing replica ", e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * This method is responsible for removing the backup entity from pc backup
   * config and pc backup metadata.
   *
   * @param backupTargetID - current backup target id to be removed
   * @param currentClusterUuidsInConfig - cluster uuids present in
   *                                    pc_backup_config
   * @param objectStoreEndpointInPCBackupConfig - object store endpoint
   *                                            present in pc_backup_config
   * @throws PCResilienceException - can throw pc backup exception.
   */
  private void removeBackupEntity(String backupTargetID, List<String> currentClusterUuidsInConfig, List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig,
                                  Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap) throws PCResilienceException {
    EntityGuid deleteEntityUuid = EntityGuid.newBuilder()
            .setEntityId(backupTargetID)
            .setEntityTypeName(
                    Constants.PC_BACKUP_CONFIG)
            .build();
    DeleteEntityArg deleteEntityArg =
            DeleteEntityArg.newBuilder()
                    .setEntityGuid(deleteEntityUuid)
                    .build();
    try {
      entityDBProxy.deleteEntity(deleteEntityArg);
      // Getting existing cluster uuids from pc_backup_config table in IDF.
      BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg =
              BatchUpdateEntitiesArg.newBuilder();
      addUpdateEntitiesArgForPCBackupMetadata(batchUpdateEntitiesArg,
              currentClusterUuidsInConfig,
              objectStoreEndpointInPCBackupConfig,
              objectStoreEndPointDtoMap,
              true,
              false);

      List<String> errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
              batchUpdateEntitiesArg, entityDBProxy);
      if (!errorList.isEmpty()) {
        log.error(errorList.toString());
        //todo: verify below message should it be add replica or remove
        Map<String,String> errorArguments = new HashMap<>();
        errorArguments.put(ErrorCodeArgumentMapper.ARG_BACKUP_TARGET_EXT_ID,backupTargetID);
        throw new PCResilienceException(ErrorMessages.getInsightsServerRemoveErrorMessage("backup target"),
                ErrorCode.PCBR_DATABASE_DELETE_BACKUP_TARGET_ENTITY_INFO_ERROR, HttpStatus.INTERNAL_SERVER_ERROR,errorArguments);
      }
    }
    catch (InsightsInterfaceException e) {
      String errorMessage = String.format("Unable to process deleteEntity %s",
              e);
      log.error(errorMessage);
      throw new PCResilienceException(ErrorMessages.getInsightsServerRemoveErrorMessage("prism central backup target"),
              ErrorCode.PCBR_DATABASE_DELETE_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * This method is responsible for updating the backtype for the given
   * backup target ID and throwing an exception if we are not able to
   * determine the backuptype
   *
   * @param backupTargetID - target id of the backup replica
   * @return - returns the backup type (kPE or kOBJECTSTORE)
   * @throws PCResilienceException - can throw exception.
   */
  private BackupType getBackupType(String backupTargetID) throws PCResilienceException {
    // In case of pre-existing entries in pc_backup_config, BackupType will
    // not be set in IDF. Set the backupType as kPE by default.
    BackupType backupTargetType = BackupType.kPE;
    PcBackupConfig pcBackupConfig = PcBackupConfig.getDefaultInstance();
    pcBackupConfig = getBackupTargetById(backupTargetID);
    if (pcBackupConfig.hasBackupType()) {
      backupTargetType = pcBackupConfig.getBackupType();
    }
    return backupTargetType;
  }

  /**
   * Delete all pc_backup_specs entities.
   *
   * @throws PCResilienceException - can throw backup exception.
   */
  private void deletePcBackupSpecsProtoEntities() throws PCResilienceException {
    GetEntitiesWithMetricsRet pcBackupSpecs =
            fetchPCBackupSpecsFromIDF();
    List<String> pcBackupSpecsUuids =
            getEntityIdsFromGetEntitiesWithMetrics(pcBackupSpecs);
    if (!pcBackupSpecsUuids.isEmpty()) {
      boolean isDeleted = IDFUtil.batchDeleteIds(pcBackupSpecsUuids,
              Constants.PC_BACKUP_SPECS_TABLE,
              entityDBProxy);
      if (!isDeleted) {
        String errorMessage = String.format("Unable to delete %s table",
                Constants.PC_BACKUP_SPECS_TABLE);
        log.error(errorMessage);
        throw new PCResilienceException(ErrorMessages.getInsightsServerRemoveErrorMessage("prism central backup target"),
                ErrorCode.PCBR_DATABASE_DELETE_BACKUP_SPECS_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);

      }
    }
  }

  public void deleteStaleZkNodeValuesFromIDF(
          BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg)
          throws PCResilienceException {
    Set<String> zkEntityIds =
            batchUpdateEntitiesArg.getEntityListList().stream()
                    .filter(updateEntityArg ->
                            updateEntityArg.getEntityGuid()
                                    .getEntityTypeName()
                                    .equals(PC_ZK_DATA)
                    )
                    .map(updateEntityArg ->
                            updateEntityArg.getEntityGuid()
                                    .getEntityId())
                    .collect(Collectors.toSet());
    Set<String> zkEntityIdsToDelete = new HashSet<>(fetchZkEntityIdsInIDF());
    zkEntityIdsToDelete.removeAll(zkEntityIds);
    log.debug("Entities required to be deleted in pc_zk_data table - {}",
            zkEntityIdsToDelete);
    if (!zkEntityIdsToDelete.isEmpty()) {
      boolean isDeleted = IDFUtil.batchDeleteIds(
              new ArrayList<>(zkEntityIdsToDelete),
              PC_ZK_DATA,
              entityDBProxy);
      if (!isDeleted) {
        log.error(String.format("Unable to delete stale entities in %s table," +
                        " entities are %s", PC_ZK_DATA,
                zkEntityIdsToDelete
        ));
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
                ErrorCode.PCBR_DATABASE_DELETE_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);

      }
    }
  }

  /**
   * Fetch existing zk entity IDs in IDF
   *
   * @return - returns the list of entity ids
   * @throws PCResilienceException - backup exception
   */
  List<String> fetchZkEntityIdsInIDF() throws PCResilienceException {
    GetEntitiesWithMetricsRet pcZkData = fetchPCZkDataFromIDF();
    return getEntityIdsFromGetEntitiesWithMetrics(pcZkData);
  }

  /**
   * Deletes all the entries present in pc_zk_data table
   *
   * @throws PCResilienceException - can throw PCResilienceException
   */
  void deletePcZkDataEntities() throws PCResilienceException {
    List<String> pcZkDataUuids = fetchZkEntityIdsInIDF();
    if (!pcZkDataUuids.isEmpty()) {
      boolean isDeleted = IDFUtil.batchDeleteIds(pcZkDataUuids,
              PC_ZK_DATA,
              entityDBProxy);
      if (!isDeleted) {
        log.error(String.format("Unable to delete %s table",
                PC_ZK_DATA));
        throw new PCResilienceException(ErrorMessages.getInsightsServerRemoveErrorMessage("prism central backup target meta"),
                ErrorCode.PCBR_DATABASE_DELETE_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);

      }
    }
  }

  /**
   * Function to delete pc_backup_metadata entity present on PC.
   *
   * @throws PCResilienceException - throws PCResilienceException.
   */
  public void deletePcBackupMetadataProtoEntity() throws PCResilienceException {
    EntityGuid pcBackupMetadataGuid =
            EntityGuid.newBuilder()
                    .setEntityId(instanceServiceFactory
                            .getClusterUuidFromZeusConfig())
                    .setEntityTypeName(Constants.PC_BACKUP_METADATA)
                    .build();
    DeleteEntityArg pcBackupMetadataDeleteEntityArg =
            DeleteEntityArg.newBuilder()
                    .setEntityGuid(pcBackupMetadataGuid)
                    .build();
    try {
      entityDBProxy.deleteEntity(pcBackupMetadataDeleteEntityArg);
    }
    catch (InsightsInterfaceException e) {
      log.error("Unable to delete {} table", Constants.PC_BACKUP_METADATA);
      throw new PCResilienceException(ErrorMessages.getInsightsServerRemoveErrorMessage("prism central backup target meta"),
              ErrorCode.PCBR_DATABASE_DELETE_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Create UpdateEntityArgs for backup specs to be backed up and add it in
   * updateEntityArgList.
   *
   * @param batchUpdateEntitiesArg - list of updateEntityArg in which
   *                               pc_backup_specs updateEntityArg will be added.
   * @throws InsightsInterfaceException - can throw
   *                                    InsightsInterfaceException if db update fails.
   */
  public void addUpdateEntitiesArgForPCBackupSpecs(
          final BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg)
          throws InsightsInterfaceException, PCResilienceException {
    Map<String, Object> attributesValueMap = new HashMap<>();
    PcBackupSpecsProto.ServiceVersionSpecs serviceVersionSpecs =
            null;

    // This is for v1
    String keyId = fetchOrCreateKeyId();
    // Set keyId and encryption version
    attributesValueMap.put(Constants.PCVM_ENCRYPTION_VERSION,
            Constants.ENCRYPTION_VERSION_V1);
    attributesValueMap.put(Constants.PCVM_KEY_ID, keyId);
    log.info("Check 9");

    PcBackupSpecsProto.PCVMSpecs pcvmSpecs =
            pcvmDataService.constructPCVMSpecs(keyId);
    log.info("Check 9.5");
    attributesValueMap.put(
            Constants.PCVM_SPECS_ATTRIBUTE,
            CompressionUtils.compress(
                    pcvmSpecs.toByteString()
            )
    );
    log.info("Check 10");
    if (!pcdrYamlAdapter.getPortfolioServicesToReconcileAfterRestore().isEmpty()) {
      serviceVersionSpecs = getServiceVersionBackupSpecs();

      if(!ObjectUtils.isEmpty(serviceVersionSpecs)) {
        attributesValueMap.put(
                SERVICE_VERSION_SPECS,
                CompressionUtils.compress(
                        serviceVersionSpecs.toByteString()
                )
        );
      }
    }
    log.info("Check 11");

    try {
      attributesValueMap.put(
              Constants.PCVM_CMSP_SPECS,
              CompressionUtils.compress(
                      pcvmDataService.fetchCMSPSpecs().toByteString()));
    }
    catch (PCResilienceException e) {
      log.warn("Unable to create CMSP spec for backup due to some issue, " +
              "not storing it as part of backup.", e);
    }
    attributesValueMap.put(
            Constants.PCVM_FILES_ATTRIBUTE,
            CompressionUtils.compress(
                    pcvmDataService.constructPCVMGeneralFiles(
                            keyId, Constants.ENCRYPTION_VERSION_V1).toByteString()
            )
    );
    log.info("Check 12");
    // Entity ID is the PC Cluster Uuid.
    UpdateEntityArg.Builder updateEntityArgBuilder =
            IDFUtil.constructUpdateEntityArgBuilder(
                    Constants.PC_BACKUP_SPECS_TABLE,
                    instanceServiceFactory
                            .getClusterUuidFromZeusConfig(),
                    attributesValueMap);
    log.info("Check 13");
    updateEntityArgBuilder.setFullUpdate(false);
    batchUpdateEntitiesArg.addEntityList(updateEntityArgBuilder.build());
  }

  /**
   * Create UpdateEntityArgs for zk_data to be backed up and add it in
   * updateEntityArgList.
   *
   * @param batchUpdateEntitiesArg - list of updateEntityArg in which zk_data
   *                               updateEntityArg will be added.
   */
  public void addUpdateEntitiesArgForPCZkData(
          final BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg) {
    pcvmZkService.addUpdateEntitiesArgForPCZkData(batchUpdateEntitiesArg);
  }

  /**
   * Create UpdateEntityArgs for backup metadata to be backed up and add it in
   * updateEntityArgList.
   *
   * @param batchUpdateEntitiesArg - list of updateEntityArg in which zk_data
   *                               updateEntityArg will be added.
   */
  public void addUpdateEntitiesArgForPCBackupMetadata(
          final BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg,
          final List<String> clusterUuidList,
          List<PcObjectStoreEndpoint> objectStoreEndpointList,
          Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
          final boolean canSkipHostingPEInfo,
          final boolean updateSyncTime)
          throws PCResilienceException {
    Map<String, Object> attributesValueMap = new HashMap<>();

    // When add-replica is called for the first time, PCVMHostingPEInfo shall
    // not be null, if null a Runtime exception will be raised.
    try {
      attributesValueMap.put(
              Constants.PCVM_HOSTING_PE_INFO,
              CompressionUtils.compress(
                      pcvmDataService.constructPCVMHostingPEInfo().toByteString()
              )
      );
    }
    catch (PCResilienceException e) {
      if (canSkipHostingPEInfo) {
        log.warn("PC is not connected to the hosting PE. Unable to update " +
                "backup data.", e);
      }
      else {
        throw e;
      }
    }

    // This should always be present.
    attributesValueMap.put(
            Constants.PCVM_BACKUP_TARGETS,
            CompressionUtils.compress(
                    pcvmDataService.constructPCVMBackupTargets(clusterUuidList,
                                    objectStoreEndpointList,
                                    objectStoreEndPointDtoMap,
                                    updateSyncTime)
                            .toByteString()
            )
    );

    // Added for both backup on Cluster and ObjectStore, but used only for Cluster.
    // For ObjectStore backup this info is stored in ObjectStore.
    attributesValueMap.put(
            Constants.DOMAIN_MANAGER_IDENTIFIER,
            CompressionUtils.compress(
                    pcvmDataService.constructDomainManagerIdentifier().toByteString()
            )
    );

    // Entity ID is the PC Cluster Uuid.
    UpdateEntityArg.Builder updateEntityArgBuilder =
            IDFUtil.constructUpdateEntityArgBuilder(
                    Constants.PC_BACKUP_METADATA,
                    instanceServiceFactory
                            .getClusterUuidFromZeusConfig(),
                    attributesValueMap);
    updateEntityArgBuilder.setFullUpdate(false);
    batchUpdateEntitiesArg.addEntityList(updateEntityArgBuilder.build());
  }

  /**
   * Create UpdateEntityArgs for pc_backup_config and add it in
   * updateEntityArgList.
   *
   * @param peClusterUuidSet - the clusterUuids for which pc_backup_config
   *                         has to be updated.
   */
  public void addUpdateEntityArgsForPCBackupConfig(
          final Set<String> peClusterUuidSet,
          List<PcObjectStoreEndpoint> objectStoreEndpointList,
          Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
          Map<String,String> credentialKeyIdMap,
          final BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg) {
    List<UpdateEntityArg> updateEntityArgList = new ArrayList<>();
    for (String peClusterUuid : peClusterUuidSet) {
      Map<String, Object> attributesValueMap = new HashMap<>();

      attributesValueMap.put(
              Constants.ZPROTOBUF,
              CompressionUtils.compress(
                      PcBackupConfig.newBuilder()
                              .setClusterUuid(peClusterUuid)
                              .setBackupType(BackupType.kPE)
                              .build()
                              .toByteString()
              )
      );
      UpdateEntityArg.Builder updateEntityArgBuilder =
              IDFUtil.constructUpdateEntityArgBuilder(
                      Constants.PC_BACKUP_CONFIG,
                      peClusterUuid,
                      // Entity Id being set as PE cluster uuid.
                      attributesValueMap);
      updateEntityArgList.add(updateEntityArgBuilder.build());
    }
    for (PcObjectStoreEndpoint objectStoreEndpoint : objectStoreEndpointList) {
      ObjectStoreEndPointDto objectStoreEndPointDto = objectStoreEndPointDtoMap.get(objectStoreEndpoint.getEndpointAddress());
      Map<String, Object> attributesValueMap = new HashMap<>();
      ObjectStoreEndpoint.Builder objectStoreEndpointProtoBuilder =
              ObjectStoreEndpoint.newBuilder()
                      .setEndpointAddress(
                              objectStoreEndpoint.getEndpointAddress())
                      .setEndpointFlavour(EndpointFlavour.forNumber(
                              objectStoreEndpoint.getEndpointFlavour()
                                      .ordinal()))
                      .setBackupRetentionDays(objectStoreEndpoint.getBackupRetentionDays())
                      .setBucketName(objectStoreEndpoint.getBucket())
                      .setRegion(objectStoreEndpoint.getRegion());

      if (objectStoreEndpoint.getEndpointFlavour().toString().equals(PcEndpointFlavour.KOBJECTS.toString())) {
        objectStoreEndpointProtoBuilder
                .setPathStyleEnabled(objectStoreEndPointDto.isPathStyle())
                .setSkipCertificateValidation(objectStoreEndPointDto.isSkipCertificateValidation());
        if (!objectStoreEndPointDto.isSkipCertificateValidation() && !ObjectUtils.isEmpty(objectStoreEndPointDto.getCertificatePath())) {
          objectStoreEndpointProtoBuilder.setCertificatePath(objectStoreEndPointDto.getCertificatePath());
        }
      }

      if(StringUtils.isNotEmpty(credentialKeyIdMap.get(objectStoreEndpoint.getEndpointAddress()))){
        objectStoreEndpointProtoBuilder.setCredentialsKeyId(credentialKeyIdMap.get(objectStoreEndpoint.getEndpointAddress()));
      }

      ObjectStoreEndpoint objectStoreEndpointProto = objectStoreEndpointProtoBuilder.build();

      attributesValueMap.put(
              Constants.ZPROTOBUF,
              CompressionUtils.compress(
                      PcBackupConfig.newBuilder()
                              .setObjectStoreEndpoint(objectStoreEndpointProto)
                              .setBackupType(
                                      BackupType.kOBJECTSTORE)
                              .setRpoSecs(objectStoreEndpoint.getRpoSeconds())
                              .setPauseBackup(true)
                              .setPauseBackupMessage(Messages.PauseBackupMessagesEnum.GATHERING_DATA.name())
                              .build()
                              .toByteString()
              )
      );
      // pc_backup_config uuid is create via the combination of pc_cluster_uuid
      // and endpoint_address.
      String pcUUID =
              instanceServiceFactory.getClusterUuidFromZeusConfig();
      UpdateEntityArg.Builder updateEntityArgBuilder =
              IDFUtil.constructUpdateEntityArgBuilder(
                      Constants.PC_BACKUP_CONFIG,
                      PCUtil.getObjectStoreEndpointUuid(
                              pcUUID, objectStoreEndpoint.getEndpointAddress()),
                      // Entity Id being set as PE cluster uuid.
                      attributesValueMap);
      updateEntityArgList.add(updateEntityArgBuilder.build());
    }
    batchUpdateEntitiesArg.addAllEntityList(updateEntityArgList);
  }

  /**
   * @return - Returns the list of PE's which are added for replica using
   * add-replica API. It returns the uuid, name and lastSyncTime of backup
   * for these PEs
   */
  public BackupTargetsInfo getReplicas() throws PCResilienceException {
    PcBackupMetadataProto.PCVMBackupTargets pcvmBackupTargets;
    try {
      pcvmBackupTargets = PcdrProtoUtil.fetchBackupTargetFromPcBackupMetadata(
              instanceServiceFactory.getClusterUuidFromZeusConfig(), entityDBProxy);
      log.debug("Fetched replica UUID's from pc_backup_metadata. {}",
              pcvmBackupTargets);
    } catch (Exception e) {
      log.error("Unable to fetch pc_backup_metadata from IDF due to exception : ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e,
              ErrorMessages.getInsightsServerReadErrorMessage("prism central backup metadata"));
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }
    List<PcBackupConfig> pcBackupConfigs =
            backupStatus.fetchPcBackupConfigProtoList();

    BackupTargetsInfo backupTargetsInfo = new BackupTargetsInfo();
    if (pcBackupConfigs.isEmpty()) {
      log.warn("No entity found in pc_backup_config table. " +
              "pc_backup_metadata should have been empty.");
    }
    List<ObjectStoreEndpointInfo> objectStoreEndpointInfoList =
            Collections.emptyList();
    List<PEInfo> peInfoList = Collections.emptyList();
    if (pcvmBackupTargets != null && !pcBackupConfigs.isEmpty()) {
      objectStoreEndpointInfoList =
              getObjectStoreReplicas(pcBackupConfigs,
                      pcvmBackupTargets.getObjectStoreBackupTargetsList());
      peInfoList = getPEInfoReplicas(
              pcBackupConfigs,
              pcvmBackupTargets.getBackupTargetsList());
    }
    backupTargetsInfo.setPeInfoList(peInfoList);
    backupTargetsInfo.setObjectStoreEndpointInfoList(
            objectStoreEndpointInfoList);
    return backupTargetsInfo;
  }

  /**
   * This method is responsible for retrieving cluster uuids present in both
   * pc_backup_metadata and pc_backup_config.
   *
   * @param pcBackupConfigs - list of pc backup config present in IDF
   * @param backupTargets - list of PE backup targets present in
   *                      pc_backup_metadata
   * @return - returns a list of pe infos
   * @throws PCResilienceException - can throw pc backup exception.
   */
  List<PEInfo> getPEInfoReplicas(List<PcBackupConfig> pcBackupConfigs,
                                 List<PCVMBackupTargets.BackupTarget> backupTargets)
          throws PCResilienceException {
    List<PEInfo> peInfoList = new ArrayList<>();
    HashMap<String, PcBackupConfig> clusterUuidInPcBackupConfig =
            new HashMap<>();
    pcBackupConfigs.forEach(pcBackupConfig -> {
      if (pcBackupConfig.hasClusterUuid() &&
              !pcBackupConfig.getClusterUuid().isEmpty()) {
        clusterUuidInPcBackupConfig.put(
                pcBackupConfig.getClusterUuid(),
                pcBackupConfig);
      }
    });
    for (PCVMBackupTargets.BackupTarget backupTarget : backupTargets) {
      if (clusterUuidInPcBackupConfig.containsKey(backupTarget.getClusterUuid())) {
        PcBackupConfig currentPcBackupConfig =
                clusterUuidInPcBackupConfig.get(backupTarget.getClusterUuid());
        PEInfo peInfo = new PEInfo();
        peInfo.setPeClusterId(backupTarget.getClusterUuid());
        peInfo.setPeName(backupTarget.getName());
        peInfo.setIsBackupPaused(currentPcBackupConfig.getPauseBackup());
        peInfo.setLastSyncTimestamp(backupTarget.getLastSyncTimestamp());
        if (currentPcBackupConfig.getPauseBackup()) {
          peInfo.setPauseBackupMessage(
                  getPauseBackupMessage(currentPcBackupConfig));
        }
        peInfoList.add(peInfo);
      }
      else {
        // It will be updated whenever the scheduler will run so that there
        // is consistency.
        log.warn("The {} PE cluster uuid is not part of pc_backup_config.",
                backupTarget.getClusterUuid());
      }
    }
    return peInfoList;
  }

  /**
   * This method is responsible for checking whether pc_backup_config and
   * pc_backup_metadata both contains the backup target and return it.
   *
   * @param pcBackupConfigs - List of pc backup configs present in IDF
   * @param objectStoreBackupTargets - list of object store backup targets
   *                                 present in pc_backup_metadata
   * @return - returns list of object store endpoint info
   * @throws PCResilienceException - can raise pc backup exception.
   */
  List<ObjectStoreEndpointInfo> getObjectStoreReplicas(
          List<PcBackupConfig> pcBackupConfigs,
          List<ObjectStoreBackupTarget> objectStoreBackupTargets)
          throws PCResilienceException {
    List<ObjectStoreEndpointInfo> objectStoreEndpointInfoList =
            new ArrayList<>();
    // Create a hashmap containing object store address and pc backup config.
    // Use the same to check if
    HashMap<String, PcBackupConfig> objectStoreAddressInPcBackupConfig =
            new HashMap<>();
    pcBackupConfigs.forEach(pcBackupConfig -> {
      if (pcBackupConfig.hasObjectStoreEndpoint()) {
        objectStoreAddressInPcBackupConfig.put(
                pcBackupConfig.getObjectStoreEndpoint().getEndpointAddress(),
                pcBackupConfig);
      }
    });
    if (objectStoreBackupTargets != null) {
      String pcUUID =
              instanceServiceFactory.getClusterUuidFromZeusConfig();
      for (ObjectStoreBackupTarget objectStoreBackupTarget : objectStoreBackupTargets) {
        if (!objectStoreAddressInPcBackupConfig.containsKey(
                objectStoreBackupTarget.getEndpointAddress())) {
          // It will be updated whenever the scheduler will run so that there
          // is consistency.
          log.warn("The {} endpoint address is not part of pc_backup_config.",
                  objectStoreBackupTarget.getEndpointAddress());
          continue;
        }
        PcBackupConfig currentPcBackupConfig =
                objectStoreAddressInPcBackupConfig.get(
                        objectStoreBackupTarget.getEndpointAddress());
        String pcBackupConfigUUID = PCUtil.getObjectStoreEndpointUuid(
                pcUUID, objectStoreBackupTarget.getEndpointAddress());
        ObjectStoreEndpointInfo objectStoreEndpointInfo =
                new ObjectStoreEndpointInfo();
        PcObjectStoreEndpoint pcObjectStoreEndpoint =
                new PcObjectStoreEndpoint();
        pcObjectStoreEndpoint.setBucket(objectStoreBackupTarget.getBucketName());
        pcObjectStoreEndpoint.setRegion(objectStoreBackupTarget.getRegion());
        if (objectStoreBackupTarget.getEndpointFlavour().toString().equalsIgnoreCase(PcEndpointFlavour.KOBJECTS.toString())) {
          pcObjectStoreEndpoint.setIpAddressOrDomain(S3ObjectStoreUtil.getHostnameOrIPFromEndpointAddress(
                  objectStoreBackupTarget.getEndpointAddress(), IS_PATH_STYLE_ENDPOINT_ADDRESS_STORED_IN_IDF));
          pcObjectStoreEndpoint.setHasCustomCertificate(!ObjectUtils.isEmpty(objectStoreBackupTarget.getCertificatePath()));
          pcObjectStoreEndpoint.setSkipCertificateValidation(objectStoreBackupTarget.getSkipCertificateValidation());
        }
        pcObjectStoreEndpoint.setEndpointFlavour(
                PcEndpointFlavour.fromString(
                        objectStoreBackupTarget.getEndpointFlavour().toString()));
        pcObjectStoreEndpoint.setRpoSeconds(currentPcBackupConfig.getRpoSecs());
        objectStoreEndpointInfo.setObjectStoreEndpoint(pcObjectStoreEndpoint);
        objectStoreEndpointInfo.setIsBackupPaused(
                currentPcBackupConfig.getPauseBackup());
        objectStoreEndpointInfo.setLastSyncTimestamp(
                objectStoreBackupTarget.getLastSyncTimestamp());
        objectStoreEndpointInfo.setEntityId(pcBackupConfigUUID);
        if (currentPcBackupConfig.getPauseBackup()) {
          objectStoreEndpointInfo.setPauseBackupMessage(
                  getPauseBackupMessage(currentPcBackupConfig));
        }
        objectStoreEndpointInfoList.add(objectStoreEndpointInfo);
      }
    }
    return objectStoreEndpointInfoList;
  }

  /**
   * Retrieves the pause backup message from the pause backup message enum key
   * present in pc_backup_config.
   * @param pcBackupConfig - pcbackupConfig object
   * @return - returns string of pause backup message
   * @throws PCResilienceException - can raise PC backup exception.
   */
  private String getPauseBackupMessage(PcBackupConfig pcBackupConfig)
          throws PCResilienceException {
    String pauseBackupDetailedMessage = "";
    if (!StringUtils.isEmpty(
            pcBackupConfig.getPauseBackupMessage())) {
      try {
        pauseBackupDetailedMessage =
                Messages.PauseBackupMessagesEnum.valueOf(
                                pcBackupConfig.getPauseBackupMessage())
                        .getPauseBackupDetailedMessage();
      }
      catch (IllegalArgumentException e) {
        log.error("Pause backup message stored in IDF is not part of" +
                " PauseBackupMessagesEnum.", e);
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
    }
    return pauseBackupDetailedMessage;
  }

  /**
   * If keyId is already present then use that from pc_backup_specs, else
   * create a new key_id.
   *
   * @return - returns key_id
   * @throws PCResilienceException - can throw PCResilienceException
   */
  String fetchOrCreateKeyId() throws PCResilienceException {
    String clusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    // Create a list containing rawColumns to query from IDF.
    List<String> rawColumns = new ArrayList<>();
    rawColumns.add(Constants.PCVM_KEY_ID);
    Query query = QueryUtil
            .constructPcBackupSpecsProtoQuery(clusterUuid, rawColumns);
    GetEntitiesWithMetricsRet result;
    try {
      result = entityDBProxy.getEntitiesWithMetrics(
              GetEntitiesWithMetricsArg.newBuilder().setQuery(query).build());
    }
    catch (InsightsInterfaceException e) {
      String error = "Querying pc_backup_specs from IDF failed.";
      log.error(error);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_DATABASE_READ_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    if (result != null && !result.getGroupResultsListList().isEmpty() &&
            !result.getGroupResultsList(0).getRawResultsList().isEmpty()) {
      DataValue dataValueKeyId = IDFUtil.getAttributeValueInEntityMetric(
              Constants.PCVM_KEY_ID,
              result.getGroupResultsList(0).getRawResults(0));
      assert dataValueKeyId != null;
      return dataValueKeyId.getStrValue();
    }

    return PCUtil.generateSecureRandomString(10);

  }

  /**
   * This method is responsible for making Mercury Fanout call to invoke
   * Insights RPC DeleteClusterReplicationState on the replica PE to reset
   * IDF sync state before we start the next backup cycle
   *
   */
  public void resetBackupSyncStates()
          throws PCResilienceException {

    String serializedReplicaPEUuids = null;

    // this list contains the replica PE Uuids for which reset IDF sync
    // was failure due to any reason
    List<String> pendingReplicaPEUuids = new ArrayList<>();
    try {
      // first check if the ZK node for reset backup sync state exists, if does not exist then create
      Stat stat = zookeeperServiceHelper.exists(IDF_BACKUP_SYNC_STATES_ZK_PATH, false);
      if (stat == null) {
        log.info("IDF_BACKUP_SYNC_STATES zkNode does not exists so just " +
                "return");
        return;
      }

      // Read the list of PE Uuids for which reset IDF sync needs to be triggered
      byte[] data = zookeeperServiceHelper.getData(IDF_BACKUP_SYNC_STATES_ZK_PATH, stat);
      serializedReplicaPEUuids = new String(data);

      if (serializedReplicaPEUuids.equals(Constants.NO_DATA_ZK_VALUE)) {
        log.warn("Since there are no PE nodes for which reset IDF sync " +
                "state needs to be triggered, so skipping the reset IDF sync states flow");
        return;
      }
      List<String> replicaPEUuids = Arrays.asList(serializedReplicaPEUuids.split(","));

      log.info(
              "List of PE Uuids for which reset IDF sync state would be triggered: {}",
              replicaPEUuids);

      // Build CompletableFuture objects for each PE Uuid for which reset IDF Sync needs to be triggered.
      List<CompletableFuture<Boolean>> resetBackupSyncStateFutureList
              = replicaPEUuids.stream().map(this::resetBackupSyncStateForPE)
              .collect(
                      Collectors.toList());

      CompletableFuture.allOf(resetBackupSyncStateFutureList.toArray(
              new CompletableFuture[resetBackupSyncStateFutureList.size()]));

      for (int i = 0; i < resetBackupSyncStateFutureList.size(); i++) {
        CompletableFuture<Boolean> future = resetBackupSyncStateFutureList.get(
                i);

        if (!future.get()) {
          log.error(
                  "Reset IDF Sync State was not successful for PE with Uuid: {}",
                  replicaPEUuids.get(i));
          pendingReplicaPEUuids.add(replicaPEUuids.get(i));
          continue;
        }
        log.info("Reset IDF Sync State was successful for PE with Uuid: {}",
                replicaPEUuids.get(i));
      }

      log.info(
              "List of PE Uuids for which reset IDF sync state was not successful " +
                      "this time and would be triggered again in next scheduler cycle: {}",
              pendingReplicaPEUuids);

      // Serialize the list of PE Uuids as data needs to be stored on zkNode in Byte format
      serializedReplicaPEUuids = pendingReplicaPEUuids.isEmpty() ?
              Constants.NO_DATA_ZK_VALUE :
              String.join(",", pendingReplicaPEUuids);

      log.info("Writing the PE Uuid list: {} for which reset IDF Sync state " +
              "was not successful" +
              "back to zkNode", serializedReplicaPEUuids);
      zookeeperServiceHelper.setData(IDF_BACKUP_SYNC_STATES_ZK_PATH,
              serializedReplicaPEUuids.getBytes(), stat.getVersion());
    } catch (Exception e) {
      log.error("Error encountered while invoking " +
              "DeleteClusterReplicationState RPC call on replica PE's ", e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * This method is responsible for making Mercury Fanout call to invoke
   * Insights RPC DeleteClusterReplicationState on the replica PE to reset
   * IDF sync state before we start the next backup cycle
   *
   * @param peUuid - Uuid of replica PE for which
   *               DeleteClusterReplicationState RPC is being invoked
   * @return CompletableFuture<Boolean>
   */
  public CompletableFuture<Boolean> resetBackupSyncStateForPE(String peUuid) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        DeleteClusterReplicationStateArg.Builder deleteClusterReplicationStateArg =
                DeleteClusterReplicationStateArg.newBuilder();
        deleteClusterReplicationStateArg.setClusterUuid(peUuid);
        deleteClusterReplicationStateArg.setDeleteBackupReplicationState(true);
        DeleteClusterReplicationStateArg rpcArgs =
                deleteClusterReplicationStateArg.build();

        log.info("Initiating DeleteClusterReplicationState RPC call for PE " +
                "with Uuid: {} with Rpc Args: {}", peUuid, rpcArgs);
        DeleteClusterReplicationStateRet deleteClusterReplicationStateRet =
                insightsFanoutClient.invokeFanoutCall(
                        MercuryFanoutRpcClient.RpcName.DELETE_CLUSTER_REPLICATION_STATE,
                        rpcArgs,
                        DeleteClusterReplicationStateRet.newBuilder(),
                        peUuid);

        log.info("Completed DeleteClusterReplicationState RPC call for PE " +
                        "with Uuid: {} with status: {}", peUuid,
                deleteClusterReplicationStateRet);
        return Boolean.TRUE;
      }
      catch (Exception e) {
        log.error(String.format("Error encountered while invoking " +
                        "DeleteClusterReplicationState RPC call for " +
                        "PE with Uuid: %s",
                peUuid), e);
        return Boolean.FALSE;
      }
    });
  }

  /**
   * This method is responsible for updating IDF Sync State zkNode by
   * removing the replica UUID which is being deleted as part of
   * deleteReplica call, from IDF Backup Sync States zkNade.
   * .
   *
   * @param peClusterUuid
   */
  public void updateIdfSyncStateZkNode(String peClusterUuid) {
    String serializedReplicaPEUuids = null;
    List<String> replicaPEUuids = null;

    try {
      // first check if the IDF_BACKUP_SYNC_STATES zkNode exists
      Stat stat = zookeeperServiceHelper.exists(IDF_BACKUP_SYNC_STATES_ZK_PATH, false);
      if (stat == null) {
        log.info("IDF_BACKUP_SYNC_STATES zkNode does not exists, so returning" +
                "from updateIdfSyncStateZkNode");
        return;
      }
      // Read the list of PE Uuids for which reset IDF sync needs to be triggered
      byte[] data = zookeeperServiceHelper.getData(IDF_BACKUP_SYNC_STATES_ZK_PATH, stat);

      serializedReplicaPEUuids = new String(data);
      if (serializedReplicaPEUuids.equals(Constants.NO_DATA_ZK_VALUE)) {
        log.warn("Since there are no replicaPE's for which reset IDF sync " +
                "needs to be invoked, so returning from updateIdfSyncStateZkNode");
        return;
      }
      replicaPEUuids = Arrays.asList(serializedReplicaPEUuids.split(","));
      log.info("List of ReplicaPE UUIDs for which reset IDF sync is being " +
                      "triggered till now: {}",
              replicaPEUuids);

      // code to check if specific uuid is present,if yes then it removes it
      replicaPEUuids = replicaPEUuids
              .stream().filter(e -> !e.equalsIgnoreCase(peClusterUuid))
              .collect(Collectors.toList());

      serializedReplicaPEUuids = replicaPEUuids.isEmpty() ?
              Constants.NO_DATA_ZK_VALUE :
              String.join(",", replicaPEUuids);

      log.info("Updated list of ReplicaPE UUID's for which reset IDF sync " +
              "needs to be triggered afer removing ReplicaPE with UUID: {}: " +
              " {}", peClusterUuid, serializedReplicaPEUuids);

      zookeeperServiceHelper.setData(IDF_BACKUP_SYNC_STATES_ZK_PATH,
              serializedReplicaPEUuids.getBytes(), stat.getVersion());
    }
    catch (Exception e) {
      log.error("Error encountered while updating list of ReplicaPE UUID's on" +
              " IDF_BACKUP_SYNC_STATES zknode", e);
    }
  }

  /**
   * Fetch keyId from Mantle service for secret credentials provided
   * @param pcObjectStoreEndpointList - object store endpoint list
   * @return Map - map of endpoint address and secret key id
   * @throws PCResilienceException - can throw PC backup exception.
   */
  public Map<String, String> writeObjectStoreCredentialsInMantle(List<PcObjectStoreEndpoint> pcObjectStoreEndpointList)
          throws PCResilienceException {
    Map<String, String> credentialsKeyIdMap = new HashMap<>();
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : pcObjectStoreEndpointList) {
      String credentialkey;
      PcEndpointCredentials endpointCredentials = pcObjectStoreEndpoint.getEndpointCredentials();
      if (endpointCredentials != null && StringUtils.isNotEmpty(endpointCredentials.getAccessKey()) && StringUtils.isNotEmpty(endpointCredentials.getSecretAccessKey())) {
        try {
          ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto =
                  S3ServiceUtil.getCredentialsProtoFromEndpointCredentials(endpointCredentials);
          // fetch the mantle key from mantle service using object credentials proto
          credentialkey = mantleUtils.writeSecret(objectStoreCredentialsProto);
          log.debug("Credential Key Id for objectstore endpoint address {} is {}",
                  pcObjectStoreEndpoint.getEndpointAddress(), credentialkey);
          credentialsKeyIdMap.put(pcObjectStoreEndpoint.getEndpointAddress(), credentialkey);
        }
        catch (MantleException e) {
          log.error("Failed to store secrets in Mantle for {} with exception {} ",
                  pcObjectStoreEndpoint.getEndpointAddress(), e.getMessage());
          // Deleting the secret from mantle service executed successfully from objectstorelist
          if(!credentialsKeyIdMap.isEmpty()){
            rollbackSecretsFromMantleService(credentialsKeyIdMap);
          }
          throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
        }
      }
    }
    return credentialsKeyIdMap;
  }

  /**
   * Delete secret credentials from Mantle service
   * @param credentialsKeyIdMap  Map storing endPointAddress and mantlekeyId
   */
  private void rollbackSecretsFromMantleService(Map<String, String> credentialsKeyIdMap) {
    credentialsKeyIdMap.forEach((endPointAddress, credentialKeyId) -> {
      try {
        boolean resp = mantleUtils.deleteSecret(credentialKeyId);
        if (!resp) {
          log.error("Failed to delete secret credentials for objectstore endPointAddress {}",
                  endPointAddress);
        }
      }
      catch (MantleException exception) {
        log.error("Failed to delete secret credentials for objectstore endPointAddress: {} with exception {} ", endPointAddress, exception);
      }
    });
  }

  /**
   * Validating bucket access,bucket policy and object lock for provided credentials parellely.
   * if any of the validation fails we will fail the add replica api
   * @param objectStoreEndpointList
   * @throws PCResilienceException
   */
  protected void checkBucketAccessAndConfiguration(List<PcObjectStoreEndpoint> objectStoreEndpointList, Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap)
          throws PCResilienceException {
    Map<String, PCResilienceException> exceptionsMap = new ConcurrentHashMap<>();
    CompletableFuture bucketAccess = CompletableFuture.runAsync(() ->
            bucketHandler(
                    () -> checkBucketAccessForObjectStoreEndpoints(
                            objectStoreEndpointList, objectStoreEndPointDtoMap), BUCKET_ACCESS,
                    exceptionsMap), adonisServiceThreadPool);
    CompletableFuture bucketPolicy = CompletableFuture.runAsync(() ->
            bucketHandler(
                    () -> validateObjectStoreBucketPolicy(
                            objectStoreEndpointList, objectStoreEndPointDtoMap), BUCKET_POLICY,
                    exceptionsMap), adonisServiceThreadPool);
    CompletableFuture objectLock = CompletableFuture.runAsync(() ->
            bucketHandler(
                    () -> validateObjectStoreBucketObjectLock(
                            objectStoreEndpointList, objectStoreEndPointDtoMap), OBJECT_LOCK,
                    exceptionsMap), adonisServiceThreadPool);

    CompletableFuture<Void> bucketValidationFutures = CompletableFuture.allOf(bucketAccess, bucketPolicy, objectLock);
    try {
      // Wait for all tasks to complete
      bucketValidationFutures.join();
      // throw the pcbackup exception in order of execution method
      String[] methodOrder = {BUCKET_ACCESS, BUCKET_POLICY, OBJECT_LOCK};
      for (String methodName : methodOrder) {
        if (exceptionsMap.containsKey(methodName)) {
          throw exceptionsMap.get(methodName);
        }
      }
    } catch (CompletionException e) {
      log.error("Caught CompletionException in bucket validation: " + e.getMessage());
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
  }

  private void bucketHandler(RunnableWithCheckedException runnable, String methodAction, Map<String, PCResilienceException> exceptionsMap) {
    try {
      runnable.run();
    } catch (PCResilienceException e) {
      log.error("Failed to validate the {} with Exception: {}", methodAction, e);
      exceptionsMap.put(methodAction, e);
    }
  }

  /**
   * checking bucket access for provided credentials.
   * @param objectStoreEndpointList
   * @throws PCResilienceException
   */
  protected void checkBucketAccessForObjectStoreEndpoints(List<PcObjectStoreEndpoint> objectStoreEndpointList, Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap)
          throws PCResilienceException {
    String error;
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : objectStoreEndpointList) {
      ObjectStoreEndPointDto objectStoreEndPointDto = objectStoreEndPointDtoMap.get(pcObjectStoreEndpoint.getEndpointAddress());
      if (pcObjectStoreEndpoint.getEndpointCredentials() != null) {
        // create objectStore credentials proto and objectStoreClient instance on basis of endpoint flavour
        ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto =
                S3ServiceUtil.getCredentialsProtoFromEndpointCredentials(pcObjectStoreEndpoint.getEndpointCredentials());
        ObjectStoreHelper objectStoreClient = ObjectStoreHelperFactory.getObjectStoreHelper(
                pcObjectStoreEndpoint.getEndpointFlavour()
                        .toString());
        if (!objectStoreClient.checkBucketAccess(objectStoreEndPointDto,
                objectStoreCredentialsProto)) {
          String bucketName = pcObjectStoreEndpoint.getBucket();
          error = ErrorMessages.getObjectStoreBucketAccessErrorMessage(bucketName);
          log.error(error);
          Map<String,String> errorArguments = new HashMap<>();
          errorArguments.put(ErrorCodeArgumentMapper.ARG_BUCKET_NAME,bucketName);
          throw new PCResilienceException(error,ErrorCode.PCBR_BUCKET_ACCESS_OR_CREDENTIAL_FAILURE,
                  HttpStatus.BAD_REQUEST,errorArguments);
        }
      }
    }
  }

  /**
   * Validating if bucket life cycle policy configured for bucket.
   * @param objectStoreEndpointList
   * @throws PCResilienceException
   */
  protected void validateObjectStoreBucketPolicy(List<PcObjectStoreEndpoint> objectStoreEndpointList, Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap)
          throws PCResilienceException{
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : objectStoreEndpointList) {
      ObjectStoreEndPointDto objectStoreEndPointDto = objectStoreEndPointDtoMap.get(pcObjectStoreEndpoint.getEndpointAddress());
      if (pcObjectStoreEndpoint.getEndpointCredentials() != null) {
        ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto =
                S3ServiceUtil.getCredentialsProtoFromEndpointCredentials(pcObjectStoreEndpoint.getEndpointCredentials());
        ObjectStoreHelper objectStoreClient = ObjectStoreHelperFactory.getObjectStoreHelper(
                pcObjectStoreEndpoint.getEndpointFlavour()
                        .toString());
        objectStoreClient.validateObjectStoreBucketPolicy(objectStoreEndPointDto,
                objectStoreCredentialsProto,pcObjectStoreEndpoint.getBackupRetentionDays());
      }
    }
  }

  /**
   * Validating if object versioning lock policy configured for bucket.
   * @param objectStoreEndpointList
   * @throws PCResilienceException
   */
  protected void validateObjectStoreBucketObjectLock(List<PcObjectStoreEndpoint> objectStoreEndpointList, Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap)
          throws PCResilienceException {
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : objectStoreEndpointList) {
      ObjectStoreEndPointDto objectStoreEndPointDto = objectStoreEndPointDtoMap.get(pcObjectStoreEndpoint.getEndpointAddress());
      if (pcObjectStoreEndpoint.getEndpointCredentials() != null) {
        ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto =
                S3ServiceUtil.getCredentialsProtoFromEndpointCredentials(pcObjectStoreEndpoint.getEndpointCredentials());
        ObjectStoreHelper objectStoreClient = ObjectStoreHelperFactory.getObjectStoreHelper(
                pcObjectStoreEndpoint.getEndpointFlavour()
                        .toString());
        objectStoreClient.validateObjectStoreBucketObjectLock(objectStoreEndPointDto,
                objectStoreCredentialsProto,
                pcObjectStoreEndpoint.getBackupRetentionDays());
      }
    }
  }

  /**
   * Function to update rpo for a given entityId Subtasks - 1. Update rpoSecs in the
   * pc_backup_config entity for the given entityID. 2. If pc_backup_config is updated then update
   * the pc_backup_metadata table.
   *
   * @param entityId  - backupTargetID(can be object store entityId or pe cluster uuid) given as
   *                  input in API request.
   * @param rpoConfig
   */
  @Override
  public void updateRpo(String entityId, RpoConfig rpoConfig)
          throws PCResilienceException {

    //Check if PC upgrade in progress
    if (pcUpgradeWatcher.isUpgradeInProgress()) {
      log.warn("Upgrade is in progress, not proceeding with updateRpo");
      throw new PCResilienceException(ErrorMessages.UPDATE_BACKUP_TARGET_RPO_UPGRADE_IN_PROGRESS_ERROR,
              ErrorCode.PCBR_UPGRADE_IN_PROGRESS, HttpStatus.SERVICE_UNAVAILABLE);
    }

    PcBackupConfig pcBackupConfig = PcBackupConfig.getDefaultInstance();
    String bucketName;
    // Fetch pc backup configs by entityId.
    // This method throws PC backup Exception if entity does not exists.
    try {
      pcBackupConfig = PCUtil.getPcBackupConfigById(entityId, pcRetryHelper);
      ObjectStoreHelper helper = ObjectStoreHelperFactory.getObjectStoreHelper(pcBackupConfig.getObjectStoreEndpoint().getEndpointFlavour().name());
      bucketName = helper.getBucketNameByEndpoint(pcBackupConfig.getObjectStoreEndpoint().getEndpointAddress());
    } catch (PCResilienceException e) {
      throw e;
    }

    // Validate if BackupTargets exists in pc_backup_metadata table in IDF
    // Throw PCResilienceException if backup target does not exist.
    PCVMBackupTargets pcvmBackupTargets = pcvmDataService.fetchPcvmBackupTargets();
    log.info("Fetched data from backup_metadata {} {}",bucketName,pcvmBackupTargets.getBackupTargetsList());

    //Update rpo configuration in  pc_backup_config and pc_backup_metadata
    updateEntitiesWithNewRpoConfigs(entityId,bucketName,rpoConfig,pcBackupConfig,
            pcvmBackupTargets);

    log.info("Updated rpo config {} for bucket {}",bucketName,rpoConfig.getRpoSeconds());
  }

  /**
   * Method to update rpo config for given entityId
   * @param entityId
   * @param rpoConfig
   * @param pcBackupConfig
   * @param pcvmBackupTargets
   * @throws PCResilienceException
   */
  private void updateEntitiesWithNewRpoConfigs(String entityId, String bucketName, RpoConfig rpoConfig,
                                               PcBackupConfig pcBackupConfig, PCVMBackupTargets pcvmBackupTargets) throws PCResilienceException {
    int oldRpo = pcBackupConfig.getRpoSecs();
    if (oldRpo != rpoConfig.getRpoSeconds()) {    // Update rpo in pc_backup_config entity in IDF
      updatePcBackupConfig(entityId, bucketName, pcBackupConfig, rpoConfig);
      // Update rpo in pc_backup_metadata config in IDF
      updateRpoAndCertsInPcBackupMetadata(bucketName, pcBackupConfig, rpoConfig,
              pcvmBackupTargets, true, oldRpo, entityId, StringUtils.EMPTY, ByteString.EMPTY);
    }
  }

  /**
   * This method updates pc_backup_config with new rpo values retaining other attributes
   * @param entityId
   * @param pcBackupConfig
   * @param rpoConfig
   * @throws PCResilienceException
   */
  private void updatePcBackupConfig(String entityId, String bucketName, PcBackupConfig pcBackupConfig, RpoConfig rpoConfig)
          throws PCResilienceException {

    Map<String, Object> attributesValueMap = new HashMap<>();
    attributesValueMap.put(
            Constants.ZPROTOBUF,
            CompressionUtils.compress(
                    PcBackupConfig.newBuilder(pcBackupConfig)
                            .setRpoSecs(rpoConfig.getRpoSeconds())
                            .build()
                            .toByteString()
            )
    );
    // Construct updateEntityBuilder
    UpdateEntityArg.Builder updateEntityArgBuilder =
            IDFUtil.constructUpdateEntityArgBuilder(
                    Constants.PC_BACKUP_CONFIG,
                    entityId ,
                    attributesValueMap);
    try{
      entityDBProxy.updateEntity(updateEntityArgBuilder.build());
    } catch (InsightsInterfaceException e) {
      String error = ErrorMessages.getUpdateRpoConfigErrorMessage(bucketName);
      log.error(error, e);
      Map<String,String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_BUCKET_NAME,bucketName);
      throw new PCResilienceException(error, ErrorCode.PCBR_DATABASE_UPDATE_RPO_CONFIG_ERROR,
              HttpStatus.INTERNAL_SERVER_ERROR,errorArguments);
    }
  }

  /**
   *
   * @param bucketName
   * @param pcBackupConfig
   * @param rpoConfig
   * @param pcvmBackupTargets
   * @param rollback
   * @param oldRpo
   * @param entityId
   * @throws PCResilienceException
   */
  protected void updateRpoAndCertsInPcBackupMetadata(String bucketName, PcBackupConfig pcBackupConfig, RpoConfig rpoConfig,
                                                     PCVMBackupTargets pcvmBackupTargets, boolean rollback, int oldRpo, String entityId, String certPath, ByteString certificate) throws  PCResilienceException{
    Map<String,Object> attributesValueMap = new HashMap<>();
    ObjectStoreEndpoint objectStoreEndpoint = pcBackupConfig.getObjectStoreEndpoint();
    // This should always be present.
    attributesValueMap.put(
            Constants.PCVM_BACKUP_TARGETS,
            CompressionUtils.compress(
                    constructPCBackupTargetsWithRpoOrCerts(objectStoreEndpoint,rpoConfig,certPath, certificate, pcvmBackupTargets)
                            .toByteString()
            )
    );
    UpdateEntityArg.Builder updateMetadataEntityArgBuilder =
            IDFUtil.constructUpdateEntityArgBuilder(
                    Constants.PC_BACKUP_METADATA,
                    instanceServiceFactory.getClusterUuidFromZeusConfig(),
                    attributesValueMap);
    updateMetadataEntityArgBuilder.setFullUpdate(true);

    try {
      entityDBProxy.updateEntity(updateMetadataEntityArgBuilder.build());
    } catch (InsightsInterfaceException e) {
      String error = ErrorMessages.getUpdateRpoConfigErrorMessage(bucketName);
      log.error(error, e);
      if (rollback){
        try {
          //Rollback to older rpo config as update to pc_metadata failed
          updatePcBackupConfig(entityId, bucketName, pcBackupConfig, new RpoConfig(oldRpo));
        }catch (Exception ex){
          log.warn("Unable to rollback pc backup config for bucket {}",bucketName);
          return;
        }
      }
      throw new PCResilienceException(error, ErrorCode.PCBR_DATABASE_WRITE_BACKUP_DATA_ERROR,
              HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  protected void updateCertsInPcBackupMetadata(String bucketName, PcBackupConfig pcBackupConfig, String certPath,
                                               ByteString certificate, PCVMBackupTargets pcvmBackupTargets) throws PCResilienceException{

    if(!StringUtils.isEmpty(certPath)) {
      Map<String, Object> attributesValueMap = new HashMap<>();
      ObjectStoreEndpoint objectStoreEndpoint = pcBackupConfig.getObjectStoreEndpoint();
      // This should always be present.
      attributesValueMap.put(
              Constants.PCVM_BACKUP_TARGETS,
              CompressionUtils.compress(
                      constructPCBackupTargetsWithRpoOrCerts(objectStoreEndpoint, new RpoConfig(), certPath, certificate, pcvmBackupTargets)
                              .toByteString()
              )
      );
      UpdateEntityArg.Builder updateMetadataEntityArgBuilder =
              IDFUtil.constructUpdateEntityArgBuilder(
                      Constants.PC_BACKUP_METADATA,
                      instanceServiceFactory.getClusterUuidFromZeusConfig(),
                      attributesValueMap);
      updateMetadataEntityArgBuilder.setFullUpdate(true);

      try {
        entityDBProxy.updateEntity(updateMetadataEntityArgBuilder.build());
      } catch (InsightsInterfaceException e) {
        String error = ErrorMessages.getUpdateRpoConfigErrorMessage(bucketName);
        log.error(error, e);
        throw new PCResilienceException(error, ErrorCode.PCBR_DATABASE_WRITE_BACKUP_DATA_ERROR,
                HttpStatus.INTERNAL_SERVER_ERROR);
      }
    }
  }

  /**
   * Construct PCBackupTargets with RPO
   * @param objectStoreEndpoint
   * @param rpoConfig
   * @param pcvmBackupTargets
   * @return updated PCVMBackupTargets
   */
  private PCVMBackupTargets constructPCBackupTargetsWithRpoOrCerts(ObjectStoreEndpoint objectStoreEndpoint,RpoConfig rpoConfig,
                                                                   String certPath, ByteString certificate, PCVMBackupTargets pcvmBackupTargets) {

    //populating all the data for existing attribute names.
    PCVMBackupTargets.Builder resultTarget = PCVMBackupTargets.newBuilder();
    pcvmBackupTargets.getBackupTargetsList().stream().forEach(target->{
      resultTarget.addBackupTargets(target);
    });
    //Update RPO of the target with same endpoint address as that of the give entityId
    List<ObjectStoreBackupTarget> objectStoreBackupTargetList = pcvmBackupTargets.getObjectStoreBackupTargetsList();
    objectStoreBackupTargetList.stream().forEach(objectStoreBackupTarget -> {
      log.debug("Comparing endpoints {} and {} to update rpo.", objectStoreBackupTarget.getEndpointAddress(),objectStoreEndpoint.getEndpointAddress());
      ObjectStoreBackupTarget.Builder backupTarget = PCVMBackupTargets.ObjectStoreBackupTarget.newBuilder(objectStoreBackupTarget)
              .setEndpointAddress(objectStoreBackupTarget.getEndpointAddress())
              .setLastSyncTimestamp(objectStoreBackupTarget.getLastSyncTimestamp())
              .setEndpointFlavour(ObjectStoreBackupTarget.EndpointFlavour
                      .valueOf(objectStoreEndpoint.getEndpointFlavour().name()));
      if (!StringUtils.isEmpty(certPath)) {
        String updatedCertPath = StringUtils.equals(objectStoreBackupTarget.getEndpointAddress(),
                objectStoreEndpoint.getEndpointAddress()) ? certPath : objectStoreBackupTarget.getCertificatePath();
        ByteString updatedCertificate = StringUtils.equals(objectStoreBackupTarget.getEndpointAddress(),
                objectStoreEndpoint.getEndpointAddress()) ? certificate : objectStoreBackupTarget.getCertificateContent();
        if (!ObjectUtils.isEmpty(updatedCertPath)) {
          backupTarget.setCertificatePath(updatedCertPath).setCertificateContent(updatedCertificate).build();
        }
      }
      if (!ObjectUtils.isEmpty(rpoConfig.getRpoSeconds()) && rpoConfig.getRpoSeconds() != 0){
        int rpoSeconds = StringUtils.equals(objectStoreBackupTarget.getEndpointAddress(),
                objectStoreEndpoint.getEndpointAddress())  ? rpoConfig.getRpoSeconds() : objectStoreBackupTarget.getRpoSecs();
        backupTarget.setRpoSecs(rpoSeconds);
      }
      resultTarget.addObjectStoreBackupTargets(backupTarget);
    });
    return resultTarget.build();
  }

  /**
   * This method is responsible for getting pcBackupConfig for a given entityId
   *
   * @param backupTargetID - target id of the backup replica
   * @return - returns the PcBackupConfig object
   * @throws PCResilienceException - can throw exception.
   */
  public PcBackupConfig getBackupTargetById(String backupTargetID) throws PCResilienceException {

    PcBackupConfig pcBackupConfig =
            pcRetryHelper.fetchPCBackupConfigWithEntityId(backupTargetID);
    // If both cluster uuid and object store endpoint are not present that
    // means pc backup config is empty and there is no replica present to
    // remove.
    if (pcBackupConfig != null && !(pcBackupConfig.hasClusterUuid() ||
            pcBackupConfig.hasObjectStoreEndpoint())) {
      Map<String,String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_BACKUP_TARGET_EXT_ID,backupTargetID);
      throw new PCResilienceException(ErrorMessages.BACKUP_TARGET_WITH_ENTITY_ID_NOT_FOUND,
              ErrorCode.PCBR_BACKUP_TARGET_NOT_FOUND,HttpStatus.NOT_FOUND,errorArguments);
    }
    return pcBackupConfig;
  }

  /**
   * This method is responsible for updating credentials for a given entityID in mantle,store the
   * mantle keyId in IDF.
   * @param pcEndpointCredentials
   * @throws PCResilienceException
   */
  @Override
  public void updateCredentials(String entityId, PcEndpointCredentials pcEndpointCredentials)
          throws PCResilienceException {

    //Get endpoint Address for the given entityId from IDF (pc_backup_config)
    PcBackupConfig pcBackupConfig = PcBackupConfig.getDefaultInstance();
    pcBackupConfig = PCUtil.getPcBackupConfigById(entityId, pcRetryHelper);
    PcBackupConfig.ObjectStoreEndpoint objectStoreEndpoint = pcBackupConfig.getObjectStoreEndpoint();

    String clusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    ObjectStoreEndPointDto objectStoreEndPointDto = S3ObjectStoreUtil.getS3EndpointDTOFromObjectStoreEndpoint(objectStoreEndpoint, entityId, clusterUuid, entityDBProxy);

    String bucketName = objectStoreEndpoint.getBucketName();
    String certPath = StringUtils.EMPTY;
    String certificate = StringUtils.EMPTY;
    String credentialKeyId;
    ByteString encodedCert = null;
    boolean accessCredentialsProvided = !ObjectUtils.isEmpty(pcEndpointCredentials.getAccessKey()) &&
            !ObjectUtils.isEmpty(pcEndpointCredentials.getSecretAccessKey());
    if (!ObjectUtils.isEmpty(pcEndpointCredentials) && !ObjectUtils.isEmpty(pcEndpointCredentials.getCertificate())){
      if (!ObjectUtils.isEmpty(objectStoreEndPointDto.getCertificatePath())){
        certPath = CertificatesUtility.updateCertificatePath(objectStoreEndPointDto.getCertificatePath());
      } else {
        certPath = PCBR_OBJECTSTORE_CERTS_PATH +
                PCUtil.getObjectStoreEndpointUuid(instanceServiceFactory.getClusterUuidFromZeusConfig(),
                        objectStoreEndpoint.getEndpointAddress()) + PCBR_OBJECTSTORE_CERTS_EXTENSION;
      }
      certificate = CertificatesUtility.normalizeAndValidateCertificateContent(pcEndpointCredentials.getCertificate());
      objectStoreEndPointDto.setCertificatePath(certPath);
      objectStoreEndPointDto.setCertificateContent(certificate);
    } else {
      validate(pcEndpointCredentials);
    }

    if(!accessCredentialsProvided){
      log.info("Access Credentials are not provided");
      credentialKeyId = pcBackupConfig.getObjectStoreEndpoint().getCredentialsKeyId();
      ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto = ClusterExternalStateProto.
              ObjectStoreCredentials.getDefaultInstance();
      try {
        objectStoreCredentialsProto = mantleUtils.fetchSecret(credentialKeyId, objectStoreCredentialsProto);
      } catch (MantleException e) {
        throw new PCResilienceException("Unable to fetch credentials from credential store",
                ErrorCode.PCBR_MANTLE_FETCH_CREDENTIAL_FAILURE, HttpStatus.INTERNAL_SERVER_ERROR);
      }
      pcEndpointCredentials.setAccessKey(objectStoreCredentialsProto.getAccessKey());
      pcEndpointCredentials.setSecretAccessKey(objectStoreCredentialsProto.getSecretAccessKey());
      log.debug("Checking access for bucket {} with new certificates",bucketName);
      //Check bucket access against new credentials or certs
      checkBucketAccess(bucketName,objectStoreEndpoint,objectStoreEndPointDto,pcEndpointCredentials);
    } else {
      log.debug("Checking access for bucket {} with new access credentials",bucketName);
      //Check bucket access against new credentials or certs
      checkBucketAccess(bucketName,objectStoreEndpoint,objectStoreEndPointDto,pcEndpointCredentials);
      // Update new credentials in mantle and get the mantle key Id
      credentialKeyId = writeSecret(bucketName,pcEndpointCredentials);
      log.info("New Credential KeyId updated");
    }

    if (!ObjectUtils.isEmpty(pcEndpointCredentials) && !ObjectUtils.isEmpty(pcEndpointCredentials.getCertificate())) {
      certFileUtil.createCertFileOnAllPCVM(Arrays.asList(objectStoreEndPointDto), false);
      encodedCert = CertificatesUtility.getCertificateByteStringFromString(
              objectStoreEndPointDto.getCertificateContent());
    }

    PCVMBackupTargets pcvmBackupTargets = pcvmDataService.fetchPcvmBackupTargets();

    updateCertsInPcBackupMetadata(bucketName, pcBackupConfig, certPath, encodedCert, pcvmBackupTargets);

    // update pcBackUpConfig with credentialsId(and/or) from mantle
    updateCredentialsInPcBackupConfigWithRollback(entityId,credentialKeyId,bucketName,certPath,pcBackupConfig,pcvmBackupTargets);
    log.info("Credentials Updated successfully");

    // If credentials are valid, erase old credentials from mantle
    if(pcBackupConfig.getObjectStoreEndpoint().hasCredentialsKeyId() && accessCredentialsProvided) {
      String oldCredentialKeyId = pcBackupConfig.getObjectStoreEndpoint().getCredentialsKeyId();
      deleteSecret(oldCredentialKeyId, bucketName);
    }
  }

  /**
   * Update secrets in mantle and return new keyId
   * @param bucketName
   * @param pcEndpointCredentials
   * @return
   * @throws PCResilienceException
   */
  public String writeSecret(String bucketName,
                            PcEndpointCredentials pcEndpointCredentials) throws PCResilienceException{
    String credentialKey;
    try {
      ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto =
              S3ServiceUtil.getCredentialsProtoFromEndpointCredentials(pcEndpointCredentials);
      // write secrets into mantle
      credentialKey = mantleUtils.writeSecret(objectStoreCredentialsProto);

    }
    catch (MantleException e) {
      String error = String.format("Failed to store secrets for bucket %s",bucketName);
      log.error(error, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    return credentialKey;
  }

  /**
   * Delete secret key for give keyId
   * @param credentialKeyId
   * @param bucketName
   */
  private void deleteSecret(String credentialKeyId,String bucketName){
    try {
      boolean resp = mantleUtils.deleteSecret(credentialKeyId);
      if (!resp) {
        String error = String.format("Failed to delete existing secret credentials for bucket: %s",
                bucketName);
        log.warn(error);
      }
    }
    catch (MantleException exception) {
      String error = String.format("Error occurred while deleting secret for bucket : "
              + "%s with credential keyID %s", bucketName,credentialKeyId);
      log.warn("{}",error,exception);
    }
  }

  /**
   *  Validate new endpoint credentials. Check bucket access with new credentials
   * @param objectStoreEndpoint
   * @param newEndpointCredentials
   * @throws PCResilienceException
   */
  private void checkBucketAccess(String bucketName, ObjectStoreEndpoint objectStoreEndpoint, ObjectStoreEndPointDto objectStoreEndPointDto, PcEndpointCredentials newEndpointCredentials)
          throws PCResilienceException {

    validate(newEndpointCredentials);
    // create objectStore credentials proto and objectStoreClient instance on basis of endpoint flavour
    ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto =
            S3ServiceUtil.getCredentialsProtoFromEndpointCredentials(newEndpointCredentials);
    ObjectStoreHelper objectStoreClient = ObjectStoreHelperFactory.getObjectStoreHelper(
            objectStoreEndpoint.getEndpointFlavour()
                    .toString());
    if (!objectStoreClient.checkBucketAccess(objectStoreEndPointDto,
            objectStoreCredentialsProto)) {
      String error =  ErrorMessages.getObjectStoreBucketAccessErrorMessage(bucketName);
      log.error(error);
      Map<String,String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_BUCKET_NAME,bucketName);
      throw new PCResilienceException(error, ErrorCode.PCBR_BUCKET_ACCESS_OR_CREDENTIAL_FAILURE,
              HttpStatus.BAD_REQUEST,errorArguments);
    }
  }

  /**
   * Validate Endpoint Credentials
   * @param pcEndpointCredentials
   * @throws PCResilienceException
   */
  private void validate(PcEndpointCredentials pcEndpointCredentials) throws PCResilienceException {
    if (!ObjectUtils.isEmpty(pcEndpointCredentials) && (
            StringUtils.isEmpty(pcEndpointCredentials.getAccessKey()) ||
                    StringUtils.isEmpty(pcEndpointCredentials.getSecretAccessKey()))) {
      log.error("AccessKey or Secret AccessKey cannot be empty");
      throw new PCResilienceException(ErrorMessages.EMPTY_CREDENTIAL_KEY_ERROR,
              ErrorCode.PCBR_INVALID_BUCKET_DETAILS, HttpStatus.BAD_REQUEST);
    }
  }

  /**
   * Update keyId in pc_backup_config
   * @param entityId
   * @param keyId
   * @param pcBackupConfig
   * @throws PCResilienceException
   */
  private void updateCredentialsInPcBackupConfigWithRollback(String entityId, String keyId, String bucketName, String certPath,
                                                             PcBackupConfig pcBackupConfig, PCVMBackupTargets pcvmBackupTargets)
          throws PCResilienceException {

    Map<String, Object> attributesValueMap = new HashMap<>();
    ObjectStoreEndpoint.Builder builder = ObjectStoreEndpoint.newBuilder(pcBackupConfig.getObjectStoreEndpoint());
    if(!StringUtils.isEmpty(keyId)){
      builder.setCredentialsKeyId(keyId);
    }
    if(!StringUtils.isEmpty(certPath)){
      builder.setCertificatePath(certPath);
    }
    attributesValueMap.put(
            Constants.ZPROTOBUF,
            CompressionUtils.compress(
                    PcBackupConfig.newBuilder(pcBackupConfig)
                            .setCredentialsModifiedTimestampUsecs(Instant.now().toEpochMilli() * 1000)
                            .setObjectStoreEndpoint(builder.build())
                            .build()
                            .toByteString()
            )
    );
    // Construct update query
    UpdateEntityArg.Builder updateEntityArgBuilder =
            IDFUtil.constructUpdateEntityArgBuilder(
                    Constants.PC_BACKUP_CONFIG,
                    entityId ,
                    attributesValueMap);
    try{
      entityDBProxy.updateEntity(updateEntityArgBuilder.build());
    } catch (InsightsInterfaceException e) {
      Map<String,String> errorArguments = new HashMap<>();
      String error;
      if (!StringUtils.isEmpty(certPath)){
        PCVMBackupTargets.ObjectStoreBackupTarget objectStoreBackupTarget = pcvmDataService.getObjectStoreBackupTargetByEntityId(pcvmBackupTargets, entityId);
        updateCertsInPcBackupMetadata(bucketName, pcBackupConfig, objectStoreBackupTarget.getCertificatePath(), objectStoreBackupTarget.getCertificateContent(), pcvmBackupTargets);
      }
      if (!StringUtils.isEmpty(keyId)) {
        deleteSecret(keyId, bucketName);
      }
      error = ErrorMessages.getBucketUpdateCredentialErrorMessage(bucketName);
      errorArguments.put(ErrorCodeArgumentMapper.ARG_BUCKET_NAME,bucketName);
      throw new PCResilienceException(error, ErrorCode.PCBR_UPDATE_BUCKET_CREDENTIALS_FAILURE,
              HttpStatus.INTERNAL_SERVER_ERROR,errorArguments);
    }
  }

  protected void updateRpoAndCredentialsInPcBackupConfigWithRollback(String entityId, String keyId, String bucketName,
                                                                     PcBackupConfig pcBackupConfig, RpoConfig rpoConfig, PCVMBackupTargets pcvmBackupTargets,
                                                                     String certPath, ByteString certificate)
          throws PCResilienceException {

    Map<String, Object> attributesValueMap = new HashMap<>();
    ObjectStoreEndpoint.Builder builder = ObjectStoreEndpoint.newBuilder(pcBackupConfig.getObjectStoreEndpoint());
    builder.setCredentialsKeyId(keyId);
    if (!ObjectUtils.isEmpty(certPath)) {
      builder.setCertificatePath(certPath);
    }
    attributesValueMap.put(
            Constants.ZPROTOBUF,
            CompressionUtils.compress(
                    PcBackupConfig.newBuilder(pcBackupConfig)
                            .setCredentialsModifiedTimestampUsecs(Instant.now().toEpochMilli() * 1000)
                            .setObjectStoreEndpoint(builder.build())
                            .setRpoSecs(rpoConfig.getRpoSeconds())
                            .build()
                            .toByteString()
            )
    );

    // Construct update query
    UpdateEntityArg.Builder updateEntityArgBuilder =
            IDFUtil.constructUpdateEntityArgBuilder(
                    Constants.PC_BACKUP_CONFIG,
                    entityId ,
                    attributesValueMap);
    try{
      entityDBProxy.updateEntity(updateEntityArgBuilder.build());
    } catch (InsightsInterfaceException e) {
      String error = ErrorMessages.getBucketUpdateCredentialErrorMessage(bucketName);
      log.error(error, e);
      try{
        ObjectStoreBackupTarget objectStoreBackupTarget = pcvmDataService.getObjectStoreBackupTargetByEntityId(pcvmBackupTargets, entityId);
        updateRpoAndCertsInPcBackupMetadata(bucketName, pcBackupConfig, new RpoConfig(pcBackupConfig.getRpoSecs()),
                pcvmBackupTargets, false, 0, StringUtils.EMPTY, objectStoreBackupTarget.getCertificatePath(), objectStoreBackupTarget.getCertificateContent());
      } catch (Exception ex){
        log.warn("Unable to rollback pc backup metadata for bucket {}",bucketName);
      }

      deleteSecret(keyId,bucketName);
      Map<String,String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_BUCKET_NAME,bucketName);
      throw new PCResilienceException(error, ErrorCode.PCBR_UPDATE_BUCKET_DETAILS_FAILURE,
              HttpStatus.INTERNAL_SERVER_ERROR,errorArguments);
    }
  }

  @Override
  public void addReplicas(final Set<String> peClusterUuidSet,
                          List<PcObjectStoreEndpoint> objectStoreEndpointList) throws PCResilienceException {
    try {
      addReplicas(peClusterUuidSet, objectStoreEndpointList, null);
    } catch (ErgonException e) {
      String error = String.format("Hitting the following Ergon Exception while " +
              "updating the task %s", e.getMessage());
      log.error(error);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
  }
  private List<String> getClusterUuidInBackupConfig(Set<String> peClusterUuidSet) throws PCResilienceException {

    List<String> clusterUuidInPCBackupConfig =
            pcRetryHelper.fetchExistingClusterUuidInPCBackupConfig();
    modifyWithoutPCBackupConfigEntries(peClusterUuidSet,
            clusterUuidInPCBackupConfig);
    return clusterUuidInPCBackupConfig;
  }

  private ObjectStoreCredentialsBackupEntity getObjectStoreCredentialsBackupEntityInPCBackupConfig(
          List<PcObjectStoreEndpoint> pcObjectStoreEndpointList) throws PCResilienceException {
    ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntityInPcBackupConfig =
            pcRetryHelper.fetchExistingObjectStoreEndpointInPCBackupConfigWithCredentials();
    List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig =
            objectStoreCredentialsBackupEntityInPcBackupConfig.getObjectStoreEndpoints();

    log.info("Object store endpoints from backup config  - {}",
            objectStoreEndpointInPCBackupConfig);
    modifyWithoutPCBackupConfigObjectStoreEntries(pcObjectStoreEndpointList,
            objectStoreEndpointInPCBackupConfig);
    return objectStoreCredentialsBackupEntityInPcBackupConfig;
  }

  @Override
  public void isBackupTargetEligibleForBackup(Set<String> peClusterUuidSet,
                                              List<PcObjectStoreEndpoint> pcObjectStoreEndpointList) throws PCResilienceException {

    // STEP - 1
    // Verifying whether the clusters sent in the request are part of
    // eligible clusters or not. If any one of them is not a part raise an
    // exception.
    checkIfPesEligibleForBackup(peClusterUuidSet, false);

    // Modify the peClusterUuidSet and objectStoreEndpointList
    // If nothing is left after checking from pc_backup_config
    // throw an exception.
    List<String> clusterUuidInPCBackupConfig =
            getClusterUuidInBackupConfig(peClusterUuidSet);

    ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntityInPCBackupConfig =
            getObjectStoreCredentialsBackupEntityInPCBackupConfig(pcObjectStoreEndpointList);
    List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig
            = objectStoreCredentialsBackupEntityInPCBackupConfig.getObjectStoreEndpoints();

    if (peClusterUuidSet.isEmpty() && pcObjectStoreEndpointList.isEmpty()) {
      String errorMessage = "Nothing to add in pc_backup_config table. " +
              "The ClusterUuids/ObjectStoreEndpoints sent are " +
              "already present in the database";
      log.warn(errorMessage);
      log.debug("PC backup config contains following cluster uuid : " +
              clusterUuidInPCBackupConfig.toString());
      throw new PCResilienceException(
              ErrorMessages.ADD_REPLICA_BACKUP_TARGET_ALREADY_PRESENT_ERROR,
              ErrorCode.PCBR_ALREADY_PROTECTED, HttpStatus.BAD_REQUEST);
    }

    // Checking the total count of replicas does not exceed maximum allowed
    // cluster value.
    checkAllowedReplicasCount(
            peClusterUuidSet, pcObjectStoreEndpointList,
            clusterUuidInPCBackupConfig, objectStoreEndpointInPCBackupConfig);
  }

  @Override
  public void removeCredentialKeyFromMantle(String credentialKeyId){
    try {
      if (!org.springframework.util.StringUtils.isEmpty(credentialKeyId)){
        if (mantleUtils.deleteSecret(credentialKeyId))
          log.info("Successfully deleted credential key id from mantle");
        else
          log.warn("Failed to delete credential key id from mantle");
      }
    }
    catch (Exception e){
      log.warn("Failed to delete credential key id from mantle");
    }
  }

  @Override
  public String generateEtag(BackupTarget backupTarget) throws PCResilienceException {

    BackupTargetDTO backupTargetDTO = null;
    if (backupTarget.getLocation() instanceof ClusterLocation){
      ClusterLocation clusterLocation = (ClusterLocation) backupTarget.getLocation();
      ClusterReference clusterReference = (ClusterReference) clusterLocation.getConfig();
      backupTargetDTO = new BackupTargetDTO(backupTarget.getExtId(), clusterReference.getExtId(), clusterReference.getName());
      return clusterHelper.generateEtagForBackupTarget(backupTargetDTO);
    } else {
      ObjectStoreLocation objectStoreLocation = (ObjectStoreLocation) backupTarget.getLocation();
      ClusterExternalStateProto.PcBackupConfig pcBackupConfig = getBackupTargetById(backupTarget.getExtId());
      String credentialId = pcBackupConfig.getObjectStoreEndpoint().getCredentialsKeyId();
      String certificatePath = pcBackupConfig.getObjectStoreEndpoint().getCertificatePath();

      backupTargetDTO = getBackupTargetDtoForObjectStore(objectStoreLocation, credentialId, certificatePath);
      ObjectStoreHelper objectStoreHelper = ObjectStoreHelperFactory
              .getObjectStoreHelper(S3ServiceUtil.getObjectStoreEndpointFlavour(objectStoreLocation).toString());
      return objectStoreHelper.generateEtagForBackupTarget(backupTargetDTO);
    }
  }

  private BackupTargetDTO getBackupTargetDtoForObjectStore(ObjectStoreLocation objectStoreLocation, String credentialKeyId,
                                                           String certificatePath) {

    BackupTargetDTO backupTargetDTO = new BackupTargetDTO();
    if (objectStoreLocation.getProviderConfig() instanceof AWSS3Config ){
      AWSS3Config awss3Config = (AWSS3Config) objectStoreLocation.getProviderConfig();
      backupTargetDTO.setBucketName(awss3Config.getBucketName());
      backupTargetDTO.setRegion(awss3Config.getRegion());
      backupTargetDTO.setCredentialKeyId(credentialKeyId);
      backupTargetDTO.setRpoInMinutes(objectStoreLocation.getBackupPolicy().getRpoInMinutes());
    } else if (objectStoreLocation.getProviderConfig() instanceof NutanixObjectsConfig) {
      NutanixObjectsConfig nutanixObjectsConfig = (NutanixObjectsConfig) objectStoreLocation.getProviderConfig();
      backupTargetDTO.setBucketName(nutanixObjectsConfig.getBucketName());
      backupTargetDTO.setRegion(nutanixObjectsConfig.getRegion());
      backupTargetDTO.setCredentialKeyId(credentialKeyId);
      backupTargetDTO.setIpAddressOrHostname(
              prismCentralBackupConverter.getStringFromIPAddressOrFQDN(
                      nutanixObjectsConfig.getConnectionConfig().getIpAddressOrHostname()));
      backupTargetDTO.setRpoInMinutes(objectStoreLocation.getBackupPolicy().getRpoInMinutes());
      backupTargetDTO.setSkipCertificateValidation(nutanixObjectsConfig.getConnectionConfig().getShouldSkipCertificateValidation());
      if (!ObjectUtils.isEmpty(certificatePath)) {
        backupTargetDTO.setCertificatePath(certificatePath);
      }
    }
    return backupTargetDTO;
  }

  private String getBackupTargetCredentialId(BackupTarget backupTarget)
          throws PCResilienceException {

    ClusterExternalStateProto.PcBackupConfig pcBackupConfig = null;
    pcBackupConfig = getBackupTargetById(backupTarget.getExtId());
    return pcBackupConfig.getObjectStoreEndpoint().getCredentialsKeyId();
  }

  public BiMap<List, String> createLcmGenesisVersionMapping(PCDRYamlAdapterImpl pcdrYamlAdapter){
    BiMap<List, String> lcmGenesisMapping = HashBiMap.create();
    List<PCDRYamlAdapterImpl.LcmGenesisServiceMapping> serviceMappings = pcdrYamlAdapter.getLcmGenesisServiceMapping();
    List<String> lcmMapping;
    for (PCDRYamlAdapterImpl.LcmGenesisServiceMapping serviceMapping : serviceMappings){
      lcmGenesisMapping.put(serviceMapping.getLcmService(), serviceMapping.getGenesisService());
    }
    log.info("lcmGenesisMapping : " + lcmGenesisMapping);
    return lcmGenesisMapping;
  }

  public List<Entity> getLcmEntities() throws PCResilienceException {
    log.info("Calling LCM Entities API");
    ListEntitiesApiResponse response;
    ApiClient apiClient;
    List<Entity> lcmEntities = new ArrayList<>();

    EntitiesApi entitiesApi = apiClientGenerator.getLcmEntitiesApiInstance();

    response = entitiesApi.listEntities(0, 50, null, null, null);
    if (ObjectUtils.isEmpty(response) || ObjectUtils.isEmpty(response.getData())){
      log.error("Unable to get a response from LCM entities API");
      return lcmEntities;
    }

    log.info("Get entities API output: {}", response.getData());
    lcmEntities = (List<Entity>) (response.getData());
    return lcmEntities;
  }

  public Map<String, String> buildServiceVersionMap() throws PCResilienceException {
    List<Entity> lcmEntities = getLcmEntities();
    BiMap<List, String> lcmGenesisMapping = getlcmGenesisMapping();
    Map<String, String> genesisVersionMapping = new HashMap<>();
    for (Entity lcmEntity : lcmEntities){
      if(!ObjectUtils.isEmpty(lcmEntity.getEntityClass()) &&
              !ObjectUtils.isEmpty(lcmEntity.getEntityModel()) &&
              !ObjectUtils.isEmpty(lcmEntity.getEntityType()) &&
              !ObjectUtils.isEmpty(lcmEntity.getTargetVersion())){
        List<String> lcmEntityList = new ArrayList<>();
        lcmEntityList.add(lcmEntity.getEntityClass());
        lcmEntityList.add(lcmEntity.getEntityModel());
        lcmEntityList.add(lcmEntity.getEntityType().toString());
        String version = lcmEntity.getTargetVersion();
        String genesisServiceName = lcmGenesisMapping.get(lcmEntityList);
        if (StringUtils.isNotEmpty(genesisServiceName)){
          genesisVersionMapping.put(genesisServiceName, version);
        }
      }
    }
    log.info("Genesis Version Mapping " + genesisVersionMapping.toString());
    return genesisVersionMapping;
  }

  protected PcBackupSpecsProto.ServiceVersionSpecs getServiceVersionBackupSpecs() throws PCResilienceException {
    Map<String, String> lcmEntities = null;
    lcmEntities = buildServiceVersionMap();
    if(ObjectUtils.isEmpty(lcmEntities)) {
      return null;
    }
    PcBackupSpecsProto.ServiceVersionSpecs.Builder serviceVersionSpecs = PcBackupSpecsProto.ServiceVersionSpecs.newBuilder();
    for(Map.Entry<String, String> lcmEntity : lcmEntities.entrySet()){
      PcBackupSpecsProto.ServiceVersionSpecs.ServiceVersionSpec.Builder serviceVersionSpec =
              PcBackupSpecsProto.ServiceVersionSpecs.ServiceVersionSpec.newBuilder();
      serviceVersionSpec.setServiceName(lcmEntity.getKey());
      serviceVersionSpec.setServiceVersion(lcmEntity.getValue());
      serviceVersionSpecs.addServiceVersionSpecs(serviceVersionSpec);
    }
    return serviceVersionSpecs.build();
  }
}
