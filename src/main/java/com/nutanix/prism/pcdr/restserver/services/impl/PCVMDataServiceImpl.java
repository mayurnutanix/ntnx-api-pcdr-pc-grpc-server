package com.nutanix.prism.pcdr.restserver.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PcBackupStatus;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PcBackupStatusOnPe;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.insights.ifc.InsightsInterfaceProto.EntityGuid;
import com.nutanix.insights.ifc.InsightsInterfaceProto.GetEntitiesWithMetricsArg;
import com.nutanix.insights.ifc.InsightsInterfaceProto.GetEntitiesWithMetricsRet;
import com.nutanix.prism.adapter.service.ZeusConfiguration;
import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.ClusterExternalState;
import com.nutanix.prism.exception.EntityDbException;
import com.nutanix.prism.pcdr.PcBackupMetadataProto;
import com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMBackupTargets;
import com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMBackupTargets.BackupTarget;
import com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMBackupTargets.ObjectStoreBackupTarget;
import com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMHostingPEInfo;
import com.nutanix.prism.pcdr.PcBackupSpecsProto.PCVMFiles;
import com.nutanix.prism.pcdr.PcBackupSpecsProto.PCVMSpecs;
import com.nutanix.prism.pcdr.PcBackupSpecsProto.PCVMSpecs.NetworkConfig;
import com.nutanix.prism.pcdr.PcBackupSpecsProto.PCVMSpecs.NetworkConfig.SVMInfo;
import com.nutanix.prism.pcdr.PcBackupSpecsProto.PCVMSpecs.SystemConfig;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.factory.ObjectStoreHelperFactory;
import com.nutanix.prism.pcdr.messages.APIErrorMessages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.clients.RecoverPcProxyClient;
import com.nutanix.prism.pcdr.restserver.dto.ObjectStoreCredentialsBackupEntity;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.util.*;
import com.nutanix.prism.pcdr.services.ObjectStoreHelper;
import com.nutanix.prism.pcdr.util.*;
import com.nutanix.prism.pojos.GroupEntityResult;
import com.nutanix.prism.service.EntityDbServiceHelper;
import com.nutanix.prism.util.EntityDBUtil;
import com.nutanix.prism.util.TrustSetupUtil;
import com.nutanix.zeus.protobuf.Configuration;
import com.nutanix.zeus.protobuf.Configuration.ConfigurationProto.Node;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.extern.slf4j.Slf4j;
import nutanix.infrastructure.cluster.deployment.Deployment;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.*;
import static com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages.PC_SIZE_FETCH_FROM_ZEUS_ERROR;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;

@Slf4j
@Service
public class PCVMDataServiceImpl implements PCVMDataService {

  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;
  @Autowired
  private EntityDbServiceHelper entityDbServiceHelper;
  @Autowired
  private PCDRYamlAdapterImpl pcdrYamlAdapter;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private ApiClientGenerator apiClientGenerator;
  @Autowired
  private PCVMFileHelper pcvmFileHelper;
  @Autowired
  private HttpHelper httpHelper;
  @Autowired
  private OSUtil osUtil;
  @Autowired
  private ClusterExternalStateHelper clusterExternalStateHelper;
  @Autowired
  private GroupsUtil groupsUtil;
  @Autowired
  private RecoverPcProxyClient recoverPcProxyClient;
  @Autowired
  @Qualifier("adonisServiceThreadPool")
  private ExecutorService adonisServiceThreadPool;

  private EntityDBUtil entityDBUtil = new EntityDBUtil();
  private TrustSetupUtil trustUtil = new TrustSetupUtil(entityDBUtil);

  /**
   * Generates PCVM specs by gathering data from zeus config, os files
   * and IDF.
   *
   * @return PCVMSpecs proto object
   * @throws InsightsInterfaceException - expect EntityDbException.
   */
  public PCVMSpecs constructPCVMSpecs(String keyId)
      throws PCResilienceException {
    PCVMSpecs.Builder pcvmSpecs = PCVMSpecs.newBuilder();
    ZeusConfiguration zeusConfig = instanceServiceFactory.getZeusConfig();
    PCVMHostingPEInfo pcvmHostingPEInfo = null;
    // Set clusterUuid and clusterName
    pcvmSpecs.setClusterUuid(zeusConfig.getClusterUuid());
    pcvmSpecs.setClusterName(zeusConfig.getClusterName());


    // ZeusConfig will always have at-least one node.
    // Get that node, and from that node fetch the vmUuid and fullVersion.
    List<String> vmUuidList = zeusConfig.getAllNodes().stream()
                                        .map(Node::getUhuraUvmUuid)
                                        .collect(Collectors.toList());

    // Fetch hosting PE info from zknode to get hosting PE ip.
    try {
      pcvmHostingPEInfo = constructPCVMHostingPEInfo();
      log.info("Entering PCVMSpecs method01.");
    }
    catch (PCResilienceException e) {
      log.error(ErrorMessages.HOSTING_PE_INFO_FETCH_ERROR, e);
      throw e;
    }

    String hostingPEIp = pcvmHostingPEInfo.getExternalIp();
    log.info("Entering PCVMSpecs method0.");
    pcvmSpecs.setFullVersion(zeusConfig.getLocalNode().getSoftwareVersion());

    pcvmSpecs.setDisplayVersion(getShortPcVersion());
    // fetch pcvm specs from hosting PE (vm entity) via groups api.
    log.info("Entering PCVMSpecs method10.");

    ResponseEntity<GroupEntityResult> groupResponseData = groupsUtil.fetchPCVmSpecs(hostingPEIp, vmUuidList);
    log.info("Entering PCVMSpecs method.");
    GroupEntityResult groupEntityResult = groupResponseData.getBody();
    // Set SystemConfig and NetworkConfig in PCVMSpecs.
    pcvmSpecs.setClusterSystemConfig(constructClusterSystemConfig(groupEntityResult.getGroupResults().get(0)));

    pcvmSpecs.setClusterNetworkConfig(constructClusterNetworkConfig(
        groupEntityResult.getGroupResults().get(0),vmUuidList,hostingPEIp));
    log.info("Entering PCVMSpecs method2");
    if (ObjectUtils.isEmpty(zeusConfig.getPCClusterInfo()) ||
            ObjectUtils.isEmpty(zeusConfig.getPCClusterInfo().get().getSize())){
      throw new PCResilienceException(PC_SIZE_FETCH_FROM_ZEUS_ERROR,
              ErrorCode.PCBR_PC_SIZE_NOT_FOUND_IN_ZEUS, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    pcvmSpecs.setSize(PCUtil.getSizeFromString(zeusConfig.getPCClusterInfo().get().getSize().toString()).toString());
    log.info("Entering PCVMSpecs method3");
    // Set certificates present in /home/certs
    pcvmSpecs.setClusterCertificates(
        getClusterCertificates(keyId, Constants.ENCRYPTION_VERSION_V1));

    log.info("Successfully constructed PCVMSpecs.");
    return pcvmSpecs.build();
  }

  /**
   * Retrieves cmsp spec from cmsp spec zk node.
   *
   * @return - returns cmsp specs in proto format.
   * @throws KeeperException.NoNodeException - can throw an exception.
   *                                         mentioning that there is no CMSP node.
   * @throws ZkClientConnectException        - can throw zkclient exception.
   * @throws InterruptedException            - can throw interrupted exception.
   * @throws InvalidProtocolBufferException  - can throw invalid protocol
   *                                         buffer exception.
   */
  public Deployment.CMSPSpec fetchCMSPSpecs() throws PCResilienceException {
    log.debug("Entering cmsp specs creation method.");
    Stat stat = new Stat();
    Deployment.CMSPSpec.Builder cmspSpecBuilder =
        Deployment.CMSPSpec.newBuilder();
    try {
      byte[] cmspSpecData = zookeeperServiceHelper
          .getData(Constants.PC_CMSP_CONFIG_PATH, stat);
      String cmspSpecString = new String(cmspSpecData);
      JsonFormat.parser().ignoringUnknownFields().merge(
          cmspSpecString, cmspSpecBuilder);
    }
    catch (Exception e) {
      log.error("Error encountered while fetching CMSP specs.", e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    log.debug("CMSP specs - {}", cmspSpecBuilder);
    return cmspSpecBuilder.build();
  }

  /**
   * Get the short version of Pc.
   * Eg: pc.2021.4 is short version of fraser-6.0-stable-pc-0.
   *
   * @return - Returns a short version of Pc.
   */
  public String getShortPcVersion() throws PCResilienceException {
    ZeusConfiguration zeusConfig = instanceServiceFactory.getZeusConfig();
    Collection<Node> zeusNodes = zeusConfig.getAllNodes();
    Map<String, String> svmIpVmUuidMap = ZookeeperUtil.getSvmIpVmUuidMap(
        zeusNodes);
    List<String> ips = new ArrayList<>();
    for (Map.Entry<String, String> entry : svmIpVmUuidMap.entrySet()) {
      ips.add(entry.getValue());
    }
    log.debug(
        "PCVM IPs extracted for finding prism central shorter version are:" + ips);
    log.info(
            "PCVM IPs extracted for finding prism central shorter version are:" + ips);
    String version = "";
    // if unable to fetch version from one PC ip try on other IPs.
    for (String ip : ips) {
      try {
        log.info("0");
        HttpHeaders serviceAuthHeaders = httpHelper.getServiceAuthHeaders(
            Constants.PRISM_SERVICE, Constants.PRISM_KEY_PATH,
            Constants.PRISM_CERT_PATH);

        log.info("1");
        String completeURL = RequestUtil.makeFullUrl(ip, JGW_BASE_PATH+VERSION_API_URL,
            Constants.JGW_PORT, Collections.emptyMap(), Collections.emptyMap(), true);
        log.info("2");
        ResponseEntity<String> result = httpHelper.invokeAPI(completeURL,
            new HttpEntity<>(serviceAuthHeaders), HttpMethod.GET,
            String.class
            );
        log.info("3");
        if (result.getStatusCode().is2xxSuccessful()) {
          HttpHeaders httpHeaders = result.getHeaders();
          log.debug(String.format(
              "Http headers from version api call on ip:%s are:%s", ip,
              httpHeaders));
          List<String> versionList = httpHeaders.getValuesAsList(
              Constants.RESPONSE_VERSION_HEADER);
          if (!versionList.isEmpty()) {
            version = versionList.get(0);
            log.info("Fetched Short version of PC is:" + version);
            log.debug("Fetched Short version of PC is:" + version);
            return version;
          }
          log.debug(
              "Returning Master as Short version of PC as version header" +
              " is not present:" + version);
          return Constants.MASTER_VERSION;
        }
      }
      catch (Exception e) {
        log.warn("Exception occurred while fetching display version:" + ip);
        log.debug("Exception occurred while fetching display version is:" + e);
      }
    }
    String versionErrorString =
        "Unable to find the display version of Prism Central." +
        " Aborting backup of Prism Central specs.";
    log.error(versionErrorString);
    throw new PCResilienceException(ErrorMessages.DOMAIN_MANAGER_DISPLAY_VERSION_UNAVAILABLE_ERROR_MESSAGE,
            ErrorCode.PCBR_FETCH_DISPLAY_VERSION_PRISM_CENTRAL_FAILURE,HttpStatus.INTERNAL_SERVER_ERROR);
  }

  /**
   * Get the certificates/files which are required to be passed to genesis
   * at the time of restore.
   *
   * @return - Returns a PCVMFiles object containing the given files
   * mentioned in pc_dr.yaml.
   */
  public PCVMFiles getClusterCertificates(String keyId,
                                          String encryptionVersion) {
    log.debug("Entering clusterCertificates function. Retrieving cluster " +
              "certificates data.");
    PCDRYamlAdapterImpl.FileSystemNodes filesystemNodes =
        pcdrYamlAdapter.getFileSystemNodes();
    List<PCDRYamlAdapterImpl.PathNode> pathNodes =
        filesystemNodes.getPcvmSpecsNodes();
    return pcvmFileHelper.constructPCVMFilesProto(pathNodes, keyId,
                                                  encryptionVersion);
  }

  /**
   * Generates SystemConfig from groups api response from PE from vm and
   * virtual_disk table. The disk data is fetched from zeus and os file
   * /sys/block/<disk-name>/size
   *
   * @param vmGroupResult - Groups api result of vm api.
   * @return - SystemConfig object of PCVMSpecs proto.
   * @throws PCResilienceException - Can throw an insights interface
   *                           exception if data_disk_bytes are not fetched.
   */
  @Override
  public SystemConfig constructClusterSystemConfig(final GroupEntityResult.GroupResult vmGroupResult)
      throws PCResilienceException {

    SystemConfig.Builder systemConfigBuilder = SystemConfig.newBuilder();
    try {
      GroupEntityResult.GroupResult.EntityResult entityResult = vmGroupResult.getEntityResults().get(0);
      List<GroupEntityResult.GroupResult.EntityResult.MetricData> dataList = entityResult.getData();
      systemConfigBuilder.setNumSockets(
          Long.parseLong(Objects.requireNonNull(groupsUtil.getParameterValueFromGroups(NUM_VCPUS, dataList),
                                                String.format(IDF_NOT_FOUND, VM_ATTRIBUTE)).get(0)));
      systemConfigBuilder.setMemorySizeBytes(
          Long.parseLong(Objects.requireNonNull(groupsUtil.getParameterValueFromGroups(MEMORY_SIZE_BYTES, dataList),
                                                String.format(IDF_NOT_FOUND, MEMORY_SIZE_BYTES)).get(0)));
      systemConfigBuilder.addAllContainerUuids(groupsUtil.getParameterValueFromGroups(CONTAINER_UUIDS, dataList));
      systemConfigBuilder.setDataDiskSizeBytes(fetchDataDiskSizeBytes());
    }
    catch (Exception e) {
      log.debug(APIErrorMessages.DEBUG_START, e);
      log.error("Error encountered while storing info for ClusterSystemConfig" +
                ". It seems like some values are not found.");
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    return systemConfigBuilder.build();
  }

  /**
   * Creates a NetworkConfig object from zeusConfig and groups virtual nic response
   * from hosting PE.
   *
   * @param vmGroupResult  vm entity group response of hosting PE
   * @param vmUuidList  pc uhura vm uuid list
   * @param ip  hosting PE ip
   * @return  returns a NetworkConfig object.
   */
  @Override
  public NetworkConfig constructClusterNetworkConfig(final GroupEntityResult.GroupResult vmGroupResult,
      final List<String> vmUuidList , String ip)
      throws PCResilienceException {
    ZeusConfiguration zeusConfig = instanceServiceFactory.getZeusConfig();
    // Get the configuration.proto (zeusConfig proto object).
    Configuration.ConfigurationProto config = zeusConfig.getConfiguration();

    NetworkConfig.Builder networkConfig = NetworkConfig.newBuilder();
    networkConfig.setGatewayIp(config.getDefaultGatewayIp());

    String externalSubnet = config.getExternalSubnet();
    // The external subnet is in the format "subnetIP/subnetMask" so getting
    // the subnet mask.
    String subnetMask = externalSubnet.split("/", 2)[1];
    networkConfig.setSubnetMask(subnetMask);
    networkConfig.setExternalIp(config.getClusterExternalIp());
    networkConfig.addAllDns(config.getNameServerIpListList());
    networkConfig.addAllNtp(config.getNtpServerListList());
    Map<String, String> svmIpVmUuidMap =
        ZookeeperUtil.getSvmIpVmUuidMap(zeusConfig.getAllNodes());
    // fetch NetworkUUID map info via Groups api from hosting PE.
    Map<String, String> networkUuidMap = fetchNetworkUuidVmUuidMapFromGroups(vmUuidList,ip);

    List<SVMInfo> svmInfoList = new ArrayList<>();
    List<GroupEntityResult.GroupResult.EntityResult> entityResultList =  vmGroupResult.getEntityResults();
    for(GroupEntityResult.GroupResult.EntityResult entityResult : entityResultList){
      List<GroupEntityResult.GroupResult.EntityResult.MetricData> dataList = entityResult.getData();
      SVMInfo.Builder svmInfoBuilder = SVMInfo.newBuilder();
      svmInfoBuilder.setIp(svmIpVmUuidMap.get(entityResult.getUuid()));
      String vmName = Optional.ofNullable(groupsUtil.getParameterValueFromGroups(VM_NAME, dataList))
                              .filter(l -> !l.isEmpty()).map(l -> l.get(0)).orElse("");
      svmInfoBuilder.setVmName(vmName);
      svmInfoBuilder.setNetworkUuid(networkUuidMap.get(entityResult.getUuid()));
      svmInfoList.add(svmInfoBuilder.build());
    }
    networkConfig.addAllSvmInfo(svmInfoList);
    return networkConfig.build();
  }


  /**
   * Creates a map of vm uuid and network uuid from virtual_nic table of
   * hosting PE for current PC nodes.
   *
   * @param vmUuidList  pc uhura vm uuid list
   * @param ip  hosting PE ip
   * @return  returns a Map<vmid,networkId>.
   */
  public Map<String, String> fetchNetworkUuidVmUuidMapFromGroups(
      final List<String> vmUuidList, String ip) throws PCResilienceException {
    Map<String, String> networkUuidMap = new HashMap<>();
    ResponseEntity<GroupEntityResult> groupResponseData = groupsUtil.fetchVirtualNetworkSpecsFromHostingPE(ip,vmUuidList);
    GroupEntityResult groupEntityResult = groupResponseData.getBody();
    GroupEntityResult.GroupResult groupResult = groupEntityResult.getGroupResults().get(0);
    List<GroupEntityResult.GroupResult.EntityResult> entityResultList =  groupResult.getEntityResults();

    //todo: below try catch block can be removed , check and do so later
    try {
      for (GroupEntityResult.GroupResult.EntityResult entityResult : entityResultList) {
        List<GroupEntityResult.GroupResult.EntityResult.MetricData> dataList = entityResult.getData();
        String vmUuid = Objects.requireNonNull(groupsUtil.getParameterValueFromGroups(VM_ATTRIBUTE, dataList),
                                               String.format(IDF_NOT_FOUND,VM_ATTRIBUTE)).get(0);
        String virtualNetworkUuid = Objects.requireNonNull(
            groupsUtil.getParameterValueFromGroups(VIRTUAL_NETWORK, dataList),
            String.format(IDF_NOT_FOUND,VM_ATTRIBUTE)).get(0);
        networkUuidMap.put(vmUuid, virtualNetworkUuid);
      }
    } catch (Exception e) {
      log.error(e.getMessage());
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    return networkUuidMap;
  }

  public boolean isHostingPEHyperV(String hostingPeUuid)
      throws PCResilienceException {
    try {
      GetEntitiesWithMetricsRet result;
      final InsightsInterfaceProto.Query.Builder query =
          InsightsInterfaceProto.Query.newBuilder();
      query.setQueryName("prism:cluster");
      query.addEntityList(EntityGuid.newBuilder()
                                    .setEntityTypeName(Constants.CLUSTER)
                                    .setEntityId(hostingPeUuid)
                                    .build());
      List<String> hypervisorValueList = Collections.singletonList(
          Configuration.ConfigurationProto.ManagementServer.HypervisorType.kHyperv.name());
      final InsightsInterfaceProto.BooleanExpression clusterHyperV = IDFUtil
          .constructBooleanExpression(
              IDFUtil.getColumnExpression(Constants.HYPERVISOR_TYPES),
              IDFUtil.getValueStringListExpression(hypervisorValueList),
              InsightsInterfaceProto.ComparisonExpression.Operator.kContains
                                     );
      query.setWhereClause(clusterHyperV);
      result = entityDBProxy.getEntitiesWithMetrics(
          GetEntitiesWithMetricsArg.newBuilder().setQuery(query).build());
      return !result.getGroupResultsListList().isEmpty();
    }
    catch (InsightsInterfaceException ex) {
      log.error(
          "Exception occurred while fetching clusters from cluster table", ex);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_VERIFY_HOSTING_CLUSTER_HYPERV_FAILURE,HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Retrieves the data disk size in bytes from centos using mount path in
   * zeus config for cassandra disk.
   *
   * @return - returns a Long value containing data_disk_size_bytes.
   */
  public Long fetchDataDiskSizeBytes() throws PCResilienceException {

    ZeusConfiguration zeusConfig = instanceServiceFactory.getZeusConfig();
    String cassandraMountPath = ZookeeperUtil.getCassandraMountPath(zeusConfig);
    if (cassandraMountPath.isEmpty()) {
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    long diskSize;
    try {
      diskSize = osUtil.getDiskSizeFromMountPath(cassandraMountPath);
    }
    catch (IOException e) {
      log.error("Querying Virtual disk size from os failed.", e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    if (diskSize == -1) {
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    return diskSize;
  }

  /**
   * Retrieves the hosting PE info from the clusterexternalstate/trustZkNode.
   *
   * @return - PCVMHostingPEInfo object.
   */
  public PCVMHostingPEInfo constructPCVMHostingPEInfo() throws PCResilienceException {
    String hostingPEUuid = getHostingPEUuid();

    if (StringUtils.isEmpty(hostingPEUuid)) {
      // If hosting PE is not found return null so that the we can skip the
      // update of PCVMHostingPEInfo data in IDF.
      throw new PCResilienceException(ErrorMessages.HOSTING_PE_NOT_CONNECTED_ERROR,
              ErrorCode.PCBR_HOSTING_CLUSTER_NOT_CONNECTED,HttpStatus.INTERNAL_SERVER_ERROR);
    }

    PCVMHostingPEInfo.Builder pcvmHostingPEInfoBuilder =
        PCVMHostingPEInfo.newBuilder();
    String zkPath = "";

    try {
      Stat stat = this.zookeeperServiceHelper.exists(TRUST_ZK_NODE, false);
      if (stat == null) {
        log.debug("Trust is not enabled on this PC, fetch from clusterExternal State");
        // Fallback to fetch the hosting PE information from PC's Cluster external state
        zkPath = CLUSTEREXTERNALSTATE_PATH ;
      } else {
        zkPath = TRUST_ZK_NODE ;
      }
    } catch (final ZkClientConnectException e) {
      final String errorMessage =
          "Could not fetch information about Hosting Prism Element cluster";
      log.error(errorMessage, e);
      throw new PCResilienceException(ErrorMessages.FETCH_HOSTING_CLUSTER_DETAILS_ERROR,
              ErrorCode.PCBR_FETCH_HOSTING_CLUSTER_DETAILS_FAILURE, HttpStatus.INTERNAL_SERVER_ERROR);
    } catch (InterruptedException e) {
      log.warn("Interrupted!", e);
      Thread.currentThread().interrupt();
      throw new PCResilienceException(ErrorMessages.SERVER_INTERRUPT_ERROR,
              ErrorCode.PCBR_SERVER_INTERRUPTED, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    ClusterExternalState clusterExternalState = clusterExternalStateHelper.getClusterExternalState(hostingPEUuid, zkPath);
    pcvmHostingPEInfoBuilder.setClusterUuid(hostingPEUuid);
    pcvmHostingPEInfoBuilder.setName(
        clusterExternalState
            .getClusterDetails().getClusterName());
    pcvmHostingPEInfoBuilder.setExternalIp(
        clusterExternalState
            .getConfigDetails().getExternalIp());
    pcvmHostingPEInfoBuilder.addAllIps(
        clusterExternalState
            .getClusterDetails().getContactInfo().getAddressListList());

    return pcvmHostingPEInfoBuilder.build();
  }

  /**
   * Creates PCVMBackupTargets on the basis of replica PE's clusterUuid.
   * Contains - external_ip (virtual_ip), cluster_uuid, cluster_name, PE node
   * IP's
   *
   * @param clusterUuids - uuids of the replica PE's
   * @return - returns a PCVMBackupTargets object containing metadata
   * regarding replica PE's
   */
  public PCVMBackupTargets constructPCVMBackupTargets(
      final List<String> clusterUuids,
      List<PcObjectStoreEndpoint> objectStoreEndpointList,
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
      boolean updateSyncTime)
      throws PCResilienceException {
    log.info("Received pe cluster uuids - {}", clusterUuids);
    String objectStoreEndpoints = objectStoreEndpointList.toString();
    objectStoreEndpoints = DataMaskUtil.maskSecrets(objectStoreEndpoints);

    log.info("Received object store endpoints - {}", objectStoreEndpoints);
    PCVMBackupTargets.Builder pcvmBackupTargetsBuilder =
        PCVMBackupTargets.newBuilder();
    final String pcClusterUuid = instanceServiceFactory
        .getClusterUuidFromZeusConfig();
    log.debug("Reading previous pcvm backup target from pc_backup_metadata " +
              "table.");
    PCVMBackupTargets previousPCVMBackupTargets = fetchPcvmBackupTargets();
    Map<String, Long> prevObjectStoreSyncMap = new HashMap<>();
    Map<String, Long> prevClusterUuidSyncMap = new HashMap<>();
    if (previousPCVMBackupTargets != null) {
      prevObjectStoreSyncMap = convertObjectStoreToSyncMap(
          previousPCVMBackupTargets.getObjectStoreBackupTargetsList(),
          pcClusterUuid);
      prevClusterUuidSyncMap = convertPeBackupTargetToSyncMap(
          previousPCVMBackupTargets.getBackupTargetsList());
    }
    List<BackupTarget.Builder> backupTargetBuilders = new ArrayList<>();
    if(CollectionUtils.isNotEmpty(clusterUuids)) {
      // Add backup target list to pcvmBackupTarget proto.
      // After we have received the
      backupTargetBuilders =
          createPEBackupTargets(clusterUuids, updateSyncTime,
              pcClusterUuid, prevClusterUuidSyncMap);
      pcvmBackupTargetsBuilder.addAllBackupTargets(
          backupTargetBuilders.stream()
              .map(BackupTarget.Builder::build)
              .collect(Collectors.toList()));
    }
    List<ObjectStoreBackupTarget.Builder> objectStoreBackupTargetBuilders =
        createObjectstoreEndpointBackupTargets(
            objectStoreEndpointList, objectStoreEndPointDtoMap, updateSyncTime,
            pcClusterUuid, prevObjectStoreSyncMap);

    pcvmBackupTargetsBuilder.addAllObjectStoreBackupTargets(
        objectStoreBackupTargetBuilders
            .stream().map(ObjectStoreBackupTarget.Builder::build)
            .collect(Collectors.toList()));
    log.debug("Generated pcvmBackupTargets: {}",
              pcvmBackupTargetsBuilder.toString());
    return pcvmBackupTargetsBuilder.build();
  }

  /**
   * Update the object store endpoint builder such that pc_backup_metadata
   * contains all the latest data present in pc_backup_config.
   *
   * @param objectStoreEndpointList - list of object store endpoints.
   * @param updateSyncTime - whether it's required to update the sync time or
   *                      not.
   * @param pcClusterUuid - Uuid of the PC
   * @param prevObjectStoreSyncMap - previous object store endpoint sync map.
   * @return - return a list of object store endpoint backup targets
   * @throws PCResilienceException - can throw pc backup exception.
   */
  private List<ObjectStoreBackupTarget.Builder>
  createObjectstoreEndpointBackupTargets(
      List<PcObjectStoreEndpoint> objectStoreEndpointList,
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
      boolean updateSyncTime, String pcClusterUuid,
      Map<String, Long> prevObjectStoreSyncMap) throws PCResilienceException {
    List<ObjectStoreBackupTarget.Builder> objectStoreBackupTargetBuilders =
        new ArrayList<>();
    Map<String, Long> objectStoreSyncTimeMap = new HashMap<>();
    if (updateSyncTime) {
      objectStoreSyncTimeMap = getObjectStoreTimestampMapFromIDF();
    }
    // If object store endpoint list is empty the loop will not populate the
    // data. So no need for size check in case of object store.
    for (PcObjectStoreEndpoint objectStoreEndpoint : objectStoreEndpointList) {
      ObjectStoreEndPointDto objectStoreEndPointDto = objectStoreEndPointDtoMap.get(objectStoreEndpoint.getEndpointAddress());
      final ObjectStoreBackupTarget.Builder objectStoreBackupTargetBuilder =
          ObjectStoreBackupTarget.newBuilder();
      objectStoreBackupTargetBuilder.setEndpointAddress(
          objectStoreEndpoint.getEndpointAddress());
      objectStoreBackupTargetBuilder.setEndpointFlavour(
          ObjectStoreBackupTarget.EndpointFlavour.forNumber(
              objectStoreEndpoint.getEndpointFlavour().ordinal()));
      objectStoreBackupTargetBuilder.setRpoSecs(objectStoreEndpoint.getRpoSeconds());
      objectStoreBackupTargetBuilder.setBackupRetentionDays(objectStoreEndpoint.getBackupRetentionDays());
      objectStoreBackupTargetBuilder.setEntityUuid(PCUtil.getObjectStoreEndpointUuid(pcClusterUuid,objectStoreEndpoint.getEndpointAddress()));
      objectStoreBackupTargetBuilder.setBucketName(objectStoreEndpoint.getBucket());
      objectStoreBackupTargetBuilder.setRegion(objectStoreEndpoint.getRegion());
      objectStoreBackupTargetBuilder.setPathStyleEnabled(objectStoreEndPointDto.isPathStyle());
      objectStoreBackupTargetBuilder.setSkipCertificateValidation(objectStoreEndPointDto.isSkipCertificateValidation());
      if (!ObjectUtils.isEmpty(objectStoreEndPointDto.getCertificatePath())) {
        objectStoreBackupTargetBuilder.setCertificatePath(objectStoreEndPointDto.getCertificatePath());
        objectStoreBackupTargetBuilder.setCertificateContent(ByteString.copyFrom(Base64.getEncoder().encode(
            objectStoreEndPointDto.getCertificateContent().getBytes()
        )));
      }
      // In case the method is executed through API objectstoresynctimemap
      // will be empty hashmap. So, the values are going to be set to
      // previous objectstore values present in metadata.
      updateObjectStoreBackupTarget(objectStoreBackupTargetBuilder,
                                    pcClusterUuid,
                                    objectStoreSyncTimeMap,
                                    prevObjectStoreSyncMap);
      objectStoreBackupTargetBuilders.add(objectStoreBackupTargetBuilder);
    }
    return objectStoreBackupTargetBuilders;
  }

  /**
   * This method is responsible for creating PE backup targets
   * @param clusterUuids - list of cluster uuids required to be added as part
   *                    of pc_backup_metadata
   * @param updateSyncTime - whether it's required to update the sync time or
   *                      not.
   * @param pcClusterUuid - pc cluster uuid
   * @param prevClusterUuidSyncMap - previous backup sync timestamp and pe
   *                               cluster uuid map.
   * @return - list of pe backup targets to be added as part of
   * pc_backup_metadata.
   */
  private List<BackupTarget.Builder> createPEBackupTargets(
      List<String> clusterUuids, boolean updateSyncTime,
      String pcClusterUuid, Map<String, Long> prevClusterUuidSyncMap) {
    List<BackupTarget.Builder> backupTargetBuilders = new ArrayList<>();
    if (!clusterUuids.isEmpty()) {
      final Map<String, Long> finalPrevClusterUuidSyncMap =
          new HashMap<>(prevClusterUuidSyncMap);

      List<CompletableFuture<Boolean>> allFutures = new ArrayList<>();
      for (final String clusterUuid : clusterUuids) {
        final BackupTarget.Builder backupTargetBuilder =
            BackupTarget.newBuilder();
        // Create a separate list containing external Ip as the first entry,
        // and rest of the PE SVM IP's entries after that, for fetching
        // lastSyncTime.
        final List<String> ipList = new ArrayList<>();
        // Set data in backupTarget.
        backupTargetBuilder.setClusterUuid(clusterUuid);
        backupTargetBuilder.setEntityUuid(clusterUuid);
        ClusterExternalState clusterExternalState;
        try {
          clusterExternalState = clusterExternalStateHelper.getClusterExternalState(clusterUuid);
        }
        catch (PCResilienceException ex) {
          log.error(
              String.format("Exception occurred while fetching cluster" +
                            " external state for cluster with uuid:%s and error",
                            clusterUuid)
              , ex);
          continue;
        }
        // Setting cluster name.
        backupTargetBuilder.setName(clusterExternalState
                                        .getClusterDetails()
                                        .getClusterName());
        String externalIp =
            clusterExternalState.getConfigDetails().getExternalIp();
        if (!externalIp.isEmpty()) {
          // Setting external IP
          backupTargetBuilder.setExternalIp(externalIp);
          ipList.add(externalIp);
        }
        List<String> peSvmIps = clusterExternalState
            .getClusterDetails().getContactInfo().getAddressListList();
        // Setting all the PE IP's
        backupTargetBuilder.addAllIps(peSvmIps);
        ipList.addAll(peSvmIps);

        if (updateSyncTime) {
          // For every PE cluster uuid as part of replica we create a
          // completable future object. For every cluster as part of
          // fetchLastSyncTimestampFromPE, we try to fetch the
          // lastsynctimestamp synchronously trying on every PE IP, and
          // stopping as soon as we receive the response.
          // Setting lastSyncTimestamp Asynchronously.
          allFutures.add(
              CompletableFuture
                  .supplyAsync(() ->
                                   fetchLastSyncTimeStampFromPE(
                                       pcClusterUuid, ipList), adonisServiceThreadPool)
                  .thenApply(lastSyncTimestamp ->
                                 updatePeBackupTarget(backupTargetBuilder,
                                                      finalPrevClusterUuidSyncMap,
                                                      lastSyncTimestamp,
                                                      clusterUuid)));
          log.info("Updating last sync timestamp for pe cluster uuid {}",
                   clusterUuid);
        }
        else {
          updatePeBackupTarget(backupTargetBuilder,
                               prevClusterUuidSyncMap,
                               null,
                               clusterUuid);
          log.info("Keeping the old last sync timestamp for pe cluster uuid" +
                   " {}", clusterUuid);
        }
        // Add backupTarget to list of backup targets.
        // As we are adding the reference to the list updating
        // lastSyncTimestamp should get reflected in backupTargetBuilder.
        backupTargetBuilders.add(backupTargetBuilder);
      }
      if (updateSyncTime) {
        // Update lastSyncTimestamp in async fashion.
        CompletableFuture.allOf(allFutures.toArray(
            new CompletableFuture[0])).join();
      }
    }
    return backupTargetBuilders;
  }

  /**
   * Retrieves the Domain Manager Identifier from ZeusConfig.
   *
   * @return - DomainManagerIdentifier object.
   */
  public PcBackupMetadataProto.DomainManagerIdentifier constructDomainManagerIdentifier() {
    PcBackupMetadataProto.DomainManagerIdentifier.Builder domainManagerIdentifierBuilder = PcBackupMetadataProto.DomainManagerIdentifier.newBuilder();

    final ZeusConfiguration zeusConfig = instanceServiceFactory.getZeusConfig();

    domainManagerIdentifierBuilder.setName(zeusConfig.getClusterName());
    domainManagerIdentifierBuilder.setUuid(zeusConfig.getClusterUuid());
    domainManagerIdentifierBuilder.addAllClusterIps(new ArrayList<String>(
        zeusConfig.getAllNodes().stream().map(Node::getServiceVmExternalIp)
                .collect(Collectors.toList()))
    );
    domainManagerIdentifierBuilder.setFqdn(zeusConfig.getClusterFullyQualifiedDomainName());
    domainManagerIdentifierBuilder.setVirtualIp(zeusConfig.getClusterExternalIp());
    return domainManagerIdentifierBuilder.build();
  }

  public PCVMBackupTargets fetchPcvmBackupTargets() throws PCResilienceException {
    // Adding it as default to avoid null entry.
    PCVMBackupTargets previousPCVMBackupTargets =
        PCVMBackupTargets.getDefaultInstance();
    try {
      previousPCVMBackupTargets = PcdrProtoUtil.fetchBackupTargetFromPcBackupMetadata(
          instanceServiceFactory.getClusterUuidFromZeusConfig(),
          entityDBProxy);
      log.debug("Fetched previous Backup Targets from pc_backup_metadata. {}",
                previousPCVMBackupTargets);
    } catch (Exception e) {
      log.error("Unable to fetch pc_backup_metadata from IDF due to exception : ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }
    return previousPCVMBackupTargets;
  }

  /**
   * This method is responsible for fetching the last sync time from IDF
   * database and creating a map containing entityId and lastSyncTime.
   *
   * @return Returns a map containing <entityId (String), lastSyncTime (Long)>
   * @throws PCResilienceException - can throw internal server exception in case
   *                           IDF is not available.
   */
  private Map<String, Long> getObjectStoreTimestampMapFromIDF()
      throws PCResilienceException {
    Map<String, Long> objectStoreSyncTimestampMap = new HashMap<>();
    try {
      GetEntitiesWithMetricsRet ret =
          entityDBProxy.getEntitiesWithMetrics(
              QueryUtil.createGetObjectStoreEntitiesTimestamp());
      for (InsightsInterfaceProto.QueryGroupResult queryGroupResult:
          ret.getGroupResultsListList()) {
        for (InsightsInterfaceProto.EntityWithMetric entityWithMetric:
            queryGroupResult.getRawResultsList()) {
          String entityId = entityWithMetric.getEntityGuid().getEntityId();
          InsightsInterfaceProto.DataValue dataValue =
              IDFUtil.getAttributeValueInEntityMetric(
              OBJECT_STORE_SYNC_COMPLETED_ATTRIBUTE, entityWithMetric);
          if(dataValue != null) {
            Long timestamp = dataValue.getInt64Value();
            objectStoreSyncTimestampMap.put(entityId, timestamp);
          }
        }
      }
    }
    catch (InsightsInterfaceException e) {
      String errorMessage = String.format(
          "Insights error encountered while fetching entities from %s.",
          OBJECT_STORE_SYNC_MARKER_TABLE);
      log.error(errorMessage, e);
      throw new PCResilienceException(ErrorMessages.getInsightsServerReadErrorMessage("object store sync data"),
              ErrorCode.PCBR_DATABASE_READ_OBJECTSTORE_SYNC_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return objectStoreSyncTimestampMap;
  }

  /**
   * Convert the PE backup targets into clusterUuid and last sync timestamp map.
   *
   * @param backupTargets - list of PE backup targets added
   * @return - returns the map containing pe cluster uuid as the key and
   * timestamp as the value.
   */
  private Map<String, Long> convertPeBackupTargetToSyncMap(
      List<BackupTarget> backupTargets) {
    Map<String, Long> clusterUuidTimestampMap = new HashMap<>();
    backupTargets.forEach(backupTarget -> {
      if (backupTarget.hasLastSyncTimestamp()) {
        String clusterUuid = backupTarget.getClusterUuid();
        Long lastSyncTimestamp = backupTarget.getLastSyncTimestamp();
        clusterUuidTimestampMap.put(clusterUuid, lastSyncTimestamp);
      }
    });
    return clusterUuidTimestampMap;
  }

  /**
   * This method is responsible for converting the ObjectStoreBackupTargets to
   * a simple map containing entityId as the key and lastSyncTimestamp as the
   * value.
   *
   * @param objectStoreBackupTargets - List of ObjectStoreBackupTarget proto
   * @return - a map containing <entityId (String), lastSyncTimestamp (Long)>
   */
  Map<String, Long> convertObjectStoreToSyncMap(
      List<ObjectStoreBackupTarget> objectStoreBackupTargets,
      String pcClusterUuid) {
    Map<String, Long> objectStoreEntityTimestampMap = new HashMap<>();
    objectStoreBackupTargets.forEach(objectStoreBackupTarget -> {
      if (objectStoreBackupTarget.hasLastSyncTimestamp()) {
        String entityId = PCUtil.getObjectStoreEndpointUuid(
            pcClusterUuid,
            objectStoreBackupTarget.getEndpointAddress());
        Long lastSyncTimestamp = objectStoreBackupTarget.getLastSyncTimestamp();
        objectStoreEntityTimestampMap.put(entityId, lastSyncTimestamp);
      }
    });
    return objectStoreEntityTimestampMap;
  }

  /**
   * This method is responsible for updating the objectStoreBackupTargetBuilder
   * with the updated timestamp from object store sync timestamp map
   *
   * @param objectStoreBackupTargetBuilder - object store backup target which
   *                                       is required to be updated.
   * @param objectStoreSyncTimestampMap    - object store timestamp map through
   *                                       which timestamp will be updated.
   */
  void updateObjectStoreBackupTarget(
      ObjectStoreBackupTarget.Builder objectStoreBackupTargetBuilder,
      String pcClusterUuid,
      Map<String, Long> objectStoreSyncTimestampMap,
      Map<String, Long> prevObjectStoreSyncTimestampMap) {
    String entityId =
        PCUtil.getObjectStoreEndpointUuid(
            pcClusterUuid, objectStoreBackupTargetBuilder.getEndpointAddress());
    if (objectStoreSyncTimestampMap.containsKey(entityId)) {
      Long timestamp = objectStoreSyncTimestampMap.get(entityId);
      log.info("Setting lastsynctimestamp {} for object store from database " +
               "value for entityId {}", timestamp, entityId);
      objectStoreBackupTargetBuilder
          .setLastSyncTimestamp(timestamp);
    } else if (prevObjectStoreSyncTimestampMap.containsKey(entityId)) {
      Long timestamp = prevObjectStoreSyncTimestampMap.get(entityId);
      log.info("Setting lastsynctimestamp {} for object store from previous " +
               "metadata value for entityId {}", timestamp, entityId);
      objectStoreBackupTargetBuilder
          .setLastSyncTimestamp(timestamp);
    }
  }

  /**
   * Updates the PE backup target lastSyncTimestamp value to the latest
   * timestamp, else keep it same as the previous value.
   *
   * @param backupTargetBuilder         - pe backup target builder which is required
   *                                    to be updated.
   * @param prevClusterUuidTimestampMap - Map of clusteruuid and previous
   *                                    timestamp.
   * @param lastSyncTimestamp           - current lastsynctimestamp (can be null).
   * @param clusterUuid                 - PE cluster uuid for which we need to update the
   *                                    timestamp.
   * @return - returns true if successful.
   */
  private boolean updatePeBackupTarget(BackupTarget.Builder backupTargetBuilder,
                                       Map<String, Long> prevClusterUuidTimestampMap,
                                       Long lastSyncTimestamp,
                                       String clusterUuid) {
    log.debug("Fetched lastSyncTimestamp from PE with value {}",
              lastSyncTimestamp);
    if (lastSyncTimestamp == null &&
        !prevClusterUuidTimestampMap.isEmpty()) {
      // If lastSyncTimestamp is null then set the value of timestamp from
      // the previous backup target entry.
      if (prevClusterUuidTimestampMap.containsKey(clusterUuid)) {
        backupTargetBuilder.setLastSyncTimestamp(
            prevClusterUuidTimestampMap.get(clusterUuid));
      }
    }
    else if (lastSyncTimestamp != null) {
      backupTargetBuilder.setLastSyncTimestamp(lastSyncTimestamp);
    }
    return true;
  }

  /**
   * Fetch the last sync time stamp from the PE, first using sdk,
   * then if PE doesn't have v4 apis and response is 404 then try v2 apis using
   * rest template
   *
   * @param clusterUuid - PC cluster uuid
   * @param ipList      - list of PE IP's
   * @return timestamp
   */
  @VisibleForTesting
  public Long fetchLastSyncTimeStampFromPE(final String clusterUuid,
                                           final List<String> ipList) {
    PcBackupStatusOnPe pcBackupStatusOnPe = null;
    log.debug("The PE ip list is: {}", ipList);
    for (String ip : ipList) {
      try {
        pcBackupStatusOnPe = recoverPcProxyClient.pcBackupStatusAPICallOnPE(ip);
      }
      catch (HttpClientErrorException e) {
        log.error("Following error encountered while making an API call for " +
            "fetching timestamp from ip {}.", ip, e);
        return null;
      }
      catch (RestClientException e) {
        log.error("Following error encountered while making an API call for " +
                  "fetching timestamp from ip {}. Trying with the next one.",
                  ip, e);
      }
      catch (PCResilienceException e) {
        log.error("Unable to make api call due to " + e.getMessage());
      }
      catch (JsonProcessingException e) {
        log.error("Following error encountered while parsing data between V2 and V4 models ", e);
      }
      if (pcBackupStatusOnPe != null) {
        log.debug("Successfully fetched lastSyncTime from PE.");
        break;
      }
    }

    if (pcBackupStatusOnPe == null) {
      log.info("Unable to make API request to the IP list provided. Request " +
               "failed. List: {}", ipList);
      return null;
    }

    if (pcBackupStatusOnPe.getPcBackupStatusList() == null) {
      return null;
    }

    for (PcBackupStatus pcBackupStatus :
        pcBackupStatusOnPe.getPcBackupStatusList()) {
      if (clusterUuid.equals(pcBackupStatus.getPcClusterUuid().toString())) {
        return pcBackupStatus.getLastBackupTime();
      }
    }
    return null;
  }

  /**
   * Retrieve the files marked for backup and store it in a PCVMFiles proto.
   *
   * @return - returns PCVMFiles proto object containing backed up files.
   */
  public PCVMFiles constructPCVMGeneralFiles(String keyId,
                                             String encryptionVersion) {
    return pcvmFileHelper.constructPCVMFilesProto(
        pcdrYamlAdapter
            .getFileSystemNodes()
            .getGeneralNodes(), keyId, encryptionVersion);
  }

  /**
   * Fetch registered Prism Element Clusters Uuids from Cluster External State
   *
   * @return - returns the list of registered Prism Element Clusters
   * @throws PCResilienceException - can throw resilience exception.
   */
  public List<String> getPeUuidsOnPCClusterExternalState() throws PCResilienceException {

    List<String> registeredClusters;
    try {
      registeredClusters = zookeeperServiceHelper.getChildren(CLUSTEREXTERNALSTATE_PATH);
      return registeredClusters;
    }
    catch (ZkClientConnectException | KeeperException.NoNodeException e) {
      log.error("Error while fetching the children from zkClient",e);
      throw new PCResilienceException(ErrorMessages.FETCH_REGISTERED_CLUSTER_DETAILS_ERROR,
              ErrorCode.PCBR_FETCH_REGISTERED_CLUSTERS_FAILURE, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    catch (InterruptedException e) {
      log.warn("Interrupted!", e);
      Thread.currentThread().interrupt();
      throw new PCResilienceException(ErrorMessages.SERVER_INTERRUPT_ERROR,
              ErrorCode.PCBR_SERVER_INTERRUPTED, HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Fetch hosting cluster uuid from idf VM entities using ip addresses as query
   *
   * @return - returns the hosting Prism Element Cluster if found, null
   * otherwise.
   */
  public String fetchHostingPeFromIdf() throws EntityDbException {
    // Build address list to be queried in Insights.
    final Collection<Node> pcvmNodes =
        instanceServiceFactory.getZeusConfig().getAllNodes();
    final List<String> pcvmIpAddresses = Lists.newArrayList();
    for (Node node : pcvmNodes) {
      pcvmIpAddresses.add(node.getServiceVmExternalIp());
    }

    // append the vip as well in IP address list.
    final String clusterExternalIp =
        instanceServiceFactory.getZeusConfig().getClusterExternalIp();
    if (!StringUtils.isEmpty(clusterExternalIp)) {
      pcvmIpAddresses.add(clusterExternalIp);
    }

    try {
      final String hostingClusterUuid =
          trustUtil.getVmClusterByIpList(pcvmIpAddresses);
      return hostingClusterUuid;
    } catch (final EntityDbException ex) {
      final String errorMessage =
          "Failed to fetch hosting cluster uuid from idf ";
      log.error(errorMessage, ex);
      throw ex;
    }
  }

  /**
   * Fetch hosting Prism Element Cluster Uuid, there are multiple scenarios -
   * 1. if PC is having trust with it's hosting PE, then fetch it from trust
   * zk node.
   * 2. else fallback to fetching this information from PC's local IDF.
   *
   * @return - returns the hosting PE cluster id if found, empty
   * string otherwise.
   */
  public String getHostingPEUuid() throws PCResilienceException {
    try {
      Stat stat = this.zookeeperServiceHelper.exists(TRUST_ZK_NODE, false);
      if (stat == null) {
        log.debug("Trust is not enabled on this PC, fetch from idf");
        // Fallback to fetch the hosting PE information from PC's local IDF.
        // This will return the hositng PE uuid if hosting PE is registered to
        // the PC.
        final String hostingClusterUuid = fetchHostingPeFromIdf();
        if (StringUtils.isEmpty(hostingClusterUuid)) {
          // returning empty string to prevent null pointer exceptions in
          // clients.
          return "";
        }

        return hostingClusterUuid;
      } else {
        List<String> hostingPEUuidList = zookeeperServiceHelper.getChildren(TRUST_ZK_NODE);
        return hostingPEUuidList.get(0);
      }
    } catch (final ZkClientConnectException | KeeperException.NoNodeException e) {
      final String errorMessage =
          "Could not fetch information about Hosting Prism Element cluster";
      log.error(errorMessage, e);
      throw new PCResilienceException(ErrorMessages.HOSTING_PE_INFO_FETCH_ERROR,
              ErrorCode.PCBR_FETCH_HOSTING_CLUSTER_DETAILS_FAILURE, HttpStatus.INTERNAL_SERVER_ERROR);
    } catch (final EntityDbException ex) {
      final String errorMessage = "Unexpected error occurred";
      log.error(errorMessage, ex);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_FETCH_HOSTING_CLUSTER_DETAILS_FAILURE, HttpStatus.INTERNAL_SERVER_ERROR);
    } catch (InterruptedException e) {
      log.warn("Interrupted!", e);
      Thread.currentThread().interrupt();
      throw new PCResilienceException(ErrorMessages.SERVER_INTERRUPT_ERROR,
              ErrorCode.PCBR_SERVER_INTERRUPTED, HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  public ObjectStoreEndPointDto getObjectStoreEndpointDtoFromPcObjectStoreEndpoint(
      PcObjectStoreEndpoint pcObjectStoreEndpoint)
      throws PCResilienceException {
    if (ObjectUtils.isEmpty(pcObjectStoreEndpoint)) {
      return null;
    }
    ObjectStoreEndPointDto objectStoreEndPointDto = this.getBasicObjectStoreEndpointDtoFromPcObjectStore(pcObjectStoreEndpoint);

    if (!ObjectUtils.isEmpty(pcObjectStoreEndpoint.getEndpointCredentials()) &&
        !ObjectUtils.isEmpty(pcObjectStoreEndpoint.getEndpointCredentials().getCertificate())) {
      objectStoreEndPointDto.setCertificateContent(pcObjectStoreEndpoint.getEndpointCredentials().getCertificate());
      String certificatePath = PCBR_OBJECTSTORE_CERTS_PATH +
          PCUtil.getObjectStoreEndpointUuid(instanceServiceFactory.getClusterUuidFromZeusConfig(),
              pcObjectStoreEndpoint.getEndpointAddress()) +
          PCBR_OBJECTSTORE_CERTS_EXTENSION;
      objectStoreEndPointDto.setCertificatePath(certificatePath);
    }

    ObjectStoreHelper objectStoreHelper = ObjectStoreHelperFactory.getObjectStoreHelper(
        pcObjectStoreEndpoint.getEndpointFlavour()
            .toString());
    objectStoreHelper.updateObjectStoreEndPointDTOParams(objectStoreEndPointDto);
    objectStoreEndPointDto.setBackupUuid(PCUtil.getObjectStoreEndpointUuid(instanceServiceFactory.getClusterUuidFromZeusConfig(),
        pcObjectStoreEndpoint.getEndpointAddress()));
    return objectStoreEndPointDto;
  }

  @Override
  public Map<String, ObjectStoreEndPointDto> getObjectStoreEndpointDtoMapFromObjectStoreCredentialsBackupEntity(
      ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntity) throws PCResilienceException {
    Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap = new HashMap<>();
    Map<String, String> certificatePathMap = objectStoreCredentialsBackupEntity.getCertificatePathMap();
    List<PcObjectStoreEndpoint> pcObjectStoreEndpointList = objectStoreCredentialsBackupEntity.getObjectStoreEndpoints();
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : pcObjectStoreEndpointList) {
      ObjectStoreEndPointDto objectStoreEndPointDto =
          getObjectStoreEndpointDtoFromPcObjectStoreEndpoint(pcObjectStoreEndpoint);
      objectStoreEndPointDto.setCertificatePath(
          certificatePathMap.get(pcObjectStoreEndpoint.getEndpointAddress())
      );
      objectStoreEndPointDtoMap.put(
          pcObjectStoreEndpoint.getEndpointAddress(),
          objectStoreEndPointDto);
    }

    return objectStoreEndPointDtoMap;
  }

  public ObjectStoreBackupTarget getObjectStoreBackupTargetByEntityId(PCVMBackupTargets pcvmBackupTargets, String entityId) throws PCResilienceException {
    ObjectStoreBackupTarget objectStoreBackupTarget = null;
    List<ObjectStoreBackupTarget> objectStoreBackupTargetList = pcvmBackupTargets.getObjectStoreBackupTargetsList().stream().filter(target -> target.getEntityUuid().equals(entityId)).collect(Collectors.toList());
    if (objectStoreBackupTargetList.isEmpty()){
      log.error("Unable to find the backup target for the entity id {}", entityId);
      Map<String,String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_BACKUP_TARGET_EXT_ID, entityId);
      throw new PCResilienceException(ErrorMessages.BACKUP_TARGET_WITH_ENTITY_ID_NOT_FOUND,
              ErrorCode.PCBR_BACKUP_TARGET_NOT_FOUND, HttpStatus.NOT_FOUND, errorArguments);
    }
    objectStoreBackupTarget = objectStoreBackupTargetList.get(0);
    return objectStoreBackupTarget;
  }

  @Override
  public Map<String, ObjectStoreEndPointDto> getObjectStoreEndpointDtoMapFromPcObjectStoreEndpointList(
      List<PcObjectStoreEndpoint> pcObjectStoreEndpointList)
      throws PCResilienceException {
    Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap = new HashMap<>();
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : pcObjectStoreEndpointList) {
      objectStoreEndPointDtoMap.put(
          pcObjectStoreEndpoint.getEndpointAddress(),
          getObjectStoreEndpointDtoFromPcObjectStoreEndpoint(pcObjectStoreEndpoint));
    }
    return objectStoreEndPointDtoMap;
  }

  @Override
  public ObjectStoreEndPointDto getBasicObjectStoreEndpointDtoFromPcObjectStore(
      PcObjectStoreEndpoint pcObjectStoreEndpoint) {
    boolean skipTLS = !ObjectUtils.isEmpty(pcObjectStoreEndpoint.getSkipTLS()) && pcObjectStoreEndpoint.getSkipTLS();
    boolean skipCertificateValidation = !ObjectUtils.isEmpty(pcObjectStoreEndpoint.getSkipCertificateValidation()) && pcObjectStoreEndpoint.getSkipCertificateValidation();
    return new ObjectStoreEndPointDto(pcObjectStoreEndpoint.getPort(),
        skipTLS, pcObjectStoreEndpoint.getBucket(),
        pcObjectStoreEndpoint.getRegion(), pcObjectStoreEndpoint.getIpAddressOrDomain(),
        skipCertificateValidation);
  }
}
