package com.nutanix.prism.pcdr.restserver.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.nutanix.api.utils.json.JsonUtils;
import com.nutanix.authn.AuthnProto;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PCRestoreData;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PcvmFile;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PcvmFiles;
import com.nutanix.dp1.pri.prism.v4.recoverpc.ZookeeperNode;
import com.nutanix.infrastructure.cluster.genesis.GenesisInterfaceProto;
import com.nutanix.infrastructure.cluster.genesis.GenesisInterfaceProto.DeleteCACertificateRet;
import com.nutanix.infrastructure.cluster.genesis.GenesisInterfaceProto.GenesisGenericError;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.insights.ifc.InsightsInterfaceProto.*;
import com.nutanix.prism.base.zk.ICloseNullEnabledZkConnection;
import com.nutanix.prism.base.zk.ProtobufZNodeManagementException;
import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.exception.GenericException;
import com.nutanix.prism.pcdr.PcBackupMetadataProto;
import com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMBackupTargets;
import com.nutanix.prism.pcdr.PcBackupSpecsProto;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.messages.APIErrorMessages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.dto.ObjectStoreCredentialsBackupEntity;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.util.*;
import com.nutanix.prism.pcdr.util.*;
import com.nutanix.prism.service.EntityDbServiceHelper;
import com.nutanix.prism.util.CompressionUtils;
import com.nutanix.util.base.CommandException;
import com.nutanix.zeus.protobuf.Configuration.ConfigurationProto;
import dp1.pri.prism.v4.protectpc.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NodeExistsException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.restserver.constants.Constants.SERVICES_ENABLEMENT_PATH;
import static com.nutanix.prism.pcdr.restserver.util.PCUtil.getByteStringFromString;
import static com.nutanix.prism.proxy.GenesisRemoteProxyImpl.RemoteRpcName.*;

@Slf4j
@Service
public class RestoreDataServiceImpl implements RestoreDataService {

  @Autowired
  private ICloseNullEnabledZkConnection zkClient;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;
  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private EntityDbServiceHelper entityDbServiceHelper;
  @Autowired
  private PCDRYamlAdapterImpl pcdrYamlAdapter;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private PCBackupService pcBackupService;
  @Autowired
  private PCVMDataService pcvmDataService;
  @Autowired
  private GenesisServiceHelper genesisServiceHelper;
  @Autowired
  private FileUtil fileUtil;
  @Autowired
  @Qualifier("adonisServiceThreadPool")
  private ExecutorService adonisServiceThreadPool;
  @Autowired
  private CertFileUtil certFileUtil;

  @Autowired
  private PCRetryHelper pcRetryHelper;

  @Value("${prism.pcdr.reconcile.wait.duration:300000}")
  private int reconcileWaitDuration;

  @PostConstruct
  public void init() {
    ExecutorService recoveryInitExecutorService =
        Executors.newSingleThreadExecutor();
    recoveryInitExecutorService.execute(() -> {
      try {
        pcRetryHelper.createDummyAdminUserForAuth();
      } catch (Exception e) {
        log.error("Some exception encountered while creation of the admin" +
                  " entity through Adonis -", e);
      }
      try {
        pcRetryHelper.createDisabledServicesZkNode();
      } catch (Exception e) {
        log.error("Some exception encountered while creation of disabled " +
                  "service zk node.", e);
      }
    });
    recoveryInitExecutorService.shutdown();
  }

  @VisibleForTesting
  public void init(PCRetryHelper pcRetryHelper) {
    this.pcRetryHelper = pcRetryHelper;
  }

  /**
   * After the restore is complete to IDF, then in the next step the data is
   * restored back to zk nodes and file systems. We raise an error incase the
   * data is not restored correctly.
   * @throws PCResilienceException - PC restore task exception.
   */
  public void restorePCDataFromIDF() throws PCResilienceException {
    // Restore files.
    try {

      PcvmRestoreFiles pcvmRestoreFiles = fetchPcvmRestoreFilesFromIdf();
      // Restore data on all the SVMs
      restoreFilesOnAllPCVms(pcvmRestoreFiles);

      // Restore Zk nodes.
      List<ZookeeperNode> zookeeperNodeList = fetchZkNodesFromIdf();
      restoreZkData(zookeeperNodeList);

    } catch (Exception e) {
      log.error("Unable to write content back to PC. Raising exception.", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      throw new PCResilienceException(exceptionDetails.getMessage(),exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }
  }

  /**
   * Restore files on all VMs in the PC cluster
   * @param pcvmRestoreFiles Files data to restore
   * @throws InterruptedException Exception when file move is interrupted
   * @throws ExecutionException Exception due to
   * multi-threading failure
   *
   */
  public void restoreFilesOnAllPCVms(PcvmRestoreFiles pcvmRestoreFiles)
    throws InterruptedException, ExecutionException,
    PCResilienceException {
    List<String> svmIps = new ArrayList<>(ZookeeperUtil.getSvmIpVmUuidMap(
      instanceServiceFactory.getZeusConfig().getAllNodes()).values());

    log.info("List of svm-ips for which restore-files API would be invoked: {}", svmIps);
    List<CompletableFuture<Object>> allFutures = new ArrayList<>();
    for (String svmIp : svmIps) {
      log.info("Making request for restoring files on SVM IP {}", svmIp);
      // The files data needs to be base64 encoded
      allFutures.add(CompletableFuture
                         .supplyAsync(() -> pcRetryHelper
                             .restoreFilesApiRetryable(svmIp,
                                                       pcvmRestoreFiles), adonisServiceThreadPool)
                         .handle((result, ex) -> {
                           if (ex != null) {
                             log.error("Unable to write files due to the" +
                                       " following exception:", ex);
                             return false;
                           }
                           // Result is null in this scenario
                           // cause 204(NO_CONTENT) request.
                           return result;
                         }));
    }
    CompletableFuture.allOf(allFutures.toArray(
      new CompletableFuture[0])).join();
    int ipReached = 0;
    List<String> errorList = new ArrayList<>();
    List<String> errorSvmIps = new ArrayList<>();
    for (CompletableFuture<Object> restoreFilesApiResponse : allFutures) {
      if (restoreFilesApiResponse.get() != null) {
        errorList.add(String.format("Unable to restore " +
          "files on the svmIP %s", svmIps.get(ipReached)));
        errorSvmIps.add(svmIps.get(ipReached));
      }
      ipReached++;
    }

    if (!errorList.isEmpty()) {
      String errorMsg = String.format("Unable to restore files on svmips %s ", errorSvmIps);
      Map<String,String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_SVM_IPS, String.valueOf(errorSvmIps));
      throw new PCResilienceException(errorMsg, ErrorCode.PCBR_RESTORE_FILES_ON_SVMIPS_FAILURE,
              HttpStatus.INTERNAL_SERVER_ERROR,errorArguments);
    }
    log.info("Successfully updated files on all SvmIPs.");
  }

  /**
   * A function to fetch ZK nodes data from IDF. This makes a call to
   * pc_zk_data table and fetches all the zk data, if it is a proto specific
   * data it writes it
   * back.
   * @throws PCResilienceException - raises a data recovery exception.
   */
  private List<ZookeeperNode> fetchZkNodesFromIdf() throws PCResilienceException {
    List<Entity> zkEntities;
    try {
      zkEntities = new ArrayList<>(IDFUtil.getEntitiesAsList(entityDBProxy,
        Constants.PC_ZK_DATA));
    } catch (InsightsInterfaceException e) {
      log.error("Error encountered while querying for pc_zk_data", e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    List<ZookeeperNode> zookeeperNodeList = new LinkedList<>();

    for (Entity entity: zkEntities) {
      ZookeeperNode zookeeperNode = new ZookeeperNode();

      boolean isParent = entityDbServiceHelper
        .getAttributeValueInEntity(Constants.IS_PARENT, entity).getBoolValue();
      zookeeperNode.setIsParent(isParent);

      String entityId = entity.getEntityGuid().getEntityId();
      String zkNodePath = entityId.split(Constants.SEPARATOR_ZK_ENTITY_ID)[1];
      zookeeperNode.setPath(zkNodePath);

      DataValue zkValue = entityDbServiceHelper
          .getAttributeValueInEntity(Constants.VALUE, entity);

      if (zkValue != null) {
        byte[] zkBytes = zkValue.getBytesValue().toByteArray();
        zookeeperNode
            .setBase64value(Base64.getEncoder().encodeToString(zkBytes));
      }
      zookeeperNodeList.add(zookeeperNode);
    }
    return zookeeperNodeList;
  }

  /**
   * This method is responsible for restoring zknodes on PC using the
   * zookeeperNodesList
   *
   * @param zookeeperNodes - Object containing list of zookeeperNodes data
   */
  private void restoreZkData(List<ZookeeperNode> zookeeperNodes)
      throws IOException, PCResilienceException {
    // Sorted lexographically so that the parent always comes before the
    // children
    // Currently for protos only two levels are getting handled. If there are
    // more than two levels then modify the code to check if previous element
    // was also a parent and keep the currentProtoEnum.
    // Making a copy of zookeeperNodes.
    List<ZookeeperNode> zookeeperNodesList = new ArrayList<>(zookeeperNodes);
    zookeeperNodesList.sort(Comparator.comparing(ZookeeperNode::getPath));
    // Extracting zeus config and removing it from zookeeper nodes list as
    // we don't want to restore complete old zeus config
    ZookeeperNode zeusConfig = zookeeperNodesList.stream()
        .filter(zkNode -> zkNode.getPath().equals(Constants.PC_ZEUS_CONFIG_PATH))
        .findAny()
        .orElse(null);
    zookeeperNodesList.removeIf(zkNode -> (
        zkNode.getPath().equals(Constants.PC_ZEUS_CONFIG_PATH)));
    try {
      log.info("Starting restoration of Zeus Configuration");
      restoreZeusConfig(zeusConfig);
      log.info("Restoration of Zeus Configuration successful");
    } catch (PCResilienceException ex){
      log.error("Exception occurred while restoring zeus config : ", ex);
      throw ex;
    }
    for (ZookeeperNode zkNode : zookeeperNodesList) {
      String nodePath = zkNode.getPath();
      log.info("Trying creation of node {}", nodePath);
      if (zkNode.getIsParent() != null && zkNode.getIsParent()) {
        // Assuming that parent node does not contain any proto with
        // logicaltimestamp.
        if (zkNode.getBase64value() != null) {
          byte[] nodeBytes =
              Base64.getDecoder().decode(zkNode.getBase64value());
          createOrUpdateZkNode(nodePath, nodeBytes);
        } else {
          createOrUpdateZkNode(nodePath, null);
        }
        log.info("Successfully created parent zkNode {}", nodePath);
      }
      else {
        try {
          ZkProtoUtil.ProtoEnum currentProtoEnum =
              pcdrYamlAdapter.findProtoEnumFromPath(nodePath);

          // Write Zk node data.
          byte[] nodeBytes =
              Base64.getDecoder().decode(zkNode.getBase64value());
          if (currentProtoEnum != null) {
            log.debug("The current protoEnum has been set to {}",
                      currentProtoEnum);
            createZkNodeWithProtoData(nodeBytes, nodePath, currentProtoEnum);
            log.info("Successfully created child zkNode {} with content in " +
                     "proto format.", nodePath);
          }
          else {
            createOrUpdateZkNode(nodePath, nodeBytes);
            log.info("Successfully created child zkNode {} with content.",
                     nodePath);
          }
        }
        catch (Exception e) {
          log.error("Unable to create ZK node {}, error occurred", nodePath, e);
          throw new IOException(String
                                    .format("Unable to write zk node %s", nodePath));
        }
      }
    }
  }

  /**
   * Create the zk node with the corresponding bytes, if they belong to the
   * corresponding proto then it will create it, if it is a string then it
   * will create the node with string values.
   * @param nodeBytes - node bytes stored
   * @param nodePath - path of the zk node
   * @param currentProtoEnum - current proto
   * @throws IOException - In case creating zk node fails
   * @throws NodeExistsException - Node exists exception changed
   * @throws ZkClientConnectException - zk client can reset in between
   * @throws InterruptedException - interruption can occur
   * @throws ProtobufZNodeManagementException - protobufznode management
   * exception
   * @throws InvocationTargetException - invocation target has some issues
   * @throws NoSuchMethodException - method is not present
   * @throws IllegalAccessException - illegally accessed
   */
  private void createZkNodeWithProtoData(byte[] nodeBytes, String nodePath,
                                         ZkProtoUtil.ProtoEnum currentProtoEnum)
      throws IOException, NodeExistsException, ZkClientConnectException,
             InterruptedException, ProtobufZNodeManagementException,
             InvocationTargetException, NoSuchMethodException,
             IllegalAccessException, PCResilienceException {
    if (nodeBytes == null || nodeBytes.length == 0) {
      createOrUpdateZkNode(nodePath, nodeBytes);
    } else {
      PCUtil.createAllParentZkNodes(nodePath, zookeeperServiceHelper);
      try {
        ZkProtoUtil.writeZkContentUsingProto(currentProtoEnum,
                                             nodePath,
                                             nodeBytes,
                                             zkClient);
      } catch (InvalidProtocolBufferException e) {
        log.warn("Unable to create zk node with proto due to " +
                 "InvalidProtocolBufferException, ", e);
        log.info("Data present in nodeBytes - {}",
                 nodeBytes);
        // Create zk node with non-proto data.
        createOrUpdateZkNode(nodePath, nodeBytes);
      }
    }
  }

  /**
   * This method is used to restore the zeus configuration partially.
   *
   * @param zeusConfig - Zues Configuration node objecty.
   */
  public void restoreZeusConfig(ZookeeperNode zeusConfig) throws PCResilienceException {
    if(zeusConfig == null){
      String warnMsg = "Zeus config is not backed up or not being restored";
      log.warn(warnMsg);
      return;
    }
    ConfigurationProto backedZeusConfig;
    try {
      backedZeusConfig = ConfigurationProto.parseFrom(
          Base64.getDecoder().decode(zeusConfig.getBase64value()));
      log.debug("Backed zeus config is-"+backedZeusConfig);
    } catch (Exception e) {
      log.error("Error occurred while fetching current zeus configuration information");
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    int retries = Constants.ZEUS_CONFIG_RETIRES;
    while (retries > 0) {
      ConfigurationProto currentZeusConfig = getCurrentZeusConfig(backedZeusConfig);
      log.debug("Updated Zeus configuration is-" + currentZeusConfig);
      boolean updateStatus = instanceServiceFactory.updateZeusConfig(
          currentZeusConfig);
      if (updateStatus) {
        log.info("Zeus configuration info was successfully updated");
        return;
      } else {
        log.error("Unable to update zeus config with backup information");
      }
      retries--;
    }
    throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
  }

  /**
   * This method is used to get current zeus config and set some of the values
   * from backed up zeus config.
   *
   * @param backedZeusConfig - Object containing Backed up zeus Config
   *                           information.
   */
  public ConfigurationProto getCurrentZeusConfig(ConfigurationProto backedZeusConfig){
    ConfigurationProto.Builder configurationProtoBuilder = instanceServiceFactory
        .getZeusConfig().getConfiguration().toBuilder();
    log.debug("Initial zeus config-"+ configurationProtoBuilder);
    if (backedZeusConfig.hasAuthConfig()) {
      log.info("Setting Auth config to - {}", backedZeusConfig.getAuthConfig());
      configurationProtoBuilder.setAuthConfig(backedZeusConfig.getAuthConfig());
    }
    if (backedZeusConfig.hasAdsConfig()) {
      log.info("Setting Ads config to - {}", backedZeusConfig.getAdsConfig());
      configurationProtoBuilder.setAdsConfig(backedZeusConfig.getAdsConfig());
    }
    if (backedZeusConfig.hasAegis()) {
      log.info("Setting Aegis to - {}", backedZeusConfig.getAegis());
      configurationProtoBuilder.setAegis(backedZeusConfig.getAegis());
    }
    if (backedZeusConfig.hasSnmpInfo()) {
      log.info("Setting SNMP Info to - {}", backedZeusConfig.getSnmpInfo());
      configurationProtoBuilder.setSnmpInfo(backedZeusConfig.getSnmpInfo());
    }
    if (backedZeusConfig.hasRsyslogConfig()) {
      log.info("Setting Rsyslog config to - {}",
               backedZeusConfig.getRsyslogConfig());
      configurationProtoBuilder.setRsyslogConfig(backedZeusConfig.getRsyslogConfig());
    }
    if (!backedZeusConfig.getSshKeyListList().isEmpty()) {
      List<ConfigurationProto.SSHKey> sshKeyList = backedZeusConfig
          .getSshKeyListList().stream()
          .filter(sshKey -> !sshKey.getKeyType().equals(
              ConfigurationProto.SSHKey.SshKeyType.kNodeKey))
          .collect(Collectors.toList());
      log.info("Setting sshKeyList as - {}", sshKeyList);
      configurationProtoBuilder.addAllSshKeyList(sshKeyList);
    }
    if (backedZeusConfig.hasTimezone()) {
      log.info("Updating timezone in zeus config to - {}",
               backedZeusConfig.getTimezone());
      configurationProtoBuilder.setTimezone(backedZeusConfig.getTimezone());
    }
    if (backedZeusConfig.hasPasswordRemoteLoginEnabled()) {
      log.info("Setting password remote login enabled - {}",
               backedZeusConfig.getPasswordRemoteLoginEnabled());
      configurationProtoBuilder.setPasswordRemoteLoginEnabled(
          backedZeusConfig.getPasswordRemoteLoginEnabled());
    }
    log.debug("Final zeus config-" + configurationProtoBuilder);
    return  configurationProtoBuilder.build();
  }


  /**
   * Creates or updates the zk node based on whether it exists or not.
   * @param nodePath - Path of the node
   * @param nodeBytes - Bytes which we want to put in node.
   * @throws IOException - Can throw IOException based on whether the write
   * was successful or not
   */
  public void createOrUpdateZkNode(String nodePath, byte[] nodeBytes)
      throws IOException, PCResilienceException {
    try {
      Stat stat = zookeeperServiceHelper.exists(nodePath, false);
      if (stat == null) {
        PCUtil.createAllParentZkNodes(nodePath, zookeeperServiceHelper);
        zookeeperServiceHelper.createZkNode(nodePath,
                              nodeBytes,
                              ZooDefs.Ids.OPEN_ACL_UNSAFE,
                              CreateMode.PERSISTENT);
      }
      else if (nodeBytes != null && nodeBytes.length > 0) {
        log.info("Setting data for node {}", nodePath);
        zookeeperServiceHelper.setData(nodePath, nodeBytes, stat.getVersion());
      }
      else {
        log.info("Skipping creation of zk node {}, it already exists.",
                 nodePath);
      }
    }
    catch (final NodeExistsException e) {
      log.info(
          "{} zk node already exists, skipping creation of this node.",
          nodePath);
    }
    catch (Exception e) {
      log.error("Unable to create ZK node {}, error occurred", nodePath, e);
      throw new IOException(String
                                .format("Unable to write zk node %s", nodePath));
    }
    if (nodePath.equals(SERVICES_ENABLEMENT_PATH)) {
      disableConfiguredServicesOnGenesisStartup(SERVICES_ENABLEMENT_PATH);
    }
  }

  /**
   * Restore pcvm files present in IDF pc_backup_specs table. This function
   * is called independently on every PC using API.
   * @throws InterruptedException - When file move is interrupted.
   * @throws IOException - if there is write issue raises exception.
   */
  public void restorePCVMFiles(PcvmRestoreFiles pcvmRestoreFiles) throws
    IOException, InterruptedException {
    log.info("Restore files count {}", pcvmRestoreFiles.getFileList().size());
    for(PcvmRestoreFile pcvmRestoreFile: pcvmRestoreFiles.getFileList()) {

      log.info("Trying to write file {}.", pcvmRestoreFile.getFilePath());
      boolean requiresSudo = pcvmRestoreFile.getFilePath().startsWith(Constants.HOME_PRIVATE_PATH);

      // File contents are base64 encoded
      fileUtil.writeFileContent(pcvmRestoreFile.getFilePath(),
                                Constants.TEMP_DIR_PATH,
        ByteString.copyFromUtf8(pcvmRestoreFile.getFileContent()),
        pcvmRestoreFile.getIsEncrypted(), requiresSudo,
                                pcvmRestoreFile.getKeyId(),
                                pcvmRestoreFile.getEncryptionVersion());
      log.info("Successfully created file {} with content.",
        pcvmRestoreFile.getFilePath());
    }
  }

  /**
   * Fetch files from PC IDF in pc_backup_specs table.
   * @throws PCResilienceException - raises exception.
   * @throws IOException - if there is write issue raises exception.
   */
  @VisibleForTesting
  public PcvmRestoreFiles fetchPcvmRestoreFilesFromIdf()
      throws PCResilienceException, IOException {
    PcvmRestoreFiles pcvmRestoreFiles = new PcvmRestoreFiles();
    List<PcvmRestoreFile> pcvmRestoreFileList = new LinkedList<>();
    String clusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    // Create a list containing rawColumns to query from IDF.
    List<String> rawColumns = new ArrayList<>();
    rawColumns.add(Constants.PCVM_FILES_ATTRIBUTE);
    rawColumns.add(Constants.PCVM_ENCRYPTION_VERSION);
    rawColumns.add(Constants.PCVM_KEY_ID);
    Query query = QueryUtil.constructPcBackupSpecsProtoQuery(clusterUuid, rawColumns);
    GetEntitiesWithMetricsRet result;
    try {
      result = entityDBProxy.getEntitiesWithMetrics(
        GetEntitiesWithMetricsArg.newBuilder().setQuery(query).build());
    } catch (InsightsInterfaceException e) {
      String error = "Querying pc_backup_specs from IDF failed.";
      log.error(error);
      throw new PCResilienceException(ErrorMessages.getInsightsServerReadErrorMessage(ErrorMessages.PC_BACKUP_DATA_STR),
              ErrorCode.PCBR_DATABASE_READ_BACKUP_CLUSTER_INFO_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    if (result == null) {
      String error = "No files data found in IDF";
      log.error(error);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }

    for (QueryGroupResult groupResult: result.getGroupResultsListList()) {
      for (EntityWithMetric entityWithMetric: groupResult.getRawResultsList()) {

        DataValue dataValueFiles = IDFUtil.getAttributeValueInEntityMetric(
          Constants.PCVM_FILES_ATTRIBUTE, entityWithMetric);
        DataValue dataValueEncryptionVersion =
            IDFUtil.getAttributeValueInEntityMetric(
            Constants.PCVM_ENCRYPTION_VERSION, entityWithMetric);
        DataValue dataValueKeyId = IDFUtil.getAttributeValueInEntityMetric(
            Constants.PCVM_KEY_ID, entityWithMetric);
        if (dataValueFiles!=null && dataValueEncryptionVersion!=null
            && dataValueKeyId!=null) {
          ByteString fileByteString = dataValueFiles.getBytesValue();
          PcBackupSpecsProto.PCVMFiles pcvmFiles =
            PcBackupSpecsProto.PCVMFiles.parseFrom(
              CompressionUtils.decompress(fileByteString));
          String keyId = dataValueKeyId.getStrValue();
          String encryptionVersion = dataValueEncryptionVersion.getStrValue();
          log.info("Successfully retrieved file content from IDF {}",
            pcvmFiles.toString());
          for (PcBackupSpecsProto.PCVMFiles.PCVMFile pcvmFile:
            pcvmFiles.getPcvmFilesList()) {
            PcvmRestoreFile pcvmRestoreFile = new PcvmRestoreFile();
            pcvmRestoreFile.setFileContent(pcvmFile.getFileContent().toStringUtf8());
            pcvmRestoreFile.setFilePath(pcvmFile.getFilePath());
            pcvmRestoreFile.setIsEncrypted(pcvmFile.getEncrypted());
            pcvmRestoreFile.setKeyId(keyId);
            pcvmRestoreFile.setEncryptionVersion(encryptionVersion);
            pcvmRestoreFileList.add(pcvmRestoreFile);
          }
        } else {
          log.error("DataValue corresponding to PCVMFiles missing.");
          throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
        }
      }
    }
    pcvmRestoreFiles.setFileList(pcvmRestoreFileList);
    return pcvmRestoreFiles;
  }

  /**
   * Wait for other services to reconcile their config data in IDF.
   * @param reconcileTimeStart - start time when the reconciliation started.
   * @return - returns a boolean stating whether the wait is finished or not.
   */
  public boolean hasTaskExceededWaitTime(
    LocalDateTime reconcileTimeStart) {
    long duration = Duration.between(reconcileTimeStart,
      LocalDateTime.now()).toMillis();
    log.info("Current time spent {} waiting for reconcile.", duration);
    return duration >= reconcileWaitDuration;
  }

  /**
   * This method is responsible for restoring the basic trust on PC using the
   * pcRestoreData object.
   * @param pcRestoreData - Object containing the files and zookeeper data
   */
  public void restoreTrustDataOnPC(PCRestoreData pcRestoreData)
    throws IOException, InterruptedException, ExecutionException, PCResilienceException {
    log.info("Restore Trust data files on PC");
    restoreFilesTrustDataOnPC(pcRestoreData.getFilesData(),
                              pcRestoreData.getEncryptionVersion(),
                              pcRestoreData.getKeyId());
    log.info("Restore Trust data zkNodes on PC");
    if(pcRestoreData.getZookeeperData() != null)
      restoreZkData(pcRestoreData.getZookeeperData().getNodes());
    else
      log.info("No Zookeeper nodes found as part of the trust data");

  }

  /**
   * This method is responsible for restoring the files on PC using the
   * files data
   * @param restoreFiles - Object containing files needed to restore trust
   */
  public void restoreFilesTrustDataOnPC(PcvmFiles restoreFiles,
                                        String encryptionVersion,
                                        String keyId)
    throws ExecutionException, InterruptedException, PCResilienceException {
    if(restoreFiles == null || restoreFiles.getFileList() == null
      || restoreFiles.getFileList().isEmpty()) {
      log.info("No files found as part of trust data restore");
      return;
    }
    PcvmRestoreFiles pcvmRestoreFiles = new PcvmRestoreFiles();
    List<PcvmRestoreFile> pcvmRestoreFileList = new LinkedList<>();

    for(PcvmFile pcvmFile: restoreFiles.getFileList()){
      PcvmRestoreFile pcvmRestoreFile = new PcvmRestoreFile();
      pcvmRestoreFile.setIsEncrypted(pcvmFile.getIsEncrypted());
      pcvmRestoreFile.setFilePath(pcvmFile.getFilePath());
      pcvmRestoreFile.setFileContent(pcvmFile.getFileContent());
      pcvmRestoreFile.setKeyId(keyId);
      pcvmRestoreFile.setEncryptionVersion(encryptionVersion);
      pcvmRestoreFileList.add(pcvmRestoreFile);
    }
    pcvmRestoreFiles.setFileList(pcvmRestoreFileList);
    restoreFilesOnAllPCVms(pcvmRestoreFiles);
  }

  /**
   * Restores the replica clusters stored in pc_backup_metadata to
   * pc_backup_config table.
   */
  public void restoreReplicaDataOnPC(RestoreInternalOpaque restoreInternalOpaque)
    throws PCResilienceException {
    Map<String, String> credentialskeyIdMap = new HashMap<>();
    String pcClusterUuid =
      instanceServiceFactory.getClusterUuidFromZeusConfigWithRetry();
    log.debug("Fetching PCVMBackupTargets for pc cluster uuid {}",
      pcClusterUuid);
    PCVMBackupTargets pcvmBackupTargets;
    try {
      pcvmBackupTargets = PcdrProtoUtil.fetchBackupTargetFromPcBackupMetadata(
          instanceServiceFactory.getClusterUuidFromZeusConfig(), entityDBProxy);
      log.debug("Fetched replica UUID's from pc_backup_metadata. {}",
        pcvmBackupTargets);
    } catch (Exception e){
      log.error("Unable to fetch pc_backup_metadata from IDF due to exception : ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }

    if (pcvmBackupTargets != null) {
      Set<String> peClusterUuids =
        pcvmBackupTargets.getBackupTargetsList().stream().map(
          PCVMBackupTargets.BackupTarget::getClusterUuid
        ).collect(Collectors.toSet());
      log.info("PE clusters in pc_backup_metadata are: {}", peClusterUuids);

      List<PcObjectStoreEndpoint> objectStoreEndpointList = new ArrayList<>();
      ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntity =
          new ObjectStoreCredentialsBackupEntity();
      Map<String, String> certificatePathMap = new HashMap<>();
      List<PCVMBackupTargets.ObjectStoreBackupTarget> objectStoreBackupTargetList = pcvmBackupTargets.getObjectStoreBackupTargetsList();
      for (PCVMBackupTargets.ObjectStoreBackupTarget objectStoreBackupTarget : objectStoreBackupTargetList) {

        PcObjectStoreEndpoint objectStoreEndpoint =
            S3ServiceUtil.getPcObjectStoreEndpointFromPCVMObjectStoreBackupTarget(objectStoreBackupTarget);
        objectStoreEndpointList.add(objectStoreEndpoint);
        certificatePathMap.put(objectStoreBackupTarget.getEndpointAddress(),
            objectStoreBackupTarget.getCertificatePath());

      }
      objectStoreCredentialsBackupEntity.setObjectStoreEndpoints(objectStoreEndpointList);
      objectStoreCredentialsBackupEntity.setCertificatePathMap(certificatePathMap);
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap =
          pcvmDataService.getObjectStoreEndpointDtoMapFromObjectStoreCredentialsBackupEntity(
              objectStoreCredentialsBackupEntity
          );
      log.info("ObjectStores in pc_backup_metadata are: {}",
               objectStoreEndpointList);

      // Setting the credential key in case of object store end needed to
      // resume backup later
      if (!ObjectUtils.isEmpty(restoreInternalOpaque.getObjectStoreEndpoint()) &&
          StringUtils.isNotEmpty(restoreInternalOpaque.getObjectStoreEndpoint().getEndpointAddress())) {
        credentialskeyIdMap.put(S3ServiceUtil.getEndpointAddressWithBaseObjectKey(restoreInternalOpaque.
                                    getObjectStoreEndpoint().getEndpointAddress(),pcClusterUuid),
                                restoreInternalOpaque.getCredentialKeyId());
      }

      InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg =
          InsightsInterfaceProto.BatchUpdateEntitiesArg.newBuilder();
       pcBackupService.addUpdateEntityArgsForPCBackupConfig(
          peClusterUuids, objectStoreEndpointList, objectStoreEndPointDtoMap, credentialskeyIdMap, batchUpdateEntitiesArg);
      if (!pcBackupService.makeBatchUpdateRPC(batchUpdateEntitiesArg)) {
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
      log.info("Successfully restored replica info in pc_backup_config table.");

      //update certs if recovery endpoint is nutanix objects
      if (!ObjectUtils.isEmpty(restoreInternalOpaque.getObjectStoreEndpoint()) &&
          !ObjectUtils.isEmpty(restoreInternalOpaque.getCertificatePath())) {
        ObjectStoreEndPointDto objectStoreEndPointDto = pcvmDataService.
                getObjectStoreEndpointDtoFromPcObjectStoreEndpoint(restoreInternalOpaque.getObjectStoreEndpoint());
        try {
          certFileUtil.createCertFileOnAllPCVM(Collections.singletonList(objectStoreEndPointDto), false);
        } catch (Exception e) {
          // Not failing recovery even if cert update fails on more than quoram.
          log.error("Failed to update Nutanix objects Certs after recovery ", e);
        }
      }

    } else {
      log.error("PCVM backup target are null. Unable to restore replica info");
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }

  }

  public void stopServicesUsingGenesis(List<String> servicesToStop)
      throws PCResilienceException, ExecutionException, InterruptedException {
    List<String> svmIps = new ArrayList<>(ZookeeperUtil.getSvmIpVmUuidMap(
        instanceServiceFactory.getZeusConfigWithRetry().getAllNodes()).values());
    log.info("Attempting to stop {} services on hosts with IP's {} by " +
             "invoking 'genesis stop' command for each host", servicesToStop,
             svmIps);
    long startTimeInMillis = System.currentTimeMillis();
    Float timeTakenInSec = null;
    try {
      List<CompletableFuture<StopServicesApiResponse>> allFutures =
          new ArrayList<>();
      for (String svmIp : svmIps) {
        log.info("Invoking API to stop {} services via genesis on SVM IP {}",
                 servicesToStop, svmIp);
        allFutures.add(
            CompletableFuture.supplyAsync(
                () -> pcRetryHelper.stopServicesApiRetryable(svmIp,servicesToStop),
                         adonisServiceThreadPool)
                             .handle((result, ex) -> {
                               if (ex != null) {
                                 log.error("Unable to make request for stop service due" +
                                           " to the following exception -", ex);
                                 return null;
                               }
                               return result;
                             }));


      }
      CompletableFuture.allOf(allFutures.toArray(
          new CompletableFuture[0])).join();
      timeTakenInSec = (float) (System.currentTimeMillis() - startTimeInMillis) /1000;
      int ipReached = 0;
      log.info("Completed execution of 'genesis stop' command on Prism " +
               "Central with SvmIp: {}", svmIps);
      List<String> errorList = new ArrayList<>();
      List<String> errorSvmIps = new ArrayList<>();
      for (CompletableFuture<StopServicesApiResponse> stopServicesResp : allFutures) {
        String errorMessage = String.format("Unable to stop services" +
                                            " on the svmIP %s",
                                            svmIps.get(ipReached));
        if (stopServicesResp.get() == null) {
          errorList.add(errorMessage);
          errorSvmIps.add(svmIps.get(ipReached));
        }
        ipReached++;
      }

      if (!errorList.isEmpty()) {
        String errorMsg = String.format("Unable to stop services on svmips %s ", errorSvmIps);
        Map<String,String> errorArguments = new HashMap<>();
        errorArguments.put(ErrorCodeArgumentMapper.ARG_SVM_IPS, String.valueOf(errorSvmIps));
        throw new PCResilienceException(errorMsg,ErrorCode.PCBR_STOP_SERVICES_ON_SVMIPS_FAILURE,
                HttpStatus.INTERNAL_SERVER_ERROR,errorArguments);
      }
    } finally {
      if (timeTakenInSec == null) {
        timeTakenInSec = (float) (System.currentTimeMillis() - startTimeInMillis) /1000;
      }
      log.info("Time taken to execute 'genesis stop' command on PC : {}s", timeTakenInSec);
    }
  }

  /**
   * this method will remove trust certs related to previous hosting PE(PE1)
   * when PC is hosted on new PE(PE2) after recovery
   *
   * @throws PCResilienceException - invocation target has some issues
   */
  @Retryable(value = {PCResilienceException.class}, maxAttempts = 3, backoff = @Backoff(delay = 10000))
  public void removePreviousHostingPETrust() throws PCResilienceException {
    GetEntitiesWithMetricsRet ret;
    PcBackupMetadataProto.PCVMHostingPEInfo oldPcVmHostingInfo;
    try {
      // Check if CMSP is enabled for this PC or not.
      if (!PCUtil.isCMSPEnabled(zookeeperServiceHelper)) {
        log.warn("Since CMSP is not enabled for this PC, " +
                  "we are skipping 'removePreviousHostingPETrust' step with SUCCESS");
        return;
      }
      // Fetch PC backup metadata proto from IDF
      ret = PcdrProtoUtil.fetchPcBackupMetadataProtoFromIDF(
          instanceServiceFactory.getClusterUuidFromZeusConfig(), entityDBProxy);

      // Fetch the details of old PE host on which this PC was hosted before recovery
      oldPcVmHostingInfo = PcdrProtoUtil.fetchHostingPEInfoFromFromIDFResult(ret);

      // Attempt to delete certs of old PE from recovered PC
      deleteTrustCertificates(oldPcVmHostingInfo.getClusterUuid());
    } catch (Exception e) {
      log.error("Failed to remove Cluster Trust certificates from prism central trust-store due to exception : ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }
  }

  /**
   * this method will remove trust certs related to previous hosting PE(PE1)
   * when PC is hosted on new PE(PE2) after recovery by making Genesis RPC call
   *
   * @param peUuid - Uuid of PE whose certs needs to be deleted from PC truststore(ca.pem)
   * @throws CommandException - invocation target has some issues
   */

  public boolean deleteTrustCertificates(String peUuid) throws CommandException {
    try {
      log.info("Removing Certificate Chain for PE with Uuid: {}", peUuid);
      final GenesisInterfaceProto.DeleteCACertificateArg.Builder delCACertArgBuilder =
          GenesisInterfaceProto.DeleteCACertificateArg.newBuilder();

      // Please note that PE UUID is passed as byteString and not String
      delCACertArgBuilder.setClusterUuid(getByteStringFromString(peUuid));

      final DeleteCACertificateRet delCertRet =
          genesisServiceHelper.doExecute(DELETE_CA_CERTIFICATE, delCACertArgBuilder.build());
      if (delCertRet == null) {
        log.error("Unable to delete certificate chain for PE with Uuid: {}", peUuid);
        throw new CommandException(String.format("Unable to delete certificate chain for PE with Uuid: %s", peUuid));
      }
      if (delCertRet.getError().getErrorType() != GenesisGenericError.ErrorType.kNoError) {
        final String rpcErrorMsg = delCertRet.getError().getErrorMsg();
        log.error("Failed to remove/delete certificates due to error: " + rpcErrorMsg);
        throw new CommandException("Failed to remove/delete certificates due to error: " + rpcErrorMsg);
      }
      log.info("Successfully deleted certificates for PE with Uuid: {}", peUuid);
      return Boolean.TRUE;
    } catch (final GenericException ex) {
      throw new CommandException(ex);
    }
  }

  /**
   * This method is responsible for retrieving the certificate for a PE
   * present in ca.pem through genesis RPC.
   *
   * @param peUuid - Uuid of prism element for which we want to retrieve the
   *               cert.
   * @return - returns the ca cert for the given Prism element
   * @throws GenericException - can throw generic exception faced while
   * making genesis rpc.
   */
  public AuthnProto.CACert getCACertificates(String peUuid)
      throws GenericException {
    // Creating the CA Cert Arg, converting the pe uuid to bytestring. The PE
    // uuid is the owner cluster uuid.
    GenesisInterfaceProto.GetCACertificateArg getCACertificateArg =
        GenesisInterfaceProto
            .GetCACertificateArg
            .newBuilder()
            .setClusterUuid(
                ByteString.copyFrom(peUuid.getBytes(StandardCharsets.UTF_8)))
            .build();
    // Makes an RPC to genesis.
    GenesisInterfaceProto.GetCACertificateRet getCACertificateRet =
        genesisServiceHelper.doExecute(GET_CA_CERTIFICATE, getCACertificateArg);
    return getCACertificateRet.getCaCert();
  }

  /**
   * This method is responsible for adding the certificate to prism central
   * ca.pem using genesis addCAcert RPC.
   *
   * @param caCert - the cacert that we want to add as part of addcert RPC.
   * @return - returns the boolean value whether the add trust was successful
   * or not.
   */
  public boolean addTrustCertificates(AuthnProto.CACert caCert)
      throws GenericException, PCResilienceException {
    // Adding CA cert as part of AddCACertificateArg.
    final GenesisInterfaceProto.AddCACertificateArg addCACertificateArg =
        GenesisInterfaceProto.AddCACertificateArg
            .newBuilder()
            .setCaCert(caCert)
            .build();
    GenesisInterfaceProto.AddCACertificateRet addCACertificateRet =
        genesisServiceHelper.doExecute(ADD_CA_CERTIFICATE, addCACertificateArg);
    // By default, the error message is kNoError and we will not raise an
    // exception in case the error is kNoError.
    if (addCACertificateRet.hasError() &&
        addCACertificateRet.getError().getErrorType()
        != GenesisGenericError.ErrorType.kNoError) {
      log.error("Error encountered while adding certificate to ca.pem {} {}",
                addCACertificateRet.getError(),
                addCACertificateRet.getError().getErrorMsg());
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    } else {
      log.info("Successfully added CA Cert for owner cluster uuid {} as part " +
               "of ca.pem", caCert.getOwnerClusterUuid());
      // Return true in case we do not face any error.
      return true;
    }
  }

  public void disableConfiguredServicesOnGenesisStartup(String zkPath) throws PCResilienceException {
    Stat stat = new Stat();
    byte[] zkNodedata;
    try {
      zkNodedata = zookeeperServiceHelper.getData(zkPath, stat);
    }
    catch (final KeeperException.NoNodeException | ZkClientConnectException | InterruptedException e) {
      if(e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.error(APIErrorMessages.ZK_READ_ERROR, SERVICES_ENABLEMENT_PATH, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    updateServicesEnablementStatusZk(zkNodedata);
  }

  public void updateServicesEnablementStatusZk(byte[] zkNodedata) throws PCResilienceException {

    if (zkNodedata == null) {
      log.error("Unexpected service enablement status, zkNodedata is null, Not proceeding further");
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    Map<String, String> serviceNameVsEnablementStatusMap;
    ObjectMapper objectMapper = JsonUtils.getObjectMapper();
    try {
      serviceNameVsEnablementStatusMap = objectMapper.readValue(zkNodedata, LinkedHashMap.class);
    }
    catch (IOException e) {
      log.error("Error in reading service enablement status zk", e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    List<String> servicesToDisableOnStartupGenesis = pcdrYamlAdapter.getServicesToDisableOnStartupGenesis();
    if (!servicesToDisableOnStartupGenesis.isEmpty()) {
      for (String serviceName : servicesToDisableOnStartupGenesis) {
        serviceNameVsEnablementStatusMap.put(serviceName, "No");
      }
    }
//map to json
    String serviceNameVsEnablementStatusJson;
    try {
      serviceNameVsEnablementStatusJson = objectMapper.writeValueAsString(serviceNameVsEnablementStatusMap);
    }
    catch (JsonProcessingException e) {
      log.error("Error in conversion to Json", e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    //json to bytes
    byte[] zkNodeDataModified = serviceNameVsEnablementStatusJson.getBytes();

    try {
      zookeeperServiceHelper.setData(SERVICES_ENABLEMENT_PATH, zkNodeDataModified, -1);
    }
    catch (final KeeperException.BadVersionException | KeeperException.NoNodeException | ZkClientConnectException |
                 InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      log.error(APIErrorMessages.ZK_UPDATE_ERROR, SERVICES_ENABLEMENT_PATH, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    catch (Exception e) {
      log.error("Exception occurred while setting zk node {}", SERVICES_ENABLEMENT_PATH, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
  }
}
