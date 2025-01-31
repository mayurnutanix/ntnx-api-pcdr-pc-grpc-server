package com.nutanix.prism.pcdr.restserver.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.nutanix.api.utils.json.JsonUtils;
import com.nutanix.dp1.pri.prism.v4.recoverpc.*;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto.*;
import com.nutanix.prism.adapter.service.ZeusConfiguration;
import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig;
import com.nutanix.prism.pcdr.PcBackupMetadataProto;
import com.nutanix.prism.pcdr.PcBackupSpecsProto;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.messages.Messages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxy;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.clients.RecoverPcProxyClient;
import com.nutanix.prism.pcdr.restserver.dto.CmspDisabledServices;
import com.nutanix.prism.pcdr.restserver.dto.ObjectStoreCredentialsBackupEntity;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.services.api.BackupStatus;
import com.nutanix.prism.pcdr.util.*;
import com.nutanix.prism.util.CompressionUtils;
import dp1.pri.prism.v4.config.Size;
import dp1.pri.prism.v4.protectpc.ApiError;
import dp1.pri.prism.v4.protectpc.ApiSuccess;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.extern.slf4j.Slf4j;
import nutanix.abac.AbacTypes;
import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.http.HttpStatus;
import org.springframework.util.ObjectUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.SecureRandom;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.*;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;

@Slf4j
public class PCUtil {
  /**
   * Prevent initialization of utility class
   */
  private PCUtil(){}
  /**
   * Checks whether the PE is compatible for backup or not.
   * It returns true if the PE is on master.
   * @return compatibility of PE with PC-DR.
   * (returns true if PEVersionString >= enabledVersionString)
   */
  public static boolean comparePEVersions(final String peVersionString,
                                          final String enabledVersion) {
    // Get the version array for the version on and after which PCDR is enabled.
    final List<Integer> enabledVersionArray =
        getVersionArrayFromString(enabledVersion);
    // Incase of master return true.
    // Assuming master will always have PCDR enabled.
    if (peVersionString.equalsIgnoreCase("master")) {
      return true;
    }
    // Get PE version array for the input version
    final List<Integer> peVersionArray = getVersionArrayFromString(
      peVersionString);
    for (int index = 0; index < enabledVersionArray.size(); index++) {
      // The loop will continue until 
      //   enabledVersionArray.get(index) == peVersionArray.get(index)
      // Example: Comparing 5.17 - peVersion and 5.21 - enabledVersion
      // 5 == 5 -> (21 < 17) and (21 > 17) will return false.
      if (index >= peVersionArray.size()) {
        return false;
      }
      if (enabledVersionArray.get(index) < peVersionArray.get(index)) {
        return true;
      } else if (enabledVersionArray.get(index) > peVersionArray.get(index)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Creates a version list from version string
   * @param version - PE version string.
   * @return Integer list for PE version separated with dots.
   * Ex - 5.21 -> [5, 21]
   */
  public static List<Integer> getVersionArrayFromString(final String version) {
    final List<Integer> versionArray = new ArrayList<>();
    final String[] peVersionArrayAsString = version.split("\\.");
    for (String s : peVersionArrayAsString) {
      versionArray.add(Integer.parseInt(s));
    }
    return versionArray;
  }

  /**
   * Returns an ApiError object.
   * @param errorMessageList - message to be sent to the UI.
   * @return ApiError - Object containing the message.
   */
  public static ApiError getApiError(final String errorType,
    final List<String> errorMessageList) {
    ApiError apiError = new ApiError();
    apiError.setErrorType(errorType);
    apiError.setErrorMessageList(errorMessageList);
    return apiError;
  }

  /**
   * Returns an ApiSuccess object.
   * @param successMessage - message to be sent to the UI.
   * @return ApiSuccess - Object containing the message.
   */
  public static ApiSuccess getApiSuccess(final String successMessage) {
    ApiSuccess apiSuccess = new ApiSuccess();
    log.info(successMessage);
    apiSuccess.setMessage(successMessage);
    return apiSuccess;
  }

  /**
   * It fetches the TrustData from replicaPE using the PE IP list pressent in
   * restoreInternalOpaque.
   * @param restoreInternalOpaque - Object containing replica PE Ip list.
   * @return - returns the trust data object.
   * @throws PCResilienceException - If an ApiError is raised or response
   * is null raise an Exception.
   */
  public static PCRestoreData fetchPCRestoreDataFromReplicaPE(
      RestoreInternalOpaque restoreInternalOpaque,
      String clusterUuid,
      PCDRYamlAdapterImpl pcdrYamlAdapter,
      RecoverPcProxyClient recoverPcProxyClient)
    throws PCResilienceException {

    PCRestoreDataQuery pcRestoreDataQuery =
      QueryUtil.getPCTrustDataQuery(restoreInternalOpaque, clusterUuid,
        pcdrYamlAdapter);
    log.info("PC Restore data query content:"+ pcRestoreDataQuery);

    for (String ip : restoreInternalOpaque.getPeIps()) {
      try {
        return  recoverPcProxyClient.pcRestoreDataAPICallOnPE(ip, pcRestoreDataQuery);
      } catch (HttpClientErrorException e) {
        log.error("Unable to acquire mutual trust info from the given PE with" +
          " IP {}.", ip, e);
        break;
      } catch (RestClientException e) {
        log.debug("Exception encountered while making mutual trust data " +
          "request to {}", ip, e);
        log.warn("Unable to acquire mutual trust data from the ip {} due to {}",
                 ip, e.getMessage());
      } catch (JsonProcessingException e) {
        log.error("Following error encountered while parsing data between V2 and V4 models ", e);
      }
    }
    log.error("Unable to fetch PC restore data from replica PE from all the " +
              "PE IP's.");
    Map<String,String> errorArguments = new HashMap<>();
    errorArguments.put(ErrorCodeArgumentMapper.ARG_CLUSTER_EXT_ID, clusterUuid);
    throw new PCResilienceException(ErrorMessages.MUTUAL_TRUST_INFO_ACQUIRE_ERROR,
            ErrorCode.PCBR_FETCH_MUTUAL_TRUST_FROM_CLUSTER, HttpStatus.INTERNAL_SERVER_ERROR, errorArguments);
  }


  public static void makeCleanIDFApiCallOnReplicaPE(
    RestoreInternalOpaque restoreInternalOpaque, String clusterUuid,
    RecoverPcProxyClient recoverPcProxyClient)
      throws PCResilienceException {
    log.info("Making cleanIDF API request with PC uuid: {}", clusterUuid);
    for (String ip : restoreInternalOpaque.getPeIps()) {
      try {
        recoverPcProxyClient.cleanIDFAfterRestoreAPICallOnPE(ip, clusterUuid);
        // In-case of 204, the response is null.
        return;
      } catch (final HttpClientErrorException e) {
        log.error("Unable to clean IDF info from the replica PE.", e);
      } catch (final RestClientException e) {
        log.debug("Exception encountered while making clean IDF " +
                  "request to {}", ip, e);
        log.warn("Unable to clean IDF data from the ip {} due to {}", ip,
                  e.getMessage());
      } catch (JsonProcessingException e) {
        log.debug("Exception while converting response models " +
            "between V2 and V4 version", e);
      }
    }
    log.error("Unable to clean IDF data on PE from all the given IP's");
    throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
  }

  /**
   * Call restoreIDF endpoint on PE.
   * @param restoreInternalOpaque: An internal Opaque class to be stored in
   * Root recovery task for the Replica PE IPs.
   * @throws PCResilienceException - can trow PCResilienceException
   */
  public static void restoreIDF(RestoreInternalOpaque restoreInternalOpaque,
                                String pcClusterUuid,
                                RecoverPcProxyClient recoverPcProxyClient)
    throws PCResilienceException {
    com.nutanix.dp1.pri.prism.v4.recoverpc.ApiSuccess response = null;
    RestoreIDF restoreIDF = new RestoreIDF();
    restoreIDF.setPcClusterUuid(pcClusterUuid);

    for(String ip: restoreInternalOpaque.getPeIps()) {
      try {
        response = recoverPcProxyClient.restoreIDFAPICallOnPE(ip, restoreIDF);
        if (response != null) {
          log.info("Response by restoreIDF API is {}.",
            response);
          return;
        }
      } catch (HttpClientErrorException e) {
        log.error("Unable to trigger restore IDF from IP {}", ip, e);
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      } catch (RestClientException e) {
        log.debug("Exception encountered while making restore IDF request" +
          "to {}", ip, e);
        log.warn("Unable to make restore idf call to the ip {} due to {}", ip,
                  e.getMessage());
      } catch (JsonProcessingException e) {
        log.debug("Following error encountered while parsing data between V2 and V4 models ", e);
      }
    }
    log.error("Failed to make restore IDF call on all the PE IP's");
    throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
  }

  /**
    * Updates pc_restore_config entity on PC.
    * @param restoreInternalOpaque: An internal Opaque class to be stored in
    * Root recovery task for the Replica PE IPs.
    * @param  entityDBProxy: idf proxy
    * @throws PCResilienceException - can trow PCResilienceException
  */
  public static void updateRestoreConfigOnPC(
          RestoreInternalOpaque restoreInternalOpaque,
          EntityDBProxyImpl entityDBProxy,
          String pcClusterUuid) throws PCResilienceException{

    String error = "Cannot start prism central restoration from objectStore as" +
        " there were few database write issues";
    // Clear any stale pc_restore_config from PC IDF.
    clearRestoreConfigInIdf(entityDBProxy);

    Map<String, Object> attributesValueMap = Maps.newHashMap();
    attributesValueMap.put(
            ENDPOINT_ADDRESS_RESTORE_IDF,
            S3ServiceUtil.getEndpointAddressWithBaseObjectKey(
                    restoreInternalOpaque.getObjectStoreEndpoint().getEndpointAddress(),
                    pcClusterUuid)
    );
    attributesValueMap.put(
            ENDPOINT_FLAVOUR_RESTORE_IDF,
            (long) restoreInternalOpaque.getObjectStoreEndpoint().getEndpointFlavour().
                    ordinal()
    );
    if (StringUtils.isNotEmpty(restoreInternalOpaque.getCredentialKeyId())) {
      attributesValueMap.put(
            CREDENTIAL_KEY_ID, restoreInternalOpaque.getCredentialKeyId()
      );
    }
    attributesValueMap.put(
            BACKUP_UUID, restoreInternalOpaque.getBackupUuid()
    );
    if (!ObjectUtils.isEmpty(restoreInternalOpaque.getObjectStoreEndpoint().getBucket())) {
      attributesValueMap.put(
          BUCKET_NAME, restoreInternalOpaque.getObjectStoreEndpoint().getBucket()
      );
    }
    if (!ObjectUtils.isEmpty(restoreInternalOpaque.getObjectStoreEndpoint().getRegion())) {
      attributesValueMap.put(
          REGION, restoreInternalOpaque.getObjectStoreEndpoint().getRegion()
      );
    }
    if (!ObjectUtils.isEmpty(restoreInternalOpaque.isPathStyle())) {
      attributesValueMap.put(
          PATH_STYLE_ENABLED, restoreInternalOpaque.isPathStyle()
      );
    }
    if (!ObjectUtils.isEmpty(restoreInternalOpaque.getObjectStoreEndpoint().getSkipCertificateValidation())) {
      attributesValueMap.put(
          SKIP_CERTIFICATE_VALIDATION, restoreInternalOpaque.getObjectStoreEndpoint().getSkipCertificateValidation()
      );
    }
    if (!ObjectUtils.isEmpty(restoreInternalOpaque.getCertificatePath())) {
      attributesValueMap.put(
            CERTIFICATE_PATH, restoreInternalOpaque.getCertificatePath()
      );
    }

    log.info(String.format("Attribute value Map for updating %s table is %s: "
            , Constants.PC_BACKUP_RESTORE_CONFIG_TABLE, attributesValueMap));
    UpdateEntityArg.Builder updateEntityArgBuilder = IDFUtil.
      constructUpdateEntityArgBuilder(
        Constants.PC_BACKUP_RESTORE_CONFIG_TABLE,
        String.valueOf(UUID.randomUUID()),
        attributesValueMap
      );
    UpdateEntityRet updateEntityRet;
    try {
      updateEntityRet = entityDBProxy.updateEntity(
        updateEntityArgBuilder.build());
      log.info(String.format("Update entity ret while updating %s table is : %s"
              , Constants.PC_BACKUP_RESTORE_CONFIG_TABLE ,updateEntityRet));

    } catch (InsightsInterfaceException dbException){
      log.error(error, dbException);
      throw new PCResilienceException(ErrorMessages.getInsightsServerWriteErrorMessage("prism central restore data"),
              ErrorCode.PCBR_DATABASE_WRITE_RESTORE_DATA_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    if(updateEntityRet == null){
      log.error(error);
      throw new PCResilienceException(ErrorMessages.getInsightsServerWriteErrorMessage("prism central restore data"),
              ErrorCode.PCBR_DATABASE_WRITE_RESTORE_DATA_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
    * Delete pc_restore_config entity on PC IDF.
    * @param  entityDBProxy: idf proxy
    * @throws PCResilienceException - can throw PCResilienceException
  */
   public static void clearRestoreConfigInIdf(EntityDBProxyImpl entityDBProxy)
           throws PCResilienceException {
       final Query.Builder query = Query.newBuilder();
       query.setQueryName(Constants.PCDR_QUERY);
       query.addEntityList(EntityGuid.newBuilder()
               .setEntityTypeName(Constants.PC_BACKUP_RESTORE_CONFIG_TABLE));
       final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
       List<String> rawColumns = Lists.newArrayList();
       rawColumns.add("entity_id");
       IDFUtil.addRawColumnsInGroupByBuilder(rawColumns, groupBy);
       query.setGroupBy(groupBy);
       GetEntitiesWithMetricsRet restoreConfigResult = null;
       List<String> entityIdList = new LinkedList<>();
       try {
           restoreConfigResult = entityDBProxy.getEntitiesWithMetrics(
                   GetEntitiesWithMetricsArg.newBuilder().setQuery(query).build());
           if (restoreConfigResult != null &&
               restoreConfigResult.getGroupResultsListCount() > 0) {

               for (EntityWithMetric entityWithMetric : restoreConfigResult
                       .getGroupResultsListList().get(0).getRawResultsList()) {
                   String entityId =
                           entityWithMetric.getEntityGuid().getEntityId();
                   log.info(String.format("Adding entity %s for removal from " +
                           "RestoreConfig Table in Database", entityId));
                   entityIdList.add(entityId);
               }
               log.info(String.format("Entity id list to be removed from " +
                               "restore config table is %s",
                       entityIdList));
               if (!entityIdList.isEmpty()) {
                   boolean isDeleted = IDFUtil.batchDeleteIds(entityIdList,
                           Constants.PC_BACKUP_RESTORE_CONFIG_TABLE,
                           entityDBProxy);
                   if (isDeleted) {
                       log.info("Successfully completed cleanup of " +
                               "RestoreConfig Table");
                   } else {
                       String error = "Failed to cleanup RestoreConfig Table";
                       log.error(error);
                       throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
                   }
               }
           }
       } catch (InsightsInterfaceException | IndexOutOfBoundsException e) {

           String error = "Unable to clear RestoreConfig table";
           log.error("Unable to clear RestoreConfig table due to ", e);
         throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
       }
   }

  /**
   * Creates a dummy Admin user entry with a specific uuid.
   * @throws PCResilienceException - can throw restore exception.
   * @throws ZkClientConnectException - can throw zk exception.
   * @throws InterruptedException - can be interrupted.
   */
  public static void createDummyAdminUserForAuth(
    ZookeeperServiceHelperPc zookeeperServiceHelper, EntityDBProxy entityDBProxy)
      throws PCResilienceException, ZkClientConnectException, InterruptedException {
    Stat stat = zookeeperServiceHelper.exists(STARTUP_NODE, false);
    if (stat == null) {
      log.info("No startup zk node found, skipping creation of the entity.");
      return;
    }
    try {
      GetEntitiesWithMetricsRet ret =
          entityDBProxy.getEntitiesWithMetrics(
          GetEntitiesWithMetricsArg
              .newBuilder()
              .setQuery(QueryUtil.constructPCAbacUserCapabilityQueryForAdmin())
              .build());
      boolean entityExists = !(ret.getTotalGroupCount() == 0 ||
                               ret.getGroupResultsList(0)
                                  .getTotalEntityCount() == 0);
      if (entityExists) {
        log.info("Admin custom entry for auth already exists," +
                 " skipping creation of entity.");
        return;
      }
    } catch (InsightsInterfaceException e) {
      log.error("Exception encountered while making Insights rpc -", e);
      throw new PCResilienceException(ErrorMessages.getInsightsServerReadErrorMessage("required data"),
          ErrorCode.PCBR_DATABASE_READ_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    Map<String, Object> attributesValueMap = new HashMap<>();
    AbacTypes.AbacUserCapability abacUserCapability =
        AbacTypes.AbacUserCapability.newBuilder()
                                    .setUsername(ADMIN_USERNAME)
                                    .setUserType(
                                        AbacTypes.AbacUserCapability.sourceType.kLOCAL)
                                    .setUuid(CUSTOM_ENTITY_ID_FOR_ABAC_UC)
                                    .setUserUuid(NIL_UUID)
                                    .build();
    attributesValueMap.put(ZPROTOBUF,
                           CompressionUtils.compress(
                               abacUserCapability.toByteString()));
    attributesValueMap.put(USERNAME_ATTRIBUTE, ADMIN_USERNAME);
    attributesValueMap.put(USER_UUID_ATTRIBUTE, NIL_UUID);
    attributesValueMap.put(USER_TYPE_ATTRIBUTE,
                           AbacTypes.AbacUserCapability.sourceType.kLOCAL.name());

    UpdateEntityArg.Builder updateEntityArgBuilder =
        IDFUtil.constructUpdateEntityArgBuilder(
            ABAC_USER_CAPABILITY_TABLE,
            CUSTOM_ENTITY_ID_FOR_ABAC_UC,
            attributesValueMap);
    try {
      entityDBProxy.updateEntity(updateEntityArgBuilder.build());
      log.info("Successfully updated abac_user_capability entity in database.");
    } catch (InsightsInterfaceException e) {
      log.error("Unable to add dummy admin entity due to -", e);
      throw new PCResilienceException(ErrorMessages.getInsightsServerWriteErrorMessage("required"),
          ErrorCode.PCBR_DATABASE_WRITE_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }

  /**
   * Deletes the hard coded entity created for admin user.
   * @throws InsightsInterfaceException - can throw IDF exception.
   */
  public static void deleteAdminUserForAuth(EntityDBProxy entityDBProxy)
      throws InsightsInterfaceException {
    EntityGuid deleteEntityUuid = EntityGuid
        .newBuilder()
        .setEntityId(CUSTOM_ENTITY_ID_FOR_ABAC_UC)
        .setEntityTypeName(ABAC_USER_CAPABILITY_TABLE)
        .build();
    DeleteEntityArg deleteEntityArg =
        DeleteEntityArg.newBuilder()
                       .setEntityGuid(deleteEntityUuid)
                       .build();
    entityDBProxy.deleteEntity(deleteEntityArg);
    log.info("Successfully deleted abac_user_capability entity with" +
             " entityId - {}", CUSTOM_ENTITY_ID_FOR_ABAC_UC);
  }

  /**
   * Fetch all the pe cluster entries of pc_backup_config from IDF.
   *
   * @return - returns GetEntitiesWithMetricsRet containing clusterUuid of
   * the replica PE's.
   */
  public static GetEntitiesWithMetricsRet fetchPCBackupConfig(
      EntityDBProxy entityDBProxy)
      throws PCResilienceException {
    GetEntitiesWithMetricsRet result;
    try {
      result =
          entityDBProxy.getEntitiesWithMetrics(
              GetEntitiesWithMetricsArg
                  .newBuilder()
                  .setQuery(
                      QueryUtil.constructPCBackupConfigQuery()).build());
    }
    catch (InsightsInterfaceException e) {
      log.error(String.format(Constants.QUERY_IDF_FAILED,
                              Constants.PC_BACKUP_CONFIG), e);
      throw new PCResilienceException(ErrorMessages.getInsightsServerReadErrorMessage("prism central backup metadata"),
              ErrorCode.PCBR_DATABASE_READ_BACKUP_CLUSTER_INFO_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return result;
  }

  /**
   * Fetch all the entries of pc_backup_config where provided attribute is set
   * from IDF.
   *
   * @return - returns GetEntitiesWithMetricsRet containing clusterUuid of
   * the replica PE's.
   */
  public static GetEntitiesWithMetricsRet fetchPCBackupConfigWithSpecificAttrSetFromIDF(
      EntityDBProxy entityDBProxy, String attrName)
      throws PCResilienceException {
    GetEntitiesWithMetricsRet result;
    try {
      result =
          entityDBProxy.getEntitiesWithMetrics(
              GetEntitiesWithMetricsArg
                  .newBuilder()
                  .setQuery(QueryUtil.
                          constructPCBackupConfigWithExistQuery(attrName)).build());
    }
    catch (InsightsInterfaceException e) {
      log.error(String.format(Constants.QUERY_IDF_FAILED,
                              Constants.PC_BACKUP_CONFIG), e);
      throw new PCResilienceException(ErrorMessages.getInsightsServerReadErrorMessage("prism central backup metadata"),
              ErrorCode.PCBR_DATABASE_READ_BACKUP_CLUSTER_INFO_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return result;
  }

  /**
   * This function makes a call to IDF and makes a list of clusterUuid
   * present in pc_backup_config table.
   *
   * @return -  a list of ClusterUuid from pc_backup_config table in IDF.
   */
  public static List<String> fetchExistingClusterUuidInPCBackupConfig(
      EntityDBProxy entityDBProxy) throws PCResilienceException {
    List<String> clusterUuidInPCBackupConfig = new ArrayList<>();
    GetEntitiesWithMetricsRet result =
            fetchPCBackupConfigWithSpecificAttrSetFromIDF(
                    entityDBProxy, CLUSTER_UUID_PC_BACKUP_CONFIG);

    result.getGroupResultsListList()
          .forEach(queryGroupResult -> queryGroupResult
              .getRawResultsList()
              .forEach(entityWithMetric -> {
                final String clusterUuid = Objects.requireNonNull(
                    IDFUtil.getAttributeValueInEntityMetric(
                        Constants.CLUSTER_UUID_PC_BACKUP_CONFIG,
                        entityWithMetric)).getStrValue();
                clusterUuidInPCBackupConfig.add(clusterUuid);
              }));

    return clusterUuidInPCBackupConfig;
  }

  /**
   * This function makes a call to IDF and makes a list of clusterUuid
   * present in pc_backup_config table.
   *
   * @return -  a list of ClusterUuid from pc_backup_config table in IDF.
   */
  public static List<PcObjectStoreEndpoint>
   fetchExistingObjectStoreEndpointInPCBackupConfig(EntityDBProxy entityDBProxy, String clusterUuid)
          throws PCResilienceException {
    PcBackupMetadataProto.PCVMBackupTargets pcvmBackupTargets =
        PcdrProtoUtil.fetchBackupTargetFromPcBackupMetadata(clusterUuid, entityDBProxy);
    List<PcObjectStoreEndpoint> objectStoreEndpointList = new ArrayList<>();
    GetEntitiesWithMetricsRet result =
            fetchPCBackupConfigWithSpecificAttrSetFromIDF(
                    entityDBProxy, PC_BACKUP_CONFIG_ENDPOINT_ATTR);

    result.getGroupResultsListList()
          .forEach(queryGroupResult -> queryGroupResult
              .getRawResultsList()
              .forEach(entityWithMetric -> {
                try {
                    final PcBackupConfig pcBackupConfig = PcBackupConfig.parseFrom(
                        CompressionUtils.decompress(
                                IDFUtil.getAttributeValueInEntityMetric(
                                    ZPROTOBUF, entityWithMetric).getBytesValue()));
                    PcObjectStoreEndpoint objectStoreEndpoint =
                        S3ServiceUtil.getPcObjectStoreEndpointFromProtoPcBackupConfig(pcBackupConfig, pcvmBackupTargets);
                    objectStoreEndpointList.add(objectStoreEndpoint);
                } catch (InvalidProtocolBufferException | PCResilienceException e) {
                    String error = "Unable to convert bytes value to " +
                    "PcBackupConfig proto.";
                    log.error(error, e);
                }
              }));

    return objectStoreEndpointList;
  }

  /**
   * This function makes a call to IDF and makes a list of objectstoreendpoint
   * present in pc_backup_config table and map of endpointAddress and credentialKeyId
   *
   * @return ObjectStoreCredentialsBackupEntity list of ClusterUuid and Map of endPointId and
   * credentialKeyId from pc_backup_config table in IDF.
   */
  public static ObjectStoreCredentialsBackupEntity fetchExistingObjectStoreEndpointInPCBackupConfigWithCredentials
      (EntityDBProxy entityDBProxy, String clusterUuid)
      throws PCResilienceException {
    PcBackupMetadataProto.PCVMBackupTargets pcvmBackupTargets =
        PcdrProtoUtil.fetchBackupTargetFromPcBackupMetadata(clusterUuid, entityDBProxy);
    List<PcObjectStoreEndpoint> objectStoreEndpointList = new ArrayList<>();
    Map<String, String> credentialsKeyIdMap = new HashMap<>();
    Map<String, String> certificatePathMap = new HashMap<>();
    ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntity = new ObjectStoreCredentialsBackupEntity();
    GetEntitiesWithMetricsRet result =
        fetchPCBackupConfigWithSpecificAttrSetFromIDF(
            entityDBProxy, PC_BACKUP_CONFIG_ENDPOINT_ATTR);

    result.getGroupResultsListList()
          .forEach(queryGroupResult -> queryGroupResult
              .getRawResultsList()
              .forEach(entityWithMetric -> {
                try {
                  final PcBackupConfig pcBackupConfig = PcBackupConfig.parseFrom(
                      CompressionUtils.decompress(
                          IDFUtil.getAttributeValueInEntityMetric(
                              ZPROTOBUF, entityWithMetric).getBytesValue()));
                  PcBackupConfig.ObjectStoreEndpoint objectStoreEndpointProto
                      = pcBackupConfig.getObjectStoreEndpoint();
                  PcObjectStoreEndpoint objectStoreEndpoint =
                      S3ServiceUtil.getPcObjectStoreEndpointFromProtoPcBackupConfig(pcBackupConfig, pcvmBackupTargets);

                  objectStoreEndpointList.add(objectStoreEndpoint);
                  credentialsKeyIdMap.put(objectStoreEndpointProto.getEndpointAddress(),
                      objectStoreEndpointProto.getCredentialsKeyId());
                  certificatePathMap.put(objectStoreEndpointProto.getEndpointAddress(),
                      objectStoreEndpointProto.getCertificatePath());
                }
                catch (InvalidProtocolBufferException e) {
                  String error = "Unable to convert bytes value to " +
                                 "PcBackupConfig proto.";
                  log.error(error, e);
                } catch (PCResilienceException e) {
                  log.error("Unable to get IPAddress or hostname from endpoint address", e);
                }
              }));
    objectStoreCredentialsBackupEntity.setObjectStoreEndpoints(objectStoreEndpointList);
    objectStoreCredentialsBackupEntity.setCredentialsKeyIdMap(credentialsKeyIdMap);
    objectStoreCredentialsBackupEntity.setCertificatePathMap(certificatePathMap);
    return objectStoreCredentialsBackupEntity;
  }

  /**
     * This function makes a call to IDF and makes a list of clusterUuid
     * present in pc_backup_config table.
     *
     * @return -  a list of ClusterUuid from pc_backup_config table in IDF.
     */
  public static PcBackupConfig fetchPCBackupConfigWithEntityId(
        EntityDBProxy entityDBProxy, String entityID) throws PCResilienceException {

      GetEntitiesWithMetricsRet result;
      PcBackupConfig pcBackupConfig = PcBackupConfig.getDefaultInstance();
      try {
        result =
            entityDBProxy.getEntitiesWithMetrics(
               GetEntitiesWithMetricsArg
                  .newBuilder()
                  .setQuery(QueryUtil.
                          constructGetPCBackupConfigQueryById(entityID)).build());
      }
      catch (InsightsInterfaceException e) {
        log.error(String.format(Constants.QUERY_IDF_FAILED,
                                Constants.PC_BACKUP_CONFIG), e);
        throw new PCResilienceException(ErrorMessages.getInsightsServerReadErrorMessage("prism central backup metadata"),
                ErrorCode.PCBR_DATABASE_READ_BACKUP_CLUSTER_INFO_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
      }
      try {
        if (result.getTotalGroupCount() > 0
            && result.getGroupResultsList(0).getTotalEntityCount() > 0) {
          pcBackupConfig = PcBackupConfig.parseFrom(
              CompressionUtils.decompress(
                  IDFUtil.getAttributeValueInEntityMetric(
                             ZPROTOBUF, result.getGroupResultsList(0)
                                              .getRawResultsList().get(0))
                         .getBytesValue()));
        }
      }
      catch (InvalidProtocolBufferException e) {
          String error = "Unable to convert bytes value to " +
                  "PcBackupConfig proto.";
          log.error(error, e);
      }

      return pcBackupConfig;
  }
  /**
   * Creates the cmsp zk node to tell which services are supposed to be
   * disabled on startup.
   * @param zookeeperServiceHelper - zookeeper client
   * @param pcdrYamlAdapter - adapter to get the yaml data
   * @throws IOException - can throw IOException
   * @throws KeeperException.NodeExistsException - can throw node exists
   * exception
   * @throws ZkClientConnectException - can throw zk client exception
   * @throws InterruptedException - can throw interrupted exception
   */
  public static void createDisabledServicesZkNode(
      ZookeeperServiceHelperPc zookeeperServiceHelper, PCDRYamlAdapterImpl pcdrYamlAdapter)
      throws IOException, KeeperException.NodeExistsException,
             ZkClientConnectException, InterruptedException {
    if (pcdrYamlAdapter.getServicesToDisableOnStartupCMSP() == null ||
        pcdrYamlAdapter.getServicesToDisableOnStartupCMSP().isEmpty()) {
      log.info("There are no services which are required to be disabled on " +
               "CMSP. Skipping creation of {} zk node.", CMSP_BASE_SERVICES_NODE);
      return;
    }
    Stat statStartupZkNode = zookeeperServiceHelper.exists(STARTUP_NODE, false);
    if (statStartupZkNode != null) {
      Stat statDisabledServiceNode = zookeeperServiceHelper.exists(CMSP_BASE_SERVICES_NODE,
                                                     false);
      if (statDisabledServiceNode == null) {
        ObjectMapper objectMapper = JsonUtils.getObjectMapper();
        CmspDisabledServices cmspDisabledServices = new CmspDisabledServices();
        cmspDisabledServices.setDisabledServices(
            pcdrYamlAdapter.getServicesToDisableOnStartupCMSP());
        String jsonDisabledServices = objectMapper.writeValueAsString(
            cmspDisabledServices);
        createAllParentZkNodes(CMSP_BASE_SERVICES_NODE, zookeeperServiceHelper);
        zookeeperServiceHelper.create(CMSP_BASE_SERVICES_NODE,
                        jsonDisabledServices.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
        log.info("Successfully created {} with content {}",
                 CMSP_BASE_SERVICES_NODE, jsonDisabledServices);
      }
      else {
        log.info("Disabled services zk node already exists, skipping creation " +
            "of it.");
      }
    } else {
      log.debug("STARTUP ZK node does not exists, skipping creation of " +
                "disabled service zk node.");
    }
  }

  /**
   * Create all the parent zknodes for the given node path.
   * @param nodePath - Path of the node for which parent nodes
   *                are required to be created.
   * @throws ZkClientConnectException - can throw ZkClientConnectException
   * @throws InterruptedException - can throw InterruptedException
   * @throws KeeperException.NodeExistsException - can throw NodeExistsException
   */
  public static void createAllParentZkNodes(String nodePath,
                                            ZookeeperServiceHelperPc zookeeperServiceHelper)
      throws ZkClientConnectException, InterruptedException,
             KeeperException.NodeExistsException {
    if (nodePath.isEmpty()) {
      log.debug("Recursion reached till end, nodePath is empty.");
      return;
    }
    int index = nodePath.lastIndexOf("/");
    String parentNodePath = nodePath.substring(0, index);
    Stat stat = zookeeperServiceHelper.exists(parentNodePath, false);
    if (stat == null) {
      createAllParentZkNodes(parentNodePath, zookeeperServiceHelper);
      zookeeperServiceHelper.createZkNode(parentNodePath,
                            null,
                            ZooDefs.Ids.OPEN_ACL_UNSAFE,
                            CreateMode.PERSISTENT);
      log.info("Successfully created parentNode {}", parentNodePath);
    }
  }

  public static long getComputeOnlyNodeCount(EntityWithMetricAndLookup entity) {
    long computeOnlyNodeCount = 0;
    for (LookupQueryResult lookupResults:
        entity.getLookupQueryResultsList()) {
      computeOnlyNodeCount += lookupResults.getResultsCount();
    }
    return computeOnlyNodeCount;
  }

  /**
   * Checks whether CMSP is enabled on PC or not
   * @return true if CMSP is enabled else false
   */
  public static boolean isCMSPEnabled(ZookeeperServiceHelperPc zookeeperServiceHelper) {
    try {
      Stat stat = zookeeperServiceHelper.exists(PC_CMSP_CONFIG_PATH, false);
      if (stat != null) {
        log.info("CMSP is enabled on PC.");
        return true;
      }
    } catch (Exception e) {
      log.error("Unable to check whether CMSP is enabled or not due to the " +
                "following exception -", e);
      // Return true so that the retry happens in the next interval of scheduler
      return true;
    }
    log.info("CMSP is not enabled on the PC");
    return false;
  }

  /**
   * Given two lists, check whether both of them contains same element or not.
   *
   * @param list1 - List 1 to compare
   * @param list2 - List 2 to compare
   * @param <T> - The list can be of any type T
   * @return - returns true if lists contains same element else false.
   */
  public static <T> boolean listEqualsIgnoreOrder(List<T> list1, List<T> list2) {
    if (list1 == null && list2 == null) {
      return true;
    } else if (list1 == null || list2 == null) {
      return false;
    }
    return new HashSet<>(list1).equals(new HashSet<>(list2));
  }

  public static ByteString getByteStringFromString(String input) {
    if (StringUtils.isEmpty(input)) {
      return ByteString.EMPTY;
    }
    else {
      try {
        return ByteString.copyFrom(input, "UTF-8");
      }
      catch (UnsupportedEncodingException var2) {
        return ByteString.EMPTY;
      }
    }
  }

  /**
   * Makes an RPC to IDF to get the cluster info like version, name, and
   * number of nodes.
   * @param replicaPEUuids - uuid of the replica PEs
   * @param entityDBProxy - entityDBProxy object
   * @return - returns cluster info metrics from IDF
   * @throws InsightsInterfaceException - can throw InsightsInterfaceException
   */
  public static GetEntitiesWithMetricsRet getClusterInfoFromIDf(
      List<String> replicaPEUuids, EntityDBProxy entityDBProxy)
      throws InsightsInterfaceException {
    // Max backup entities supported is the minimum of the number of backup
    // entities supported by all individual PEs
    GetEntitiesWithMetricsRet ret;
    ret =
        entityDBProxy.getEntitiesWithMetrics(
            QueryUtil.createGetEntitiesClusterWithNodes(
                replicaPEUuids.stream()
                              .map(replicaPEUuid ->
                                       EntityGuid.newBuilder()
                                                 .setEntityId(replicaPEUuid)
                                                 .setEntityTypeName(CLUSTER)
                                                 .build())
                              .collect(Collectors.toList())));
    return ret;
  }

  /**
   * Method to return the objectstoreendpointuuid from pcUUid and
   * objectstoreendpoitnaddress.
   * @param pcUUID - Prism central cluster uuid
   * @param objectStoreEndpointAddress - address of the object store endpoint.
   * @return - returns the created custom uuid of objectstoreendpoint.
   */
  public static String getObjectStoreEndpointUuid(
      String pcUUID, String objectStoreEndpointAddress) {
    String entityIdStr = pcUUID + objectStoreEndpointAddress;
    return UUID.nameUUIDFromBytes(entityIdStr.getBytes()).toString();
  }

  public static void publishSeedDataInIdf(
          final PcSeedData pcSeedData, final String pcClusterUuid,
          final EntityDBProxy entityDBProxy) {
    Map<String, Object> attributesValueMap = new HashMap<>();
    final String errorMessage = "Unable to publish seed data in IDF";
    log.info("Initiating backup of Prism Central seed data to IDF for PC id " +
             "{}", pcClusterUuid);
    try {
      final ObjectMapper objectMapper = JsonUtils.getObjectMapper();
      final byte[] seedDataAsBytes = objectMapper.writeValueAsBytes(pcSeedData);
      final long currentTimeUsecs = System.currentTimeMillis() * 1000;
      attributesValueMap.put(SEED_DATA_ATTRIBUTE, seedDataAsBytes);
      attributesValueMap.put(SEED_DATA_LAST_UPDATE_ATTRIBUTE, currentTimeUsecs);
      UpdateEntityArg.Builder updateEntityArgBuilder =
          IDFUtil.constructUpdateEntityArgBuilder(PC_SEED_DATA_ENTITY_TYPE,
                                                  pcClusterUuid,
                                                  attributesValueMap);
      entityDBProxy.updateEntity(
          updateEntityArgBuilder.build());
    } catch (final JsonProcessingException ex) {
      // Ignoring this exception as seed data write will be attempted by
      // backup scheduler in next cycle.
      log.error(errorMessage, ex);
    } catch (final InsightsInterfaceException dbException) {
      // Ignoring this exception as seed data write will be attempted by
      // backup scheduler in next cycle.
      log.error(errorMessage, dbException);
    }
  }

  /**
   * Method to write/put PC backup metadata object in a specified bucket on S3.
   * @param objectStoreEndpointList - List of object store endpoints
   * @param instanceServiceFactory - To instantiate zeus config of PC
   * @return
   */
  public static void writePcMetadataOnS3(
      final List<PcObjectStoreEndpoint> objectStoreEndpointList,
      final Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
      final InstanceServiceFactory instanceServiceFactory) {

    final ZeusConfiguration zeusConfig = instanceServiceFactory.getZeusConfig();
    final String pcClusterUuid = zeusConfig.getClusterUuid();
    final String pcClusterName = zeusConfig.getClusterName();
    final String pcClusterExternalIp = zeusConfig.getClusterExternalIp();
    final String pcClusterFqdn = zeusConfig.getClusterFullyQualifiedDomainName();
    final String pcNodeIp = zeusConfig.getLocalNode().getServiceVmExternalIp();

    final String metadataObjectKey =
        ObjectStoreUtil.constructMetadataObjectKey(pcClusterUuid, pcClusterName,
                                 pcClusterFqdn, pcClusterExternalIp, pcNodeIp);
    log.debug("Constructed metadata object key to update {}",
              metadataObjectKey);
    // Iterate over all the endpoints and process them sequentially.
    for (PcObjectStoreEndpoint pcObjectStoreEndpoint : objectStoreEndpointList) {
      ObjectStoreEndPointDto objectStoreEndPointDto = objectStoreEndPointDtoMap.get(pcObjectStoreEndpoint.getEndpointAddress());
      String accessKey =
          pcObjectStoreEndpoint.endpointCredentials.getAccessKey();
      String secretAccessKey =
          pcObjectStoreEndpoint.endpointCredentials.getSecretAccessKey();
      String endpointAddress = pcObjectStoreEndpoint.getEndpointAddress();
      List<String> existingObjectKeysList =
          S3ServiceUtil.getExistingMetadataListForPc(
              objectStoreEndPointDto, pcClusterUuid, accessKey, secretAccessKey);
      final String currentMetadataKey =
          S3ServiceUtil.getPcMetadata(existingObjectKeysList, pcClusterUuid);
      // Check if metadata already exists for this PC. If it doesn't exist,
      // blindly write the metadata on S3.
      if (StringUtils.isEmpty(currentMetadataKey)) {
        log.info(String.format("Writing PC backup metadata %s for PC %s at S3 " +
                        "endpoint %s", metadataObjectKey, pcClusterUuid,
                               endpointAddress));
        S3ServiceUtil
            .writeMetadataObjectOnS3(metadataObjectKey, objectStoreEndPointDto,
                                     accessKey, secretAccessKey);
        continue;
      }

      // Match the current metadata object with the one stored in S3.
      // If difference found delete the earlier object and create new.
      if (!ObjectStoreUtil.checkIfObjectStoreMetadataIsLatest(currentMetadataKey, zeusConfig)) {
        // Exceptions are ignored as this can be later updated by
        // backup scheduler.
        log.info(String.format("Deleting outdated PC backup metadata %s for " +
                                "pc %s", currentMetadataKey, pcClusterUuid));
        if (S3ServiceUtil
            .deleteMetadataObjectFromS3(currentMetadataKey, objectStoreEndPointDto,
                                        accessKey, secretAccessKey)) {
            // Write the updated metadata only if the earlier delete is success.
            log.info(String.format("Writing PC backup metadata %s for PC %s at s3 " +
                       "endpoint %s", metadataObjectKey, pcClusterUuid,
                                   endpointAddress));
            S3ServiceUtil
                .writeMetadataObjectOnS3(metadataObjectKey, objectStoreEndPointDto,
                                         accessKey, secretAccessKey);
        }
      }
    }
  }

  /**
   * Async wrapper to write PC backup metadata object on S3.
   * @return
   */

  public static GetEntitiesWithMetricsRet fetchPcSeedDataFromIdf(
      final EntityDBProxy entityDBProxy) {
    GetEntitiesWithMetricsRet result = null;
    try {
      result = entityDBProxy.getEntitiesWithMetrics(
          GetEntitiesWithMetricsArg
              .newBuilder()
              .setQuery(QueryUtil.constructPcSeedDataQuery())
              .build());
    } catch (final InsightsInterfaceException ex) {
      log.error(String.format(Constants.QUERY_IDF_FAILED,
                              PC_SEED_DATA_ENTITY_TYPE, ex));

    }
    return result;
  }

  /**
   * Extract entity id list from IDF GetEntitiesRet.
   * @param getEntitiesWithMetricsRet - IDF GetEntities response
   *
   * @return Entity id list
   */
  public static List<String> getEntityIdsFromGetEntitiesWithMetrics(
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
   * Check if Idf contains the seed data for current pcvm. This method is
   * responsible to query "pc_seed_data" entity from idf and check if any
   * entityId matches the pc cluster-id.
   * @param entityDBProxy - EntitydbProxy instance
   * @param pcClusterUuid - pc cluster uuid
   *
   * @return true if there is an entity corresponding to this pc in idf,
   * false otherwise.
   */
  public static boolean checkIfSeedDataInserted(
      final EntityDBProxy entityDBProxy,
      final String pcClusterUuid) {
    GetEntitiesWithMetricsRet pcSeedData = fetchPcSeedDataFromIdf(entityDBProxy);
    if (pcSeedData == null) {
      log.info("Unable to find PC seed data");
      return false;
    }

    final List<String> entityIdList =
        getEntityIdsFromGetEntitiesWithMetrics(pcSeedData);
    // Fetching the first element from list as there should be only one
    // entity for a PC.
    if (!entityIdList.isEmpty() && pcClusterUuid.equals(entityIdList.get(0))) {
      log.debug(String.format("PC seed data is available in IDF for pc %s",
               pcClusterUuid));
      return true;
    }

    return false;
  }

  /**
   * Toggle the status of backup scheduler with the message being passed in
   * the method.
   * @param objectStoreEndpointList - List of object store endpoints
   * @param pauseBackup - bool indicating the status of scheduler
   * @param pauseBackupMessage - Message with which the status will be toggled.
   * @param backupStatus - backup status interface
   *
   * @return true if changing the status of scheduler is successful, false
   * otherwise.
   */
  public static boolean resumeBackupStatusForObjectStoreEndpoint(
      final List<PcObjectStoreEndpoint> objectStoreEndpointList,
      final boolean pauseBackup,
      final String pauseBackupMessage,
      final BackupStatus backupStatus) {
    try {
      backupStatus.setBackupStatusForAllObjectStoreReplicasWithRetries(
          objectStoreEndpointList, pauseBackup,
          pauseBackupMessage);
    } catch (final PCResilienceException | InsightsInterfaceException ex) {
      final String errorMessage =
          String.format("Unable to toggle the backup status to %s due to %s",
                        pauseBackup, ex);
      log.error(errorMessage);
      return false;
    }
    return true;
  }

  /**
   * This function validates the pause state of backup, it checks for
   * following conditions -
   * 1. if it's an objectstore endpoint
   * 2. if pause back reason is "gathering backup data"
   * @param pauseMessage - pause message to be matched
   */
  public static boolean isValidPauseStateForObjectStoreEndpoint(
      final EntityDBProxy entityDBProxy, final String pauseMessage) {
    try {
      GetEntitiesWithMetricsRet pcBackupConfigEntities =
          PCUtil.fetchPCBackupConfig(entityDBProxy);
      List<QueryGroupResult> queryGroupResultList =
          pcBackupConfigEntities.getGroupResultsListList();
      for (QueryGroupResult queryGroupResult : queryGroupResultList) {
        List<EntityWithMetric> entityWithMetrics =
            queryGroupResult.getRawResultsList();
        for (EntityWithMetric entityWithMetric : entityWithMetrics) {
          final PcBackupConfig pcBackupConfig =
              PcBackupConfig
                  .parseFrom(
                      CompressionUtils.decompress(
                          IDFUtil.getAttributeValueInEntityMetric(
                              ZPROTOBUF, entityWithMetric)
                                 .getBytesValue()));

          // Process only object store endpoints since seed data is relied upon
          // only when backup to S3.
          if (!pcBackupConfig.hasObjectStoreEndpoint()) {
            continue;
          }

          // Ignore the objectstore endpoint where pause_backup is already false
          if (pcBackupConfig.hasPauseBackup() && !pcBackupConfig.getPauseBackup()) {
            continue;
          }

          String pauseBackupDetailedMessage =
              Messages.PauseBackupMessagesEnum.valueOf(
                  pcBackupConfig.getPauseBackupMessage())
                                              .getPauseBackupDetailedMessage();

          return (pauseBackupDetailedMessage.equals(pauseMessage));
        }
      }
    } catch (final InvalidProtocolBufferException ex) {
      log.error("Failed to verify the pause status due to ", ex);
    } catch (final PCResilienceException ex) {
      log.error("Failed to verify the pause status due to ", ex);
    }

    return false;
  }

  public static OffsetDateTime getOffsetDateTimeFromLong(Long lastSyncTime) {
    if (lastSyncTime!=null)
      return OffsetDateTime.ofInstant(Instant.EPOCH.plus(lastSyncTime, ChronoUnit.MICROS), ZoneOffset.UTC);
    return null;
  }

  public static Long getLongFromOffsetDateTime(OffsetDateTime offsetDateTime) {
    if (offsetDateTime!=null)
      return ChronoUnit.MICROS.between(Instant.EPOCH, offsetDateTime.toInstant());
    return null;
  }

  /**
   * This method is responsible for getting pcBackupConfig for a given entityId
   *
   * @param backupTargetID - target id of the backup replica
   * @param pcRetryHelper - pcRetryHelper instance which is to be used to call IDF
   * @return - returns the PcBackupConfig object
   * @throws PCResilienceException - can throw exception.
   */
  public static PcBackupConfig getPcBackupConfigById(String backupTargetID, PCRetryHelper pcRetryHelper) throws PCResilienceException {

    PcBackupConfig pcBackupConfig =
        pcRetryHelper.fetchPCBackupConfigWithEntityId(backupTargetID);
    // If object store endpoint is not present that
    // means pc backup config does not have object store target configured
    if (pcBackupConfig != null && !(pcBackupConfig.hasClusterUuid() || pcBackupConfig.hasObjectStoreEndpoint())) {
      Map<String,String> errorArguments= new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_BACKUP_TARGET_EXT_ID,backupTargetID);
      throw new PCResilienceException(ErrorMessages.BACKUP_TARGET_WITH_ENTITY_ID_NOT_FOUND,
          ErrorCode.PCBR_BACKUP_TARGET_NOT_FOUND,HttpStatus.NOT_FOUND,errorArguments);
    }
    return pcBackupConfig;
  }

  /**
   * Generates a random string. Using SecureRandom
   * function ensures to have true randomness in the returned string
   *
   * @param length
   * @return random string
   */
  public static String generateSecureRandomString(int length) {
    SecureRandom secureRandom = new SecureRandom();
    int numBytes = (int) Math.floor(length * 6.0 / 8.0);
    byte[] bytes = new byte[numBytes];
    secureRandom.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  public static Size getSizeFromString(String size){
    switch(size) {
      case PC_SIZE_STARTER:
        return Size.STARTER;
      case PC_SIZE_SMALL:
        return Size.SMALL;
      case PC_SIZE_LARGE:
        return Size.LARGE;
      case PC_SIZE_XLARGE:
        return Size.EXTRALARGE;
      default:
        return Size.$UNKNOWN;
    }
  }

   /**
   * Fetch service version spec proto list from pc_backup_specs
   * @param entityDBProxy
   * @param pcClusterUuid
   * @return serviceVersionSpecProtoList
   * @throws PCResilienceException
   */
  public static List<PcBackupSpecsProto.ServiceVersionSpecs.ServiceVersionSpec> fetchServiceVersionsListFromPCBackupSpecs
  (final EntityDBProxy entityDBProxy, String pcClusterUuid)
      throws PCResilienceException {
    List<PcBackupSpecsProto.ServiceVersionSpecs.ServiceVersionSpec> serviceVersionSpecsList = new ArrayList<>();
    List<String> rawColumns = new ArrayList<>();
    rawColumns.add(SERVICE_VERSION_SPECS_ATTRIBUTE);
    Query query = QueryUtil
        .constructPcBackupSpecsProtoQuery(pcClusterUuid, rawColumns);
    GetEntitiesWithMetricsRet result;
    try {
      result = entityDBProxy.getEntitiesWithMetrics(
          GetEntitiesWithMetricsArg.newBuilder().setQuery(query).build());
    }
    catch (InsightsInterfaceException e) {
      String error = "Querying pc_backup_specs for service version specs from IDF failed.";
      log.error(error);
      throw new PCResilienceException(error, ErrorCode.PCBR_DATABASE_READ_BACKUP_SPECS_ERROR,
                                   HttpStatus.INTERNAL_SERVER_ERROR);
    }
    EntityWithMetric entity = null;
    List<QueryGroupResult> groupList = result
        .getGroupResultsListList();
    if (!groupList.isEmpty()) {
      QueryGroupResult group = groupList.get(0);
      List<EntityWithMetric> entityList = group.getRawResultsList();
      if (!entityList.isEmpty()) {
        entity = entityList.get(0);
        try {
          PcBackupSpecsProto.ServiceVersionSpecs serviceVersionSpecs = PcBackupSpecsProto.ServiceVersionSpecs.
              parseFrom(
                  CompressionUtils.decompress(IDFUtil.getAttributeValueInEntityMetric(SERVICE_VERSION_SPECS_ATTRIBUTE
                                  , entity).getBytesValue()));
          serviceVersionSpecsList =
              serviceVersionSpecs.getServiceVersionSpecsList();
        } catch(final InvalidProtocolBufferException e) {
          String error = "Invalid data/content in prism central backup metadata.";
          log.error(error, e);
          throw new PCResilienceException(error, ErrorCode.PCBR_DATABASE_READ_BACKUP_SPECS_ERROR ,
                  HttpStatus.INTERNAL_SERVER_ERROR);
        }
      }
    }
    log.debug("Service version specs list returned from pc_backup_specs",serviceVersionSpecsList);
    return serviceVersionSpecsList;
  }

   /**
   * Read the json config file from path
   * @param configPath - path of the config file
   * @return json data of file
   * @throws PCResilienceException for IOexception
   */
  public static JsonNode fetchServiceMappingJsonConfig(String configPath) throws PCResilienceException {
      try {
        Path path = Paths.get(configPath);
        try (InputStream inputStream = Files.newInputStream(path)) {
          ObjectMapper objectMapper = new ObjectMapper();
          return objectMapper.readTree(inputStream);
        }
      }
    catch (IOException e) {
      String error = "Failed to fetch pc service mapping json from config";
      log.error(error, e);
      throw new PCResilienceException(error , ErrorCode.PCBR_SERVICE_CONFIG_MISSING,
                                         HttpStatus.INTERNAL_SERVER_ERROR);
    }
  }
}