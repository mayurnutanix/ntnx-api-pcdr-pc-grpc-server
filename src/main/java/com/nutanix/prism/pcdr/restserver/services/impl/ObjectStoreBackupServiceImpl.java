/*
 * Copyright (c) 2022 Nutanix Inc. All rights reserved.
 *
 * Author: kapil.tekwani@nutanix.com
 */
package com.nutanix.prism.pcdr.restserver.services.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import com.nutanix.dp1.pri.prism.v4.recoverpc.*;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.prism.pcdr.PcBackupMetadataProto;
import com.nutanix.prism.pcdr.PcBackupSpecsProto;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.proxy.EntityDBProxy;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.services.api.BackupStatus;
import com.nutanix.prism.pcdr.restserver.services.api.ObjectStoreBackupService;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.IDFUtil;
import com.nutanix.prism.pcdr.util.PcdrProtoUtil;
import com.nutanix.prism.util.CompressionUtils;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.extern.slf4j.Slf4j;
import nutanix.infrastructure.cluster.deployment.Deployment;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.*;

import static com.nutanix.prism.pcdr.messages.Messages.GATHERING_DATA_PAUSE_BACKUP_MESSAGE;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.PC_SEED_DATA_ENTITY_TYPE;

@Slf4j
@Service
public class ObjectStoreBackupServiceImpl implements ObjectStoreBackupService {

  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private BackupStatus backupStatus;

  /**
   * Fetches the pc info data for the pc cluster uuid.
   * @param pcClusterUuid - uuid of the current PC
   * @return - returns the PcInfo object
   * @throws PCResilienceException - can throw PC backup exception.
   */
  @Override
  public PcInfo fetchPcInfoData(String pcClusterUuid)
      throws PCResilienceException {

    // Construct PcInfo model by parsing fetched bytes using proto defintion
    InsightsInterfaceProto.GetEntitiesWithMetricsRet pcBackupSpecsResult;
    List<String> attributeList =
        Arrays.asList(Constants.PCVM_SPECS_ATTRIBUTE,
            Constants.PCVM_ENCRYPTION_VERSION,
            Constants.PCVM_KEY_ID,
            Constants.PCVM_CMSP_SPECS);

    pcBackupSpecsResult = fetchPcBackupSpecsProto(pcClusterUuid, attributeList);
    // Construct PcInfo model by parsing fetched bytes using proto defintion
    PcInfo pcInfo = constructPcInfo(pcBackupSpecsResult);
    log.debug("Fetched Prism Central info from Database is: {}", pcInfo);
    return pcInfo;
  }

  /**
   * Parses fetched PC backup metadata bytes and wraps them in PcInfo object.
   * @param pcBackupSpecsResult - GetEntitiesWithMetricsRet object containing data fetched
   * from IDF.
   * @return PcInfo object containing PC backup metadata.
   */
  public PcInfo constructPcInfo(InsightsInterfaceProto.GetEntitiesWithMetricsRet pcBackupSpecsResult)
      throws PCResilienceException {
    InsightsInterfaceProto.EntityWithMetric entity = null;
    List<PcvmFile> fileList = new ArrayList<>();
    PcBackupSpecsProto.PCVMSpecs pcvmSpecs = null;

    // We expect only one element in both groupList and entityList
    // because fetched metadata is only for one PC.
    List<InsightsInterfaceProto.QueryGroupResult> groupList = pcBackupSpecsResult
        .getGroupResultsListList();
    InsightsInterfaceProto.QueryGroupResult group = groupList.get(0);
    List<InsightsInterfaceProto.EntityWithMetric> entityList = group.getRawResultsList();
    entity = entityList.get(0);

    try {
      pcvmSpecs = PcBackupSpecsProto.PCVMSpecs.parseFrom(CompressionUtils.decompress(
          IDFUtil.getAttributeValueInEntityMetric(Constants.PCVM_SPECS_ATTRIBUTE,
              entity).getBytesValue()));

    } catch(final InvalidProtocolBufferException e) {
      String error = "Invalid data/content found in prism central backup metadata";
      log.error(error, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    // Parse keyId and encryptionVersion from result
    String encryptionVersion =
        Objects.requireNonNull(IDFUtil.getAttributeValueInEntityMetric(
            Constants.PCVM_ENCRYPTION_VERSION,
            entity)).getStrValue();
    String keyId =
        Objects.requireNonNull(IDFUtil.getAttributeValueInEntityMetric(
            Constants.PCVM_KEY_ID, entity)).getStrValue();

    // set keyId and encryption version
    PcInfo pcInfo = new PcInfo();
    pcInfo.setEncryptionVersion(encryptionVersion);
    pcInfo.setKeyId(keyId);
    PcDeploySpec pcSpec = new PcDeploySpec();
    PcvmFiles files = new PcvmFiles();
    List<PcvmSpec> pcvmSpecList = new ArrayList<>();

    // Extract the cmsp spec from IDF entity and construct cmsp spec
    try {
      CmspConfig cmspConfig = constructCmspSpecForPcInfo(entity);
      if(cmspConfig != null) {
        pcSpec.setCmspConfig(cmspConfig);
      }
    } catch(Exception e){
      String error = "Error occurred while extracting and setting CMSP Config.";
      log.error(error, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR,
              ErrorCode.PCBR_INTERNAL_SERVER_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }

    // PC Version (Mandatory). In case of master full version of PC
    // will be sent of UI and if its release PC shorter version of PC is sent.
    // Example - el7.3-opt-master-f4397ac4d22fa22afa585bc1e067e16a561a9eeb
    String fullVersion = pcvmSpecs.getFullVersion();
    String displayVersion = pcvmSpecs.getDisplayVersion();
    if(displayVersion.equals(Constants.MASTER_VERSION)){
      pcSpec.setVersion(fullVersion);
    } else {
      pcSpec.setVersion(displayVersion);
    }

    // PC Cluster Name (Optional)
    // Example - Auto_prod_khuntwal_32cde43
    if (pcvmSpecs.hasClusterName()){
      pcSpec.setPcClusterName(pcvmSpecs.getClusterName());
    }

    // List of DNS IPs (Optional)
    if (pcvmSpecs.getClusterNetworkConfig().getDnsCount() > 0){
      List<String> dnsIpList = new ArrayList<>(
          pcvmSpecs.getClusterNetworkConfig().getDnsList());
      pcSpec.setDnsIpList(dnsIpList);
    }

    // List of NTP IPs (Optional)
    if (pcvmSpecs.getClusterNetworkConfig().getNtpCount() > 0){
      List<String> ntpIpList = new ArrayList<>(
          pcvmSpecs.getClusterNetworkConfig().getNtpList());
      pcInfo.setNtpIpList(ntpIpList);
    }

    // PC Virtual IP (Optional)
    if (pcvmSpecs.getClusterNetworkConfig().hasExternalIp()) {
      pcSpec.setVirtualIp(pcvmSpecs.getClusterNetworkConfig().getExternalIp());
    }

    for (int iter=0; iter<pcvmSpecs.getClusterNetworkConfig().
        getSvmInfoCount(); iter++) {
      PcvmSpec pcvmSpec = new PcvmSpec();
      SystemConfig systemConfig = new SystemConfig();
      NetworkConfig networkConfig = new NetworkConfig();

      // PCVM ContainerUuid (Mandatory)
      pcvmSpec.setContainerUuid(pcvmSpecs.getClusterSystemConfig().getContainerUuids(0));

      // PCVM Name (Mandatory)
      pcvmSpec.setVmName(pcvmSpecs.getClusterNetworkConfig().getSvmInfo(iter)
          .getVmName());
      // PCVM VCPUs (Mandatory)
      systemConfig.setNumSockets(pcvmSpecs.getClusterSystemConfig()
          .getNumSockets());
      // PCVM Memory Size (Mandatory)
      systemConfig.setMemorySizeBytes(pcvmSpecs.getClusterSystemConfig()
          .getMemorySizeBytes());
      // PCVM Disk Size (Mandatory)
      systemConfig.setDataDiskSizeBytes(pcvmSpecs.getClusterSystemConfig()
          .getDataDiskSizeBytes());
      // pcvmSpec.setContainerUuid()???
      pcvmSpec.setSystemConfig(systemConfig);

      // PCVM IP (Mandatory)
      networkConfig.setIp(pcvmSpecs.getClusterNetworkConfig().getSvmInfo(iter)
          .getIp());
      // PCVM Gateway IP (Mandatory)
      networkConfig.setGateway(pcvmSpecs.getClusterNetworkConfig()
          .getGatewayIp());
      // PCVM Subnet Mask (Mandatory)
      networkConfig.setSubnetMask(pcvmSpecs.getClusterNetworkConfig()
          .getSubnetMask());
      // PCVM Network UUID (Mandatory)
      // This is the UUID of network that exists/existed on the PE on which
      // the PC was deployed and was used by the PC.
      networkConfig.setNetworkUuid(pcvmSpecs.getClusterNetworkConfig()
          .getSvmInfo(iter).getNetworkUuid());

      pcvmSpec.setNetworkConfig(networkConfig);
      pcvmSpecList.add(pcvmSpec);
    }
    pcSpec.setPcvmList(pcvmSpecList);

    pcSpec.setSize(pcvmSpecs.getSize());
    Set<String> fileQuerySet = Collections.singleton("*");
    fileList.addAll(convertFileProtoToApiModel(pcvmSpecs.getClusterCertificates(), fileQuerySet));
    files.setFileList(fileList);
    pcInfo.setPcDeploySpec(pcSpec);

    pcInfo.setFiles(files);
    return pcInfo;
  }

  /**
   * Converts file definitions in protobuff to Model classes
   *
   * @param pcvmFiles - PCVMFiles Object.
   * @return List of PcvmFile object containing required files data
   */
  private List<PcvmFile> convertFileProtoToApiModel(
      PcBackupSpecsProto.PCVMFiles pcvmFiles, Set<String> fileQuerySet)
      throws PCResilienceException {
    log.info("File Query Set {}", fileQuerySet.toString());
    List<PcvmFile> fileList = new ArrayList<>();
    Set<String> currentQuerySet = new HashSet<>(fileQuerySet);
    for (PcBackupSpecsProto.PCVMFiles.PCVMFile fileData :
        pcvmFiles.getPcvmFilesList()) {

      log.info("Checking file {} part of query set or everything is part of " +
          "query", fileData.getFilePath());
      if(currentQuerySet.contains("*") ||
          currentQuerySet.contains(fileData.getFilePath())){

        PcvmFile file = new PcvmFile();
        file.setFilePath(fileData.getFilePath());
        file.setFileContent(fileData.getFileContent().toStringUtf8());
        file.setIsEncrypted(fileData.getEncrypted());
        fileList.add(file);
        currentQuerySet.remove(fileData.getFilePath());
        log.info("The file " + file.getFilePath() + " added to files list");
      }
    }

    if (currentQuerySet.contains("*")) {
      log.info("Removing * from currentQuerySet, added all the files in fileList" +
          ".");
      currentQuerySet.remove("*");
    }

    if(!currentQuerySet.isEmpty())
    {
      log.error("Unable to find files {} as part of query" +
          currentQuerySet.toString());
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    return fileList;
  }


  /**
   * @param entity  - IDF returned entity from which cmsp values needs to be extracted.
   * @return CmspConfig - Constructed Cmsp Config.
   * @throws PCResilienceException - When an error occurs while extracting cmsp.
   */
  public CmspConfig constructCmspSpecForPcInfo(InsightsInterfaceProto.EntityWithMetric entity)
      throws PCResilienceException{
    List<String> ipBlockList = new ArrayList<>();
    Deployment.CMSPSpec cmspSpec = fetchCmspSpecs(entity);

    if(cmspSpec != null) {
      CmspConfig cmspConfig = new CmspConfig();
      // Cmsp Type
      if(cmspSpec.hasType()){
        Deployment.CMSPSpec.CMSPDeployType cmspDeployType = cmspSpec.getType();
        cmspConfig.setCmspDeploytype(
            CmspDeployTypeEnum.fromString(cmspDeployType.toString()));
      }
      // Platform Block list
      if (cmspSpec.getInfraIpBlockCount() > 0) {
        ipBlockList.addAll(cmspSpec.getInfraIpBlockList());
        cmspConfig.setPlatformIpBlockList(ipBlockList);
      }
      log.debug("Constructed CMSP Ip block list-"+ ipBlockList);
      // Pc Domain Name
      if (cmspSpec.hasPcDomain()) {
        cmspConfig.setPcDomainName(cmspSpec.getPcDomain());
      }
      // Platform Network config
      updateCMSPNetworkConfig(cmspConfig, cmspSpec);
      log.info("Constructed CMSP spec is-"+ cmspConfig);
      return cmspConfig;
    }
    return null;
  }

  /**
   * Update CMSP network config in CMSP config
   * @param cmspConfig - CMSP config which is required to be updated
   * @param cmspSpec - cmsp specs using which the config is getting updated
   * @throws PCResilienceException - can throw pc backup exception.
   */
  private void updateCMSPNetworkConfig(
      CmspConfig cmspConfig, Deployment.CMSPSpec cmspSpec)
      throws PCResilienceException {
    CmspPlatformNetworkConfig cmspNetworkConfig =
        new CmspPlatformNetworkConfig();
    if (cmspSpec.hasInfraNetwork()) {
      Deployment.UvmSpec.IpInfo cmspIpInfo = cmspSpec.getInfraNetwork();
      if (cmspIpInfo.hasGatewayIpv4Address()) {
        cmspNetworkConfig.setGateway(cmspIpInfo.getGatewayIpv4Address());
      }
      if (cmspIpInfo.hasNetmaskIpv4Address()) {
        cmspNetworkConfig.setSubnetMask(cmspIpInfo.getNetmaskIpv4Address());
      }
      if(cmspIpInfo.hasVirtualNetworkName() && cmspIpInfo.hasVirtualNetworkUuid()){
        String error = "Only one of the virtual network expected in backup";
        log.error(error);
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
      if (cmspIpInfo.hasVirtualNetworkUuid()) {
        cmspNetworkConfig.setNetworkUuid(cmspIpInfo.getVirtualNetworkUuid());
      }
      // In case of ESX we are actually backing network name and the
      // network name is set in network Uuid
      if(cmspIpInfo.hasVirtualNetworkName()){
        cmspNetworkConfig.setNetworkUuid(cmspIpInfo.getVirtualNetworkName());
      }
    }
    cmspConfig.setPlatformNetworkConfiguration(cmspNetworkConfig);
  }

  /**
   *
   * @param entity - fetches the CMSP specs from IDF
   * @return - returns the CMSP specs proto
   * @throws PCResilienceException - can throw pc backup exception.
   */
  private Deployment.CMSPSpec fetchCmspSpecs(
      InsightsInterfaceProto.EntityWithMetric entity) throws PCResilienceException {
    Deployment.CMSPSpec cmspSpec;
    try {
      if(IDFUtil.getAttributeValueInEntityMetric(
          Constants.PCVM_CMSP_SPECS, entity) == null) {
        log.info("Setting CMSP spec as null in Prism Central info as CMSP spec is not " +
            "present in DB.");
        return null;
      }
      cmspSpec = Deployment.CMSPSpec.parseFrom(CompressionUtils.decompress(
          IDFUtil.getAttributeValueInEntityMetric(Constants.PCVM_CMSP_SPECS,
                                                  entity).getBytesValue()));
      log.debug("Decompressed CMSP spec - "+ cmspSpec);
    } catch(final InvalidProtocolBufferException e) {
      String error = "Invalid data/content found in prism central backup metadata";
      log.error(error, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    } catch (Exception e){
      String error = "Exception occurred while extracting CMSP specs.";
      log.error(error, e);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    return cmspSpec;
  }

  /**
   * Fetches PC Backup metadata(specs and files) from IDF.
   * @param pcClusterUuid - Cluster UUID of the PC for which data is being
   * fetched.
   * @return GetEntitiesWithMetricsRet object containing required specs
   * and files data.
   */
  private InsightsInterfaceProto.GetEntitiesWithMetricsRet fetchPcBackupSpecsProto(
          String pcClusterUuid,
          List<String> attributeList)
      throws PCResilienceException {
    // Creating a query builder
    final InsightsInterfaceProto.Query.Builder query =
            InsightsInterfaceProto.Query.newBuilder();
    // Set the name of the query.
    query.setQueryName(Constants.PCDR_QUERY);
    // Add the entity type for the query.
    query.addEntityList(InsightsInterfaceProto.EntityGuid.newBuilder()
        .setEntityTypeName(Constants.PC_BACKUP_SPECS_TABLE)
        .setEntityId(pcClusterUuid));
    final InsightsInterfaceProto.QueryGroupBy.Builder groupBy =
            InsightsInterfaceProto.QueryGroupBy.newBuilder();

    // Set raw Columns in query
    IDFUtil.addRawColumnsInGroupByBuilder(attributeList, groupBy);
    query.setGroupBy(groupBy);
    // Fetch PCVMSpec and PCVMFiles from IDF
    InsightsInterfaceProto.GetEntitiesWithMetricsRet result = null;

    try {
      result = entityDBProxy.getEntitiesWithMetrics(
          InsightsInterfaceProto.GetEntitiesWithMetricsArg.newBuilder().
                  setQuery(query).build());

    } catch (InsightsInterfaceException e) {
      String error = "Querying prism central backup data from database failed.";
      log.error(error, e);
      throw new PCResilienceException(ErrorMessages.getInsightsServerReadErrorMessage("prism central backup data"),
              ErrorCode.PCBR_DATABASE_READ_BACKUP_CLUSTER_INFO_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
    }

    if(result != null &&
        result.getGroupResultsListCount()>0) {
      log.info(String.format("Found prism central backup metadata: %s", result));
    }else {
      String error;
      error = "Couldn't retrieve Prism Central backup data from database. " +
          "This could be because Prism Central cluster id is incorrect.";
      log.error(error);
      throw new PCResilienceException(ErrorMessages.getInsightsServerReadErrorMessage("prism central backup data"),
              ErrorCode.PCBR_DATABASE_READ_BACKUP_CLUSTER_INFO_ERROR,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    return result;
  }

  @Override
  public OriginalHostingPeInfo populateOriginalHostingPeInfo(String pcClusterUuid)
      throws PCResilienceException{
    // Fetch the Pc backup targets from IDF Table.
    // Query IDF table that contains backup info using pc uuid
    // Filter the data returned from IDF and fetch backup targets and
    // hostingPe from it.
    InsightsInterfaceProto.GetEntitiesWithMetricsRet result;
    PcBackupMetadataProto.PCVMHostingPEInfo pcvmHostingPEInfo;
    try {
      result = PcdrProtoUtil.fetchPcBackupMetadataProtoFromIDF
          (pcClusterUuid, entityDBProxy);
      pcvmHostingPEInfo = PcdrProtoUtil.fetchHostingPEInfoFromFromIDFResult(result);
    } catch (Exception e) {
      log.error("Failed to fetch the details of backup Prism Element from Database due to exception :", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }

    OriginalHostingPeInfo originalHostingPeInfo = new  OriginalHostingPeInfo();

    if (pcvmHostingPEInfo != null) {
      originalHostingPeInfo.setClusterUuid(pcvmHostingPEInfo.getClusterUuid());
      originalHostingPeInfo.setName(pcvmHostingPEInfo.getName());
      originalHostingPeInfo.setIps(pcvmHostingPEInfo.getIpsList());
      String externalIp = pcvmHostingPEInfo.getExternalIp();
      if (!StringUtils.isBlank(externalIp))  {
        originalHostingPeInfo.setExternalIp(externalIp);
      }
    }
    return originalHostingPeInfo;
  }

  @Override
  public List<String> populatePcIpAddresses(PcInfo pcInfo) {
    List<String> pcIpAddresses = new ArrayList<>();

    // first insert the  virtual IP of cluster
    // then insert ip of individual pc-vm's
    if (!StringUtils.isBlank(pcInfo.getPcDeploySpec().getVirtualIp())) {
      pcIpAddresses.add(pcInfo.getPcDeploySpec().getVirtualIp());
    }

    for(PcvmSpec pcvmSpec : pcInfo.getPcDeploySpec().getPcvmList()) {
      pcIpAddresses.add(pcvmSpec.getNetworkConfig().getIp());
    }
    return pcIpAddresses;
  }

  @Override
  public PcSeedData populateBackupSeedData(String pcClusterUuid) throws PCResilienceException {
    PcSeedData pcSeedData = new PcSeedData();
    PcInfo pcInfo = fetchPcInfoData(pcClusterUuid);
    pcSeedData.setPcClusterUuid(pcClusterUuid);
    pcSeedData.setOriginalHostingPeInfo(populateOriginalHostingPeInfo(pcClusterUuid));
    pcSeedData.setPcInfo(pcInfo);
    pcSeedData.setPcIpAddresses(populatePcIpAddresses(pcInfo));
    log.debug("Seed data: {}", pcSeedData);
    return pcSeedData;
  }

  public void initiateBackupToObjectStore(
      final Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
      final String pcClusterUuid,
      final EntityDBProxy entityDBProxy,
      final List<PcObjectStoreEndpoint> objectStoreEndpointList) {
    try {
      // Publishing the seed data in IDF.
      final PcSeedData pcSeedData = populateBackupSeedData(pcClusterUuid);
      PCUtil.publishSeedDataInIdf(pcSeedData, pcClusterUuid, entityDBProxy);

      // Handle the pause backup status based on seed data availability.
      handlePcBackupSchedulerPauseStatus(objectStoreEndpointList, pcClusterUuid,
                                         entityDBProxy);

      // Write object metadata string to S3.
      // 1. If writing metadata on S3 failed during add replica.
      // 2. If PC's parameters have modified (change in name, vip, fqdn etc)
      // then earlier metadata object needs to be deleted and latest
      // metadata should be updated on S3 instead.
      PCUtil.writePcMetadataOnS3(objectStoreEndpointList,
                                 objectStoreEndPointDtoMap,
                                 instanceServiceFactory);
    } catch (final PCResilienceException ex) {
      log.warn("Error encountered while initiating backup to object store.", ex);
    }
  }

  /**
   * Async method to provide support of backup to object store.
   *
   * @param objectStoreEndPointDtoMap - map of DTO of endpoint address and Object store DTO
   * @param pcClusterUuid - PC cluster uuid.
   */
  @Async("adonisServiceThreadPool")
  public void initiateBackupToObjectStoreAsync(
      final Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
      final String pcClusterUuid,
      final EntityDBProxy entityDBProxy,
      final List<PcObjectStoreEndpoint> objectStoreEndpointList) {
    try {
      initiateBackupToObjectStore(objectStoreEndPointDtoMap, pcClusterUuid,
                                  entityDBProxy,
                                  objectStoreEndpointList);
    }
    catch (Exception e) {
      log.error("Error encountered while initiating backup to object store.",
                e);
    }
  }

  /**
   * This function deletes the PCSeedData from IDF upon remove replica
   * synchronously.
   *
   * @param pcClusterUuid - cluster-id of PCVM.
   */
  public void deletePcSeedDataFromInsightsDb(final String pcClusterUuid) {
    try {
      final InsightsInterfaceProto.EntityGuid deleteEntityUuid =
          InsightsInterfaceProto.EntityGuid
              .newBuilder()
              .setEntityId(pcClusterUuid)
              .setEntityTypeName(PC_SEED_DATA_ENTITY_TYPE)
              .build();

      final InsightsInterfaceProto.DeleteEntityArg deleteEntityArg =
          InsightsInterfaceProto.DeleteEntityArg
              .newBuilder()
              .setEntityGuid(deleteEntityUuid)
              .build();

      entityDBProxy.deleteEntity(deleteEntityArg);
    } catch (final InsightsInterfaceException ex) {
      String errorMessage =
          String.format("Failed to delete PC seed data for PC %s due to ",
                        pcClusterUuid);
      log.error(errorMessage, ex);
    }
  }

  /**
   * This method ensures the handling of pause backup status of objectstore
   * endpoint entity.
   * @param objectStoreEndpointList - list of PC object store endpoint lists.
   * @param pcClusterUuid - Cluster uuid of pcvm
   * @param entityDBProxy - EntityDb instance
   */
  public void handlePcBackupSchedulerPauseStatus(
      final List<PcObjectStoreEndpoint> objectStoreEndpointList,
      final String pcClusterUuid,
      final EntityDBProxy entityDBProxy) {
    if (PCUtil.checkIfSeedDataInserted(entityDBProxy, pcClusterUuid) &&
        PCUtil.isValidPauseStateForObjectStoreEndpoint(entityDBProxy, GATHERING_DATA_PAUSE_BACKUP_MESSAGE)) {
        log.info("Resuming the backup for PC {}", pcClusterUuid);
        PCUtil.resumeBackupStatusForObjectStoreEndpoint(
            objectStoreEndpointList, false,
            "", backupStatus);
    }
  }
}
