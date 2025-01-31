package com.nutanix.prism.pcdr.restserver.services.api;

import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.prism.pcdr.PcBackupMetadataProto;
import com.nutanix.prism.pcdr.PcBackupSpecsProto;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.dto.ObjectStoreCredentialsBackupEntity;
import com.nutanix.prism.pojos.GroupEntityResult.GroupResult;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import nutanix.infrastructure.cluster.deployment.Deployment;

import java.util.List;
import java.util.Map;

import static com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMBackupTargets;
import static com.nutanix.prism.pcdr.PcBackupMetadataProto.PCVMHostingPEInfo;

/**
 * PCVMDataService is used to generate PCVMSpecs and PCVMBackupTarget info
 * using IDF and Zookeeper.
 */
public interface PCVMDataService {
  PcBackupSpecsProto.PCVMSpecs constructPCVMSpecs(String keyId)
      throws InsightsInterfaceException, PCResilienceException;

  PcBackupSpecsProto.PCVMSpecs.SystemConfig constructClusterSystemConfig(
       final GroupResult groupResult) throws PCResilienceException;

  Deployment.CMSPSpec fetchCMSPSpecs() throws PCResilienceException;

  Long fetchDataDiskSizeBytes() throws PCResilienceException;

  PcBackupSpecsProto.PCVMSpecs.NetworkConfig constructClusterNetworkConfig(
      final GroupResult groupResult,
      final List<String> vmUuidList , String ip) throws PCResilienceException;

  PCVMHostingPEInfo constructPCVMHostingPEInfo() throws PCResilienceException;

  PCVMBackupTargets constructPCVMBackupTargets(
      final List<String> clusterUuids,
      List<PcObjectStoreEndpoint> objectStoreEndpointList,
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
      boolean updateSyncTime)
      throws PCResilienceException;
  PcBackupMetadataProto.DomainManagerIdentifier constructDomainManagerIdentifier();

  PcBackupSpecsProto.PCVMFiles constructPCVMGeneralFiles(String keyId,
                                                         String encVersion);

  Map<String, String> fetchNetworkUuidVmUuidMapFromGroups(
      final List<String> vmUuidList, String ip) throws PCResilienceException;
  boolean isHostingPEHyperV(String hostingPeUuid)
    throws PCResilienceException;
  List<String> getPeUuidsOnPCClusterExternalState() throws PCResilienceException;
  String getHostingPEUuid() throws PCResilienceException;

  PCVMBackupTargets fetchPcvmBackupTargets() throws PCResilienceException;
  PCVMBackupTargets.ObjectStoreBackupTarget getObjectStoreBackupTargetByEntityId(PCVMBackupTargets pcvmBackupTargets, String entityId) throws PCResilienceException;

  Map<String, ObjectStoreEndPointDto> getObjectStoreEndpointDtoMapFromObjectStoreCredentialsBackupEntity(
      ObjectStoreCredentialsBackupEntity objectStoreCredentialsBackupEntity)
      throws PCResilienceException;

  Map<String, ObjectStoreEndPointDto> getObjectStoreEndpointDtoMapFromPcObjectStoreEndpointList(
      List<PcObjectStoreEndpoint> pcObjectStoreEndpointList)
    throws PCResilienceException;

  ObjectStoreEndPointDto getBasicObjectStoreEndpointDtoFromPcObjectStore(
      PcObjectStoreEndpoint pcObjectStoreEndpoint);

  ObjectStoreEndPointDto getObjectStoreEndpointDtoFromPcObjectStoreEndpoint(
      PcObjectStoreEndpoint pcObjectStoreEndpoint) throws PCResilienceException;

}
