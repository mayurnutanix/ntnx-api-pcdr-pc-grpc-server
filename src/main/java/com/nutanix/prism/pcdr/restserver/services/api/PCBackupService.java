package com.nutanix.prism.pcdr.restserver.services.api;

import com.google.protobuf.ByteString;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.insights.ifc.InsightsInterfaceProto.BatchUpdateEntitiesArg;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto;
import com.nutanix.prism.exception.EntityDbException;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import dp1.pri.prism.v4.management.BackupTarget;
import dp1.pri.prism.v4.protectpc.*;
import dp1.pri.prism.v4.protectpc.RpoConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;


public interface PCBackupService {
  EligibleClusterList getEligibleClusterList(boolean isSortingRequired) throws InsightsInterfaceException, PCResilienceException;

  InsightsInterfaceProto.GetEntitiesWithMetricsRet fetchPEClusterListFromIDF()
          throws PCResilienceException;
  void addReplicas(final Set<String> peClusterUuidSet,
                   List<PcObjectStoreEndpoint> objectStoreEndpointList,
                   ByteString task)
          throws PCResilienceException, ErgonException;

  void addReplicas(final Set<String> peClusterUuidSet,
                   List<PcObjectStoreEndpoint> objectStoreEndpointList)
      throws PCResilienceException;

  void removeReplica(String backupTargetID)
          throws PCResilienceException;

  void removeReplica(String backupTargetID, ByteString taskId)
          throws PCResilienceException;

  BackupTargetsInfo getReplicas() throws PCResilienceException;

  void addUpdateEntitiesArgForPCBackupSpecs(
      BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg)
      throws InsightsInterfaceException, PCResilienceException;

  void addUpdateEntitiesArgForPCBackupMetadata(
      BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg,
      List<String> replicaPEUuids,
      List<PcObjectStoreEndpoint> objectStoreEndpointList,
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
      final boolean canSkipSpecs,
      final boolean updateSyncTime) throws PCResilienceException;

  void addUpdateEntitiesArgForPCZkData(
      BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg);

  void generateEndpointAddress(PcObjectStoreEndpoint pcObjectStoreEndpoint,
                               String pcClusterUuid) throws PCResilienceException;

  boolean makeBatchUpdateRPC(BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg);
  void addUpdateEntityArgsForPCBackupConfig(
      final Set<String> peClusterUuidSet,
      List<PcObjectStoreEndpoint> objectStoreEndpointList,
      Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
      Map<String,String> credentialKeyIdMap,
      final BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg);
  void deletePcBackupMetadataProtoEntity() throws PCResilienceException;
  void deleteStaleZkNodeValuesFromIDF(
      BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg)
      throws PCResilienceException;

  void resetBackupSyncStates() throws PCResilienceException;

  void updateRpo(String entityId, RpoConfig rpoConfig)
          throws PCResilienceException;

  void updateCredentials(String entityId, PcEndpointCredentials pcEndpointCredentials) throws PCResilienceException;

  void createBackupTarget(ByteString taskId, BackupTargets backupTargets) throws ErgonException, PCResilienceException;

  void updateBackupTarget(ByteString taskId, BackupTargets backupTargets, String extId) throws ErgonException, PCResilienceException;

  public String writeSecret(String bucketName,
                            PcEndpointCredentials pcEndpointCredentials) throws PCResilienceException;

  ClusterExternalStateProto.PcBackupConfig getBackupTargetById(String backupTargetID) throws PCResilienceException,
                                                                                             EntityDbException.EntityNotExistException;

  void isBackupTargetEligibleForBackup(Set<String> peClusterUuidSet,
                                       List<PcObjectStoreEndpoint> pcObjectStoreEndpointList) throws PCResilienceException;

  void removeCredentialKeyFromMantle(String credentialKeyId);

  String generateEtag(BackupTarget backupTarget) throws PCResilienceException ;
}