package com.nutanix.prism.pcdr.restserver.services.api;

import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.PcBackupConfig;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;

import java.util.List;

public interface BackupStatus {
  void setBackupStatusForReplicaPEs(List<String>replicaPEUuids,
                                    boolean pauseBackup,
                                    String pauseBackupMessage)
      throws InsightsInterfaceException, PCResilienceException;
  void setBackupStatusForAllReplicaPEsWithRetries(List<String>replicaPEUuids,
                                                  boolean pauseBackup,
                                                  String pauseBackupMessage)
      throws InsightsInterfaceException, PCResilienceException;
  void setBackupStatusForAllObjectStoreReplicasWithRetries(
          List<PcObjectStoreEndpoint> objectStoreEndpointInPCBackupConfig,
          boolean pauseBackup,
          String pauseBackupMessage)
     throws InsightsInterfaceException, PCResilienceException;
  public void updateReplicaPEsPauseBackupStatus()
      throws InsightsInterfaceException, PCResilienceException;
  List<PcBackupConfig> fetchPcBackupConfigProtoListForPEReplicas()
      throws PCResilienceException;

  List<PcBackupConfig> fetchPcBackupConfigProtoList() throws PCResilienceException;

  List<InsightsInterfaceProto.UpdateEntityArg> createUpdateEntityArgListForObjectStoreReplicas(
      List<PcBackupConfig> pcBackupConfigs);
}
