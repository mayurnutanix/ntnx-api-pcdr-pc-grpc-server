package com.nutanix.prism.pcdr.restserver.services.api;

import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;

import java.util.List;
import java.util.Set;

public interface PulsePublisherService{
  void insertReplicaData(Set<String> clusterUuid,
                         List<PcObjectStoreEndpoint> objectStoreEndpointList);
  void removeReplicaData(String replicaEntityId);
  void sendMetricDataToPulse();
  void updatePauseBackupStatus(
      List<ClusterExternalStateProto.PcBackupConfig> pcBackupConfigs);
}
