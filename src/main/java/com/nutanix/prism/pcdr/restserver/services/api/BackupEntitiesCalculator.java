package com.nutanix.prism.pcdr.restserver.services.api;

import com.nutanix.prism.pcdr.exceptions.EntityCalculationException;
import com.nutanix.prism.pcdr.restserver.dto.BackupEntity;
import dp1.pri.prism.v4.protectpc.EligibleCluster;

import java.util.List;

public interface BackupEntitiesCalculator {
  void updateBackupEntityList() throws EntityCalculationException;
  List<EligibleCluster> getBackupLimitExceededPes(List<String> replicaPEUuids)
    throws EntityCalculationException;
  List<BackupEntity> getBackupEntityList();
  long getTotalBackupEntityCount() throws EntityCalculationException;
  long getMaxBackupEntitiesSupportedForAOS(String aosVersion,
                                           long numNodes);
}
