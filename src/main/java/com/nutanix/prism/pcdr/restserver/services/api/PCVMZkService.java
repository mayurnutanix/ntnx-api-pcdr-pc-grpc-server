package com.nutanix.prism.pcdr.restserver.services.api;

import com.nutanix.insights.ifc.InsightsInterfaceProto.BatchUpdateEntitiesArg;

/**
 * PCVMZKService is used to add backup data to PC Backup in IDF.
 */
public interface PCVMZkService {
  void addUpdateEntitiesArgForPCZkData(
    BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArgBuilder);
  void updateEntitiesArgListForZkNodeData(
    BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArgBuilder,
    final String znode);
}
