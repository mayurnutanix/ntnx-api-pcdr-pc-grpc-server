package com.nutanix.prism.pcdr.restserver.dto;

import lombok.Data;

@Data
public class BackupEntity {
  private String entityName;
  private boolean isEnableReplicationFromNDFS;

  public BackupEntity(String entityName,
                      boolean isEnableReplicationFromNDFS) {
    setEntityName(entityName);
    setEnableReplicationFromNDFS(isEnableReplicationFromNDFS);
  }
}
