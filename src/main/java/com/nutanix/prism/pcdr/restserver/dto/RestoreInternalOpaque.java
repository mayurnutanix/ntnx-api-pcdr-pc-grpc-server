package com.nutanix.prism.pcdr.restserver.dto;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;

/**
 * An internal Opaque class to be stored in Root recovery task for the Replica
 * PE IPs.
 */
@Data
@Slf4j
@JsonDeserialize
@Setter
@Getter
public class RestoreInternalOpaque implements Serializable {
  private List<String> peIps;
  private String peUuid;
  private PcObjectStoreEndpoint objectStoreEndpoint;
  private String credentialKeyId;
  private String backupUuid;
  private String certificatePath;
  private boolean isPathStyle;
}
