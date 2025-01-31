package com.nutanix.prism.pcdr.restserver.dto;

import com.nutanix.authn.AuthnProto;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * An internal Opaque class to be stored in Restore Data subtask.
 */
@Data
@Setter
@Getter
public class RestoreDataInternalOpaque implements Serializable {
  // For future compatibility of having multiple hosting PEs for a given PC,
  // we are creating a list.
  private List<AuthnProto.CACert> hostingPesCACert;
}