package com.nutanix.prism.pcdr.restserver.dto;

/*
 * Copyright (c) 2023 Nutanix Inc. All rights reserved.
 *
 * Author: mayur.ramavat@nutanix.com
 */

import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ObjectStoreCredentialsBackupEntity {
  private List<PcObjectStoreEndpoint> objectStoreEndpoints;
  private Map<String, String> credentialsKeyIdMap;
  private Map<String, String> certificatePathMap;
}

