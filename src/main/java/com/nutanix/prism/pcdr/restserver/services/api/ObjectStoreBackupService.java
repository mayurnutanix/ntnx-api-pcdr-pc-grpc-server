/*
 * Copyright (c) 2022 Nutanix Inc. All rights reserved.
 *
 * Author: kapil.tekwani@nutanix.com
 */
package com.nutanix.prism.pcdr.restserver.services.api;

import com.nutanix.dp1.pri.prism.v4.recoverpc.OriginalHostingPeInfo;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PcInfo;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PcSeedData;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.EntityDBProxy;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;

import java.util.List;
import java.util.Map;

public interface ObjectStoreBackupService {

  PcInfo fetchPcInfoData(String pcClusterUuid)
      throws PCResilienceException;

  OriginalHostingPeInfo populateOriginalHostingPeInfo(String pcClusterUuid)
      throws PCResilienceException;

  PcSeedData populateBackupSeedData(String pcClusterUuid)
      throws PCResilienceException;

  List<String> populatePcIpAddresses(PcInfo pcInfo)
    throws PCResilienceException;

  void initiateBackupToObjectStore(Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoMap,
                                   String pcClusterUuid,
                                   EntityDBProxy entityDBProxy,
                                   List<PcObjectStoreEndpoint> objectStoreEndpointList);
  void initiateBackupToObjectStoreAsync(Map<String, ObjectStoreEndPointDto> objectStoreEndPointDtoList,
                                        String pcClusterUuid,
                                        EntityDBProxy entityDBProxy,
                                        List<PcObjectStoreEndpoint> objectStoreEndpointList);
  void deletePcSeedDataFromInsightsDb(String pcClusterUuid);
}
