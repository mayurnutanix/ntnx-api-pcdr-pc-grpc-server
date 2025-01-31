package com.nutanix.prism.pcdr.restserver.services.api;

import com.nutanix.prism.pcdr.exceptions.PCResilienceException;

import java.util.List;

public interface RestorePEService {
  void cleanIdfOnRegisteredPEs() throws PCResilienceException;
  void setPeUuidsInPCDRZkNode(List<String> peUuids);
  List<String> getPeUuidsInPCDRZkNode();
}

