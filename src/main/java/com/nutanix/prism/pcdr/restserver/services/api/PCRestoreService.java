package com.nutanix.prism.pcdr.restserver.services.api;

import com.google.protobuf.ByteString;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import dp1.pri.prism.v4.protectpc.RecoveryStatus;
import dp1.pri.prism.v4.protectpc.ReplicaInfo;
import org.springframework.security.core.Authentication;

import java.util.List;

public interface PCRestoreService {
  ByteString restore(ReplicaInfo replicaInfo, Authentication authentication)
      throws PCResilienceException, ErgonException;
  RecoveryStatus getRecoveryStatus()
      throws PCResilienceException, ErgonException;
  void executePCRestoreTasks()
      throws PCResilienceException, ErgonException;
  void stopClusterServices(List<String> stopServicesList);
}
