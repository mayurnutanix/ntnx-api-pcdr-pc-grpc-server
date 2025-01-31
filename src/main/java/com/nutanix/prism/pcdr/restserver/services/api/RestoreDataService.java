package com.nutanix.prism.pcdr.restserver.services.api;

import com.google.protobuf.InvalidProtocolBufferException;
import com.nutanix.authn.AuthnProto;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PCRestoreData;
import com.nutanix.prism.exception.GenericException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import dp1.pri.prism.v4.protectpc.PcvmRestoreFiles;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;

public interface RestoreDataService {
  void restorePCDataFromIDF() throws PCResilienceException;
  void restoreTrustDataOnPC(PCRestoreData pcRestoreData)
    throws IOException, InterruptedException, ExecutionException, PCResilienceException;
  void restorePCVMFiles(PcvmRestoreFiles pcvmRestoreFiles)
    throws IOException, InterruptedException;
  boolean hasTaskExceededWaitTime(LocalDateTime reconcileTimeStart);
  void restoreReplicaDataOnPC(RestoreInternalOpaque restoreInternalOpaque)
    throws PCResilienceException;
  void createOrUpdateZkNode(String nodePath, byte[] nodeBytes)
      throws IOException, PCResilienceException;
  void stopServicesUsingGenesis(List<String> servicesToStop)
      throws PCResilienceException, ExecutionException, InterruptedException;
  void removePreviousHostingPETrust() throws PCResilienceException;
  AuthnProto.CACert getCACertificates(String peUuid)
      throws GenericException, InvalidProtocolBufferException;
  boolean addTrustCertificates(AuthnProto.CACert caCert)
      throws GenericException, PCResilienceException;
  void disableConfiguredServicesOnGenesisStartup(String zkPath) throws PCResilienceException;
}
