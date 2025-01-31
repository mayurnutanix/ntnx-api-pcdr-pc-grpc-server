/**
 * Author @vipul.gupta@nutanix.com - October 2022
 */
package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.google.protobuf.ByteString;
import com.nutanix.authn.AuthnProto;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.dto.RestoreDataInternalOpaque;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.STORE_HOSTING_PE_CA_CERT_STEP_PERCENTAGE;

@Slf4j
@Service
public class StoreNewHostingPECACert implements PcdrStepsHandler {

  private final ErgonServiceHelper ergonServiceHelper;
  private final RestoreDataService restoreDataService;
  private final PCVMDataService pcvmDataService;

  @Autowired
  public StoreNewHostingPECACert(InstanceServiceFactory instanceServiceFactory,
                                 ErgonServiceHelper ergonServiceHelper,
                                 RestoreDataService restoreDataService, PCVMDataService pcvmDataService) {
    this.ergonServiceHelper = ergonServiceHelper;
    this.restoreDataService = restoreDataService;
    this.pcvmDataService = pcvmDataService;
  }

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Store the CA certs for hosting PE, as part of Ergon " +
             "restore data subtask.", currentStep);
    try {
      // Creates a new RestoreDataInternalOpaque object to store the CA Cert
      // for the current i.e. new hosting PEs.
      RestoreDataInternalOpaque restoreDataInternalOpaque =
          new RestoreDataInternalOpaque();
      List<AuthnProto.CACert> newHostingPeCertList =
          new ArrayList<>();
      // fetching the Hosting PE Information from Trust zkNode
      String hostingPE = pcvmDataService.getHostingPEUuid();
      newHostingPeCertList.add(
          restoreDataService.getCACertificates(hostingPE));
      restoreDataInternalOpaque.setHostingPesCACert(newHostingPeCertList);
      byte[] serializedOpaque =
          SerializationUtils.serialize(restoreDataInternalOpaque);
      // Update the task step and internal opaque value.
      currentSubtask = ergonServiceHelper.updateTask(
          currentSubtask, currentStep,
          ByteString.copyFrom(Objects.requireNonNull(
              serializedOpaque)),
          STORE_HOSTING_PE_CA_CERT_STEP_PERCENTAGE);
    }
    catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      log.error("Error encountered while storing new hosting PE CA cert in " +
                "Ergon task internal Opaque.", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    log.info("STEP {}: Successfully Stored the CA certs for hosting PE, as " +
             "part of Ergon restore Data subtask.", currentStep);
    return currentSubtask;
  }
}
