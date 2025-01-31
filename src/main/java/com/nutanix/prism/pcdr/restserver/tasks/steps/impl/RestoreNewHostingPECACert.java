/**
 * Author @vipul.gupta@nutanix.com - October 2022
 */
package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.google.protobuf.ByteString;
import com.nutanix.authn.AuthnProto;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.restserver.dto.RestoreDataInternalOpaque;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.SerializationUtils;

import java.util.Objects;

import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.RESTORE_HOSTING_PE_CA_CERT_STEP_PERCENTAGE;

@Slf4j
@Service
public class RestoreNewHostingPECACert implements PcdrStepsHandler {
  private final ErgonServiceHelper ergonServiceHelper;
  private final RestoreDataService restoreDataService;

  @Autowired
  public RestoreNewHostingPECACert(ErgonServiceHelper ergonServiceHelper,
                                   RestoreDataService restoreDataService) {
    this.ergonServiceHelper = ergonServiceHelper;
    this.restoreDataService = restoreDataService;
  }

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Restore the CA certs for new hosting PE," +
             " from restore data Ergon subtask.", currentStep);
    try {
      // Retrieves the current restore data internal opaque object and
      // deserialize it.
      RestoreDataInternalOpaque restoreDataInternalOpaque =
          (RestoreDataInternalOpaque) SerializationUtils.deserialize(
              Objects.requireNonNull(
                  currentSubtask.getInternalOpaque().toByteArray()));
      // Loops for all the hosting PEs and add the ca cert to the Prism central.
      // In case the restoreDataInternalOpaque is empty raises an exception,
      // as that is not possible i.e. some data corruption might be there.
      if (!ObjectUtils.isEmpty(restoreDataInternalOpaque)) {
        for (AuthnProto.CACert peCaCert :
            restoreDataInternalOpaque.getHostingPesCACert()) {
          if (restoreDataService.addTrustCertificates(peCaCert)) {
            log.info("Added trust certificate for Prism Element {} as part of" +
                     " ca.pem", peCaCert.getOwnerClusterUuid());
          } else {
            log.error("Unable to add trust certificates for Prism Element " +
                      "cluster {}", peCaCert.getOwnerClusterUuid());
            throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
          }
        }
        // Updates the current step and empties the internal opaque to save
        // space in Ergon tasks.
        currentSubtask = ergonServiceHelper.updateTask(
            currentSubtask, currentStep,
            ByteString.EMPTY,
            RESTORE_HOSTING_PE_CA_CERT_STEP_PERCENTAGE);
      } else {
        // Logs and throw the error.
        log.error("The serialised restore data internal opaque object is " +
                  "either null or empty {}.", restoreDataInternalOpaque);
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
    } catch (Exception e) {
      // In case of any exception, log it and throw it. As part of the
      // subtask code, the subtask will be marked as failed.
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      log.error("Error encountered while restoring new hosting Prism Element " +
                "certs on the Prism Central.", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    log.info("STEP {}: Successfully restored the CA certs for new hosting PE," +
             " from restore data present as part of Ergon subtask.",
             currentStep);
    // Returns the latest subtask values for the next step.
    return currentSubtask;
  }
}
