package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.dp1.pri.prism.v4.recoverpc.PCRestoreData;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

@Service
@Slf4j
public class RestoreTrustDataOnPC implements PcdrStepsHandler {

  @Autowired
  RestoreDataService restoreDataService;
  @Autowired
  ErgonServiceHelper ergonServiceHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    // STEP: Retrieve the trust data from the
    // restoreBasicTrustSubtask and write the data at the appropriate
    // locations.
    log.info("STEP {}: Retrieving trust data from subtask internal " +
      "opaque and writing it at appropriate location on PC.", currentStep);
    log.debug("Decoding internal opaque in subtask: {}",
      currentSubtask.getInternalOpaque().toString());
    PCRestoreData pcRestoreData =
      (PCRestoreData) SerializationUtils.deserialize(
        currentSubtask.getInternalOpaque().toByteArray());
    try {
      restoreDataService.restoreTrustDataOnPC(pcRestoreData);
    } catch (Exception e) {
      log.error("The following exception encountered while restoring trust " +
        "data on PC - ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(),exceptionDetails.getArguments());
    }
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        TaskConstants.WRITE_TRUST_DATA_PERCENTAGE);
    log.info("STEP {}: Successfully completed restore basic trust Step.",
             currentStep);
    return currentSubtask;
  }
}