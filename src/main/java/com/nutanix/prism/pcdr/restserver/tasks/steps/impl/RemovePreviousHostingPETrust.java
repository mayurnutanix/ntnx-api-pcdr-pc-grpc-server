package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RemovePreviousHostingPETrust implements PcdrStepsHandler {
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Autowired
  private RestoreDataService restoreDataService;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;

    log.info("STEP {}: Removing original hosting PE " +
             "root certs from recovered PC trust-store", currentStep);

    try {
      // Invoke remove PE Trust task to remove old Hosting PE certs from recovered PC
      restoreDataService.removePreviousHostingPETrust();
    } catch (Exception e) {
      // If exception occurs while removing PE certs, just log the error and move ahead.
      // Don't throw any exception for this failure
      log.error("Failed to remove old PE root certs from recovered PC trust-store due to error:", e);
    }
    // if Remove PE Trust task is successful
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep, TaskConstants.REMOVE_PE_TRUST_PERCENTAGE);
    log.info("STEP {}: Successfully completed. Removed original hosting PE " +
             "root certs from recovered PC trust-store.",
             currentStep);
    return currentSubtask;
  }
}
