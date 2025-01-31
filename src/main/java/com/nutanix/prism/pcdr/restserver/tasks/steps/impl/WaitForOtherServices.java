package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.util.PcdrStepUtil;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class WaitForOtherServices implements PcdrStepsHandler {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Value("${prism.pcdr.reconcile.wait.duration.millis:300000}")
  private int reconcileWaitDuration;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    // STEP: Wait for the services to reconcile the PC data. When the
    // wait is over complete this step.
    log.info("STEP {}: Waiting for other services to reconcile their " +
             "data.", currentStep);

    boolean isWaitDone = PcdrStepUtil
        .hasStepExceededWaitTime(currentSubtask.getDisplayName(),
                                 currentSubtask.getStartTimeUsecs() / 1000,
                                 reconcileWaitDuration);
    if (!isWaitDone)
      return null;
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        TaskConstants.RECONCILE_WAIT_PERCENTAGE);
    log.info("STEP {}: Successfully completed the wait.", currentStep);
    return currentSubtask;
  }
}
