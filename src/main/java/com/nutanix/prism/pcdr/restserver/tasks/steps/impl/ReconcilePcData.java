package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class ReconcilePcData implements PcdrStepsHandler {

  @Autowired
  private PCVMDataService pcvmDataService;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Verification of Prism data started.", currentStep);
    //Currently no verification as part of reconciliation
    // [ENG-535050] Removed rc table reconciliation as part of
    //  pe-pc remote connection deprecation
    currentSubtask = ergonServiceHelper.updateTask(
            currentSubtask, currentStep,
            TaskConstants.VERIFY_PRISM_DATA_PERCENTAGE);
    log.info("STEP {}: Successfully verified Prism data.", currentStep);
    return currentSubtask;
  }
}
