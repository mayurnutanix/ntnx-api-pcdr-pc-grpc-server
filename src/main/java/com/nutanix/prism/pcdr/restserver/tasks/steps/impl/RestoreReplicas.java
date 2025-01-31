package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.util.Objects;

@Slf4j
@Service
public class RestoreReplicas implements PcdrStepsHandler {

  @Autowired
  private RestoreDataService restoreDataService;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Restoring replicas in pc_backup_config.", currentStep);
    try {
      // Start the backup using our scheduler.
      RestoreInternalOpaque restoreInternalOpaque =
          (RestoreInternalOpaque) Objects.requireNonNull(SerializationUtils.
                                                             deserialize(rootTask.getInternalOpaque().toByteArray()));
      restoreDataService.restoreReplicaDataOnPC(restoreInternalOpaque);
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      log.error("Unable to restore replica data on PC due" +
                " to following - :", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    log.info("STEP {}: Successfully restored replicas in pc_backup_config.",
             currentStep);
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        TaskConstants.RESTORE_REPLICAS_STEP_PERCENTAGE);
    return currentSubtask;
  }
}
