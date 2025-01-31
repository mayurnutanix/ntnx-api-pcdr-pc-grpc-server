package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.ResetArithmosSync;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.WaitForOtherServices;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class ResetArithmosSyncSubTask extends AbstractPcdrSubtaskHandler {

  private final ErgonServiceHelper ergonServiceHelper;
  private final WaitForOtherServices waitForOtherServices;
  private final ResetArithmosSync resetArithmosSync;


  @Getter
  private final TaskConstants.TaskOperationType taskOperationType;

  private final List<PcdrStepsHandler> pcdrStepsHandlers = new ArrayList<>();

  @PostConstruct
  public void init() {
    pcdrStepsHandlers.add(waitForOtherServices);
    pcdrStepsHandlers.add(resetArithmosSync);
  }


  @Autowired
  public ResetArithmosSyncSubTask(ErgonServiceHelper ergonServiceHelper,
                                  WaitForOtherServices waitForOtherServices,
                                  ResetArithmosSync resetArithmosSync,
                                  ZookeeperServiceHelperPc zookeeperServiceHelper) {
    super(zookeeperServiceHelper);
    this.ergonServiceHelper = ergonServiceHelper;
    this.waitForOtherServices = waitForOtherServices;
    this.resetArithmosSync = resetArithmosSync;
    this.taskOperationType = TaskConstants.TaskOperationType.kResetArithmosSync;
  }


  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask, ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {

    int stepNum = (int) rootTask.getStepsCompleted() + 1;
    log.info("SUBTASK {}: Starting Reset Arithmos Sync subtask." , stepNum);
    try {
      while (currentSubtask.getStepsCompleted() < pcdrStepsHandlers.size()) {
        int currentStep = (int) currentSubtask.getStepsCompleted();
        currentSubtask = pcdrStepsHandlers.get(currentStep).execute(rootTask , currentSubtask);
        // If current subtask is null that means we want the scheduler to check
        // the status of task in the next go and update the status.
        if (currentSubtask == null) {
          return null;
        }

      }
      createSucceededZkNode();
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ergonServiceHelper.updateTaskStatus(currentSubtask.getUuid(), ErgonTypes.Task.Status.kFailed,
                                          exceptionDetails, ErrorArgumentKey.RESET_ARITHMOS_SYNC_OPERATION);
      log.error("Reset Arithmos Sync subtask failed with error: " , e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    ergonServiceHelper.updateTaskStatus(currentSubtask , ErgonTypes.Task.Status.kSucceeded);
    // Update the status of the Root task.
    log.info("Reset Arithmos Sync subtask succeeded. Task {} " + "successful" , stepNum);
    rootTask = ergonServiceHelper.updateTask(rootTask , stepNum,
                                             TaskConstants.RESET_ARITHMOS_SYNC_TASK_PERCENTAGE);
    log.info("SUBTASK {}: Reset Arithmos Sync subtask ended." , stepNum);
    return rootTask;
  }
}