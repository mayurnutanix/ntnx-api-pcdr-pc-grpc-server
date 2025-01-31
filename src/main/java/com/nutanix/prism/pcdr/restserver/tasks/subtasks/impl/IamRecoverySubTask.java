package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.WaitForIamRestore;
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

// This class deals with recovery of IAM V2
public class IamRecoverySubTask extends AbstractPcdrSubtaskHandler {

  private final WaitForIamRestore waitForIamRestore;

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Getter
  private final TaskConstants.TaskOperationType taskOperationType;

  private final List<PcdrStepsHandler> pcdrStepsHandlers = new ArrayList<>();

  @PostConstruct
  public void init() {
    pcdrStepsHandlers.add(waitForIamRestore);
  }

  @Autowired
  public IamRecoverySubTask(ErgonServiceHelper ergonServiceHelper , WaitForIamRestore waitForIamRestore ,
                            ZookeeperServiceHelperPc zookeeperServiceHelper) {
    super(zookeeperServiceHelper);
    this.ergonServiceHelper = ergonServiceHelper;
    this.waitForIamRestore = waitForIamRestore;
    this.taskOperationType = TaskConstants.TaskOperationType.kIamRestore;
  }

  /**
   * IAM Recovery subtask checks for existense of IAM Restore zknode
   * on path "appliance/logical/iam_flags/iam_v2_enabled"
   * @param rootTask - Root task for PC restore.
   * @param currentSubtask - Current subtask executing IAM recovery
   * @return - returns updated root task
   * @throws ErgonException - can throw Ergon task exception
   * @throws PCResilienceException - can throw PC restore task exception
   */
  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask , ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int stepNum = (int) rootTask.getStepsCompleted() + 1;
    log.info("SUBTASK {}: Starting IAM Recovery subtask." , stepNum);
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
      ergonServiceHelper.updateTaskStatus(currentSubtask.getUuid() , ErgonTypes.Task.Status.kFailed ,
                                          exceptionDetails, ErrorArgumentKey.IAM_RECOVERY_OPERATION);
      log.error("IAM Recovery subtask failed with error: " , e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    ergonServiceHelper.updateTaskStatus(currentSubtask , ErgonTypes.Task.Status.kSucceeded);
    // Update the status of the Root task.
    log.info("IAM Recovery subtask succeeded. Task {} " + "successful" , stepNum);
    rootTask = ergonServiceHelper.updateTask(rootTask , stepNum ,
                                             TaskConstants.IAM_RESTORE_PERCENTAGE);
    log.info("SUBTASK {}: IAM Recovery subtask ended." , stepNum);
    return rootTask;
  }

  @Override
  public TaskConstants.TaskOperationType getTaskOperationType() {
    return this.taskOperationType;
  }
}
