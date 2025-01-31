package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.CleanIdfOnPC;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.CleanIdfOnPE;
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
public class CleanIdfStaleValuesSubtask extends AbstractPcdrSubtaskHandler {

  private final CleanIdfOnPE cleanIdfOnPE;
  private final CleanIdfOnPC cleanIdfOnPC;
  private final ErgonServiceHelper ergonServiceHelper;

  @Getter
  private final TaskConstants.TaskOperationType taskOperationType;

  private final List<PcdrStepsHandler> pcdrStepsHandlers =
    new ArrayList<>();

  @Autowired
  public CleanIdfStaleValuesSubtask(CleanIdfOnPE cleanIdfOnPE,
                                    CleanIdfOnPC cleanIdfOnPC,
                                    ErgonServiceHelper ergonServiceHelper,
                                    ZookeeperServiceHelperPc zookeeperServiceHelper) {
    super(zookeeperServiceHelper);
    this.cleanIdfOnPC = cleanIdfOnPC;
    this.cleanIdfOnPE = cleanIdfOnPE;
    this.ergonServiceHelper = ergonServiceHelper;
    taskOperationType = TaskConstants.TaskOperationType.kCleanIDF;
  }

  @PostConstruct
  public void init() {
    pcdrStepsHandlers.add(cleanIdfOnPE);
    pcdrStepsHandlers.add(cleanIdfOnPC);
  }

  /**
   * Clean IDF subtask calls the cleanIDF API call on replica PE, and creates
   * clusterdatastate clean task as independent task.
   * @param rootTask - Root task for PC restore.
   * @return - returns updated root task
   * @throws PCResilienceException - can throw PC restore task exception
   */
  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    // STEP 3: Clean IDF table on replica PE and show an alert if the
    // replica PE is the new hosting PE for the PC.
    int stepNum = (int) rootTask.getStepsCompleted() + 1;
    log.info("SUBTASK {}: Starting clean IDF subtask.", stepNum);
    try {
      while(currentSubtask.getStepsCompleted() < pcdrStepsHandlers.size()) {
        int currentStep = (int) currentSubtask.getStepsCompleted();
        currentSubtask = pcdrStepsHandlers.get(currentStep)
                                          .execute(rootTask, currentSubtask);
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
                                          exceptionDetails, ErrorArgumentKey.CLEAN_IDF_AFTER_RESTORE_OPERATION);
      log.error("The cleanIDF task failed with error: ", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    ergonServiceHelper.updateTaskStatus(currentSubtask,
      ErgonTypes.Task.Status.kSucceeded);
    // Update the status of the Root task.
    log.info("Clean IDF subtask succeeded. Task {} " +
      "successful", stepNum);
    rootTask = ergonServiceHelper.updateTask(rootTask,
                                             stepNum,
                                             TaskConstants.CLEAN_IDF_PERCENTAGE);
    log.info("SUBTASK {}: Clean IDF subtask ended.", stepNum);
    return rootTask;
  }
}