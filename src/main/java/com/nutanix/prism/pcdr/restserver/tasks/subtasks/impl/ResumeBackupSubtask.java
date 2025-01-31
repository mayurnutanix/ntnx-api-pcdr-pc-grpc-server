package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.RestoreReplicas;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.ResumeBackupScheduler;
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
public class ResumeBackupSubtask extends AbstractPcdrSubtaskHandler {

  private final ErgonServiceHelper ergonServiceHelper;
  private final RestoreReplicas restoreReplicas;
  private final ResumeBackupScheduler resumeBackupScheduler;

  @Getter
  private final TaskConstants.TaskOperationType taskOperationType;

  @Autowired
  public ResumeBackupSubtask(ErgonServiceHelper ergonServiceHelper,
                             RestoreReplicas restoreReplicas,
                             ResumeBackupScheduler resumeBackupScheduler,
                             ZookeeperServiceHelperPc zookeeperServiceHelper) {
    super(zookeeperServiceHelper);
    this.ergonServiceHelper = ergonServiceHelper;
    this.restoreReplicas = restoreReplicas;
    this.resumeBackupScheduler = resumeBackupScheduler;
    taskOperationType = TaskConstants.TaskOperationType.kResumeBackup;
  }

  private final List<PcdrStepsHandler> pcdrStepsHandlers =
      new ArrayList<>();

  @PostConstruct
  public void init() {
    pcdrStepsHandlers.add(restoreReplicas);
    pcdrStepsHandlers.add(resumeBackupScheduler);
  }

  /**
   * Resume backup subtask gets the pc_backup_metadata values and fills the
   * content of pc_backup_config.
   * @param rootTask - Root task for PC restore.
   * @return - returns updated root task
   * @throws PCResilienceException - can throw PC restore task exception
   */
  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int stepNum = (int) rootTask.getStepsCompleted() + 1;
    // Update the status of the Root task.
    log.info("SUBTASK {}: Resume backup on Prism central started.", stepNum);
    try {
      while(currentSubtask.getStepsCompleted() < pcdrStepsHandlers.size()) {
        int currentStep = (int) currentSubtask.getStepsCompleted();
        currentSubtask = pcdrStepsHandlers.get(currentStep)
                                          .execute(rootTask, currentSubtask);
      }
      createSucceededZkNode();
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ergonServiceHelper.updateTaskStatus(currentSubtask.getUuid(), ErgonTypes.Task.Status.kFailed,
                                          exceptionDetails, ErrorArgumentKey.RESUME_BACKUP_OPERATION);
      log.error("The resume backup task failed with error: ", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    ergonServiceHelper.updateTaskStatus(currentSubtask,
      ErgonTypes.Task.Status.kSucceeded);
    rootTask = ergonServiceHelper.updateTask(rootTask,
                                             stepNum,
                                             TaskConstants.RESUME_BACKUP_PERCENTAGE);
    // Update the status of the Root task.
    log.info("SUBTASK {}: Resume backup on Prism central succeeded.", stepNum);
    return rootTask;
  }
}