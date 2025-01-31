package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.DeleteServicesZkNode;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.EnableCmspBasedServices;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.RestartServicesOnPC;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class RestartServicesSubtask extends AbstractPcdrSubtaskHandler {

  private final ErgonServiceHelper ergonServiceHelper;
  private final PCDRYamlAdapterImpl pcdrYamlAdapter;
  private final DeleteServicesZkNode deleteServicesZkNode;
  private final EnableCmspBasedServices enableCmspBasedServices;
  private final RestartServicesOnPC restartServicesOnPC;

  @Getter
  private final TaskConstants.TaskOperationType taskOperationType;

  private final List<PcdrStepsHandler> pcdrStepsHandlers = new ArrayList<>();

  @PostConstruct
  public void init() {
    restartServicesOnPC.setPercentageCompleted(
        TaskConstants.RESTART_SERVICES_AFTER_RESTORE);
    restartServicesOnPC.setServicesToStop(
        pcdrYamlAdapter.getServicesToRestartAfterRestoreData());
    pcdrStepsHandlers.add(deleteServicesZkNode);
    pcdrStepsHandlers.add(enableCmspBasedServices);
    pcdrStepsHandlers.add(restartServicesOnPC);
  }

  public RestartServicesSubtask(ErgonServiceHelper ergonServiceHelper,
                                PCDRYamlAdapterImpl pcdrYamlAdapter,
                                DeleteServicesZkNode deleteServicesZkNode,
                                EnableCmspBasedServices enableCmspBasedServices,
                                RestartServicesOnPC restartServicesOnPC,
                                ZookeeperServiceHelperPc zookeeperServiceHelper) {
    super(zookeeperServiceHelper);
    this.ergonServiceHelper = ergonServiceHelper;
    this.pcdrYamlAdapter = pcdrYamlAdapter;
    this.deleteServicesZkNode = deleteServicesZkNode;
    this.enableCmspBasedServices = enableCmspBasedServices;
    this.restartServicesOnPC = restartServicesOnPC;
    this.taskOperationType = TaskConstants.TaskOperationType.kRestartPcServices;
  }

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask , ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int stepNum = (int) rootTask.getStepsCompleted() + 1;
    log.info("SUBTASK {}: Starting 'Restart Services' subtask", stepNum);

    try {
      log.info("Starting 'Restart Services' subtask from completed step {}",
               currentSubtask.getStepsCompleted());

      while(currentSubtask.getStepsCompleted() < pcdrStepsHandlers.size()) {
        int currentStep = (int) currentSubtask.getStepsCompleted();
        currentSubtask = pcdrStepsHandlers.get(currentStep)
                                          .execute(rootTask, currentSubtask);
        if (currentSubtask == null) {
          // If currentSubtask is null that means we are still
          // waiting for the reconciliation default time. So, return
          // and let the scheduler run next time to check for it.
          return null;
        }
      }
      createSucceededZkNode();
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ergonServiceHelper.updateTaskStatus(currentSubtask.getUuid(), ErgonTypes.Task.Status.kFailed,
                                          exceptionDetails, ErrorArgumentKey.RESTART_SERVICES_OPERATION);
      log.error("The 'Restart Services' subtask failed with error: ", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    ergonServiceHelper.updateTaskStatus(currentSubtask,
                                        ErgonTypes.Task.Status.kSucceeded);
    log.info("'Restart Services' task succeeded. Task {} " +
             "successful", stepNum);
    rootTask = ergonServiceHelper.updateTask(rootTask,
                                             stepNum,
                                             TaskConstants.RESTART_SERVICES_PERCENTAGE);
    log.info("SUBTASK {}: 'Restart Services' subtask ended.", stepNum);
    return rootTask;
  }

  @Override
  public TaskConstants.TaskOperationType getTaskOperationType() {
    return taskOperationType;
  }
}
