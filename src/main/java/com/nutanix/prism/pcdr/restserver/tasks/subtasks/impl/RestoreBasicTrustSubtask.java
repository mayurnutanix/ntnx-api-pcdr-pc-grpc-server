package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;


import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.FetchTrustDataFromPE;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.RestartServicesOnPC;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.RestoreTrustDataOnPC;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.SerializationUtils;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.BASIC_TRUST_ALL_STEPS_COMPLETE_PERCENTAGE;
import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.RESTART_SERVICES_AFTER_TRUST_DATA_PERCENTAGE;

@Slf4j
@Service
public class RestoreBasicTrustSubtask extends AbstractPcdrSubtaskHandler {

  private final FetchTrustDataFromPE fetchTrustDataFromPE;
  private final RestoreTrustDataOnPC restoreTrustDataOnPC;
  private final RestartServicesOnPC restartServicesOnPC;
  private final ErgonServiceHelper ergonServiceHelper;
  private final PCDRYamlAdapterImpl pcdrYamlAdapter;

  @Getter
  private final TaskConstants.TaskOperationType taskOperationType;

  private final List<PcdrStepsHandler> pcdrStepsHandlers =
    new ArrayList<>();

  @Autowired
  public RestoreBasicTrustSubtask(PCDRYamlAdapterImpl pcdrYamlAdapter,
                                  ErgonServiceHelper ergonServiceHelper,
                                  RestartServicesOnPC restartServicesOnPC,
                                  RestoreTrustDataOnPC restoreTrustDataOnPC,
                                  FetchTrustDataFromPE fetchTrustDataFromPE,
                                  ZookeeperServiceHelperPc zookeeperServiceHelper) {
    super(zookeeperServiceHelper);
    this.pcdrYamlAdapter = pcdrYamlAdapter;
    this.ergonServiceHelper = ergonServiceHelper;
    this.restartServicesOnPC = restartServicesOnPC;
    this.restoreTrustDataOnPC = restoreTrustDataOnPC;
    this.fetchTrustDataFromPE = fetchTrustDataFromPE;
    taskOperationType = TaskConstants.TaskOperationType.kRestoreBasicTrust;
  }

  @PostConstruct
  public void init() {
    restartServicesOnPC.setPercentageCompleted(
        RESTART_SERVICES_AFTER_TRUST_DATA_PERCENTAGE);
    restartServicesOnPC.setServicesToStop(
        pcdrYamlAdapter.getServicesToRestartAfterBasicTrust());
    pcdrStepsHandlers.add(fetchTrustDataFromPE);
    pcdrStepsHandlers.add(restoreTrustDataOnPC);
    pcdrStepsHandlers.add(restartServicesOnPC);
  }

  /**
   * This is the subtask to restore the basic trust of the PC with the
   * replica PE. It fetches the clusterexternalstate and userrepository from
   * the PE and puts it on PC.
   * @param rootTask - Root PC restore task.
   * @return - Returns the updated root task with steps completed.
   */
  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {

    RestoreInternalOpaque restoreInternalOpaque =
       (RestoreInternalOpaque) Objects.requireNonNull(
               SerializationUtils.deserialize(
                       rootTask.getInternalOpaque().toByteArray()));
    int stepNum = (int) rootTask.getStepsCompleted() + 1;
    log.info("SUBTASK {}: Starting restore basic trust subtask.", stepNum);
    // If recovery source is ObjectStore then we immediately mark this subtask
    // as complete.
    if (!ObjectUtils.isEmpty(restoreInternalOpaque.getObjectStoreEndpoint())) {
        currentSubtask = ergonServiceHelper.updateTask(currentSubtask,
                pcdrStepsHandlers.size(),
                BASIC_TRUST_ALL_STEPS_COMPLETE_PERCENTAGE);
        ergonServiceHelper.updateTaskStatus(currentSubtask,
                ErgonTypes.Task.Status.kSucceeded);
        log.info("Restore from ObjectStore so skipping this subtask and " +
                 "marking it complete.");
        rootTask = ergonServiceHelper.updateTask(
            rootTask, stepNum,
            TaskConstants.BASIC_TRUST_PERCENTAGE);
        log.info("SUBTASK {}: Restore basic trust subtask ended.", stepNum);
        return rootTask;
    }
    // STEP 1 : Restore the basic trust by fetching
    // ClusterExternalState and UserRepository from PE3.
    // It creates a subtask for it having 3 steps and updates the
    // rootTask appropriately and returns it.
    try {
      log.info("Restore basic trust subtask starting from step {}",
        currentSubtask.getStepsCompleted());

      while(currentSubtask.getStepsCompleted() < pcdrStepsHandlers.size()) {
        int currentStep = (int) currentSubtask.getStepsCompleted();
        currentSubtask = pcdrStepsHandlers.get(currentStep)
                                          .execute(rootTask, currentSubtask);
      }
      createSucceededZkNode();
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ergonServiceHelper.updateTaskStatus(currentSubtask.getUuid(), ErgonTypes.Task.Status.kFailed,
                                          exceptionDetails, ErrorArgumentKey.RESTORE_BASIC_TRUST_OPERATION);
      log.error("Restore basic trust task failed with error: ", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    // When all the steps are complete, mark the status of the task as
    // succeeded. And update the parent root task.
    ergonServiceHelper.updateTaskStatus(currentSubtask,
                                        ErgonTypes.Task.Status.kSucceeded);
    log.info("Restore basic trust task succeeded. Task {} successful",
      stepNum);
    rootTask = ergonServiceHelper.updateTask(
      rootTask, stepNum,
      TaskConstants.BASIC_TRUST_PERCENTAGE);
    log.info("SUBTASK {}: Restore basic trust subtask ended.", stepNum);
    return rootTask;
  }
}