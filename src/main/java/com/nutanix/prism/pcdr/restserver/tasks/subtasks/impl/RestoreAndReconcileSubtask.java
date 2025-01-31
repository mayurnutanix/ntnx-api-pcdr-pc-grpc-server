package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.*;
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

@Slf4j
@Service
public class RestoreAndReconcileSubtask extends AbstractPcdrSubtaskHandler {

  private final RestorePcDataFromIdf restorePCDataFromIDF;
  private final ReconcilePcData reconcilePcData;
  private final ErgonServiceHelper ergonServiceHelper;
  private final RemovePreviousHostingPETrust removePreviousHostingPETrust;
  private final StoreNewHostingPECACert storeNewHostingPECACert;
  private final RestoreNewHostingPECACert restoreNewHostingPECACert;
  private final ApplyLimitsOverride applyLimitsOverride;
  private final ReconcileLCMServicesVersion reconcileLCMServicesVersion;


  @Getter
  private final TaskConstants.TaskOperationType taskOperationType;

  private final List<PcdrStepsHandler> pcdrStepsHandlers =
    new ArrayList<>();

  @Autowired
  public RestoreAndReconcileSubtask(RestorePcDataFromIdf restorePCDataFromIDF,
                                    ReconcilePcData reconcilePcData,
                                    ErgonServiceHelper ergonServiceHelper,
                                    RemovePreviousHostingPETrust removePreviousHostingPETrust,
                                    StoreNewHostingPECACert storeNewHostingPECACert,
                                    RestoreNewHostingPECACert restoreNewHostingPECACert,
                                    ZookeeperServiceHelperPc zookeeperServiceHelper,
                                    ApplyLimitsOverride applyLimitsOverride,
                                    ReconcileLCMServicesVersion reconcileLCMServicesVersion) {
    super(zookeeperServiceHelper);
    this.restorePCDataFromIDF = restorePCDataFromIDF;
    this.reconcilePcData = reconcilePcData;
    this.ergonServiceHelper = ergonServiceHelper;
    this.removePreviousHostingPETrust = removePreviousHostingPETrust;
    this.storeNewHostingPECACert = storeNewHostingPECACert;
    this.restoreNewHostingPECACert = restoreNewHostingPECACert;
    this.applyLimitsOverride = applyLimitsOverride;
    this.reconcileLCMServicesVersion = reconcileLCMServicesVersion;
    taskOperationType = TaskConstants.TaskOperationType.kRestoreDataAndReconcile;
  }

  @PostConstruct
  public void init() {
    pcdrStepsHandlers.add(storeNewHostingPECACert);
    pcdrStepsHandlers.add(restorePCDataFromIDF);
    // Moving removePreviousHostingPETrust before adding new hosting
    // PE cert to ensure that if the PE owner cluster uuid is same,
    // then new hosting PE certs remain intact.
    pcdrStepsHandlers.add(removePreviousHostingPETrust);
    pcdrStepsHandlers.add(restoreNewHostingPECACert);
    pcdrStepsHandlers.add(reconcilePcData);
    pcdrStepsHandlers.add(applyLimitsOverride);
    pcdrStepsHandlers.add(reconcileLCMServicesVersion);
  }

  /**
   * This subtask is having 3 steps, one to restore all the
   * zk-nodes and files. Second bring up all processes by invoking
   * genesis RPC and wait for 5 min after starting up processes.
   * Third to reconcile the data and wait for other services to reconcile.
   * @param rootTask - Root pc restore task.
   * @return - returns the updated rootTask object.
   */
  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    // STEP 4: Create a subtask having 3 steps, one to restore all the
    // zk-nodes and files. Second bring up all processes by invoking
    // genesis RPC and wait for 5 min after starting up processes.
    // Third to reconcile the data and wait for
    // other services to reconcile.
    int stepNum = (int) rootTask.getStepsCompleted() + 1;
    log.info("SUBTASK {}: Starting restore Data and reconcile " +
      "subtask.", stepNum);

    try {
      log.info("Starting restoreDataAndReconcileSubtask from completed step {}",
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
                                          exceptionDetails, ErrorArgumentKey.RESTORE_AND_RECONCILE_OPERATION);
      log.error("The restore data and reconcile task failed with error: ", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    ergonServiceHelper.updateTaskStatus(currentSubtask,
      ErgonTypes.Task.Status.kSucceeded);
    log.info("Restore data and reconcile task succeeded. Task {} " +
      "successful", stepNum);
    rootTask = ergonServiceHelper.updateTask(rootTask,
                                             stepNum,
                                             TaskConstants.RESTORE_AND_RECONCILE_PERCENTAGE);
    log.info("SUBTASK {}: Restore Data and reconcile subtask ended.", stepNum);
    return rootTask;
  }
}
