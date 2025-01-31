package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.clients.RecoverPcProxyClient;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.RestoreIdfCallOnPC;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.RestoreIdfCallOnPE;
import com.nutanix.prism.pcdr.restserver.tasks.steps.impl.WaitForIdfRestore;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static com.nutanix.prism.pcdr.restserver.util.PCUtil.makeCleanIDFApiCallOnReplicaPE;
@Slf4j
@Service
public class RestoreIdfDataSubtask extends AbstractPcdrSubtaskHandler {

  private final RestoreIdfCallOnPC restoreIdfCallOnPC;
  private final RestoreIdfCallOnPE restoreIdfCallOnPE;
  private final WaitForIdfRestore waitForIdfRestore;
  private final ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private RecoverPcProxyClient recoverPcProxyClient;
  @Getter
  private final TaskConstants.TaskOperationType taskOperationType;

  // 2 Hours for subTaskTotalTimeout.
  @Value("${prism.pcdr.restoreidfdatasubtask.timeout.seconds:7200}")
  private long maxTimeLimitForSubtask;

  private boolean proceedWithTask = false;

  private final List<PcdrStepsHandler> pcdrStepsHandlers =
    new ArrayList<>();

  @Autowired
  public RestoreIdfDataSubtask(RestoreIdfCallOnPE restoreIdfCallOnPE,
                               WaitForIdfRestore waitForIdfRestore,
                               ErgonServiceHelper ergonServiceHelper,
                               ZookeeperServiceHelperPc zookeeperServiceHelper,
                               RestoreIdfCallOnPC restoreIdfCallOnPC) {
    super(zookeeperServiceHelper);
    this.restoreIdfCallOnPE = restoreIdfCallOnPE;
    this.waitForIdfRestore = waitForIdfRestore;
    this.ergonServiceHelper = ergonServiceHelper;
    this.restoreIdfCallOnPC = restoreIdfCallOnPC;
    taskOperationType = TaskConstants.TaskOperationType.kRestoreIDF;
  }

  @PostConstruct
  public void init() {
    pcdrStepsHandlers.add(restoreIdfCallOnPE);
    pcdrStepsHandlers.add(restoreIdfCallOnPC);
    pcdrStepsHandlers.add(waitForIdfRestore);
  }

  /**
   * Execute the restore idf data subtask.
   * @param rootTask - Root pc restore task.
   * @return - returns the root task
   * @throws PCResilienceException - Can throw PCResilienceException while
   * there is a task exception.
   */
  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    // STEP 2: Restore the IDF data from replica PE. Creates a subtask
    // having 2 steps, first step will create an entry in
    // pc_backup_restore_config table on replica PE. And second step
    // will wait for IDF data to be restored.
    int stepNum = (int) rootTask.getStepsCompleted() + 1;
    log.info("SUBTASK {}: Starting restore IDF data from replica PE " +
      "subtask.", stepNum);
    try {
      log.info("Starting restore IDF data subtask from the step {}",
        currentSubtask.getStepsCompleted());
      while(currentSubtask.getStepsCompleted() < pcdrStepsHandlers.size()) {
        proceedWithTask = checkProceedWithTask(rootTask);
        if (proceedWithTask) {
          int currentStep = (int) currentSubtask.getStepsCompleted();
          currentSubtask = pcdrStepsHandlers.get(currentStep)
                                            .execute(rootTask, currentSubtask);
          // If rootPCRestoreTask is null
          // either IDF RPC call failed
          // or pc_restore_idf_sync_marker has zero entities. Hence,
          // terminate the current execution and let the scheduler run next
          // time to check for it.
          if (currentSubtask == null) {
            return null;
          }
        }
        else {
          RestoreInternalOpaque restoreInternalOpaque =
              (RestoreInternalOpaque) Objects.requireNonNull(SerializationUtils.
                                                                 deserialize(rootTask.getInternalOpaque().toByteArray()));
          makeCleanIDFApiCallOnReplicaPE(
              Objects.requireNonNull(restoreInternalOpaque),
              instanceServiceFactory.getClusterUuidFromZeusConfigWithRetry(), recoverPcProxyClient);
          log.error(String.format("Aborting the RestoreIdfDataSubtask as it exceeded maxTimeLimitForSubtask %s seconds", maxTimeLimitForSubtask));
          throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
        }
      }
      createSucceededZkNode();
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ergonServiceHelper.updateTaskStatus(currentSubtask.getUuid(), ErgonTypes.Task.Status.kFailed,
                                          exceptionDetails, ErrorArgumentKey.RESTORE_IDF_OPERATION);
      log.error("The IDF data subtask failed with error: ", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }

    ergonServiceHelper.updateTaskStatus(currentSubtask,
      ErgonTypes.Task.Status.kSucceeded);
    log.info("Restore IDF data task succeeded.Task {} " +
      "successful", stepNum);
    rootTask = ergonServiceHelper.updateTask(rootTask,
                                             stepNum,
                                             TaskConstants.RESTORE_IDF_PERCENTAGE);
    log.info("SUBTASK {}: Restore IDF data from replica PE subtask " +
             "ended.", stepNum);
    return rootTask;
  }

  public boolean checkProceedWithTask(ErgonTypes.Task rootTask) {
    if (!rootTask.getUuid().isEmpty()) {
      LocalDateTime from = LocalDateTime.now();
      long currentTimestampMisec = Timestamp.valueOf(from).getTime();
      long ergonTaskCreationTimeStampMisec = rootTask.getCreateTimeUsecs() / 1000;
      log.info("Current time stamp:{} and Ergon Creation time stamp:{}",
                currentTimestampMisec, ergonTaskCreationTimeStampMisec);
      long taskDurationSecs = (currentTimestampMisec - ergonTaskCreationTimeStampMisec) / 1000;
      proceedWithTask = (taskDurationSecs < maxTimeLimitForSubtask);
    }
    return proceedWithTask;
  }
}
