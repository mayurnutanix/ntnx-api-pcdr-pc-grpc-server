
package com.nutanix.prism.pcdr.restserver.services.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants.TaskOperationType;
import com.nutanix.prism.pcdr.restserver.services.api.PCRestoreService;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrSubtaskHandler;
import com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl.*;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import com.nutanix.prism.pcdr.zklock.DistributedLock;
import com.nutanix.util.base.UuidUtils;
import dp1.pri.prism.v4.protectpc.RecoveryStatus;
import dp1.pri.prism.v4.protectpc.ReplicaInfo;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonInterface.TaskListRet;
import nutanix.ergon.ErgonTypes.Task;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

import static com.nutanix.prism.pcdr.constants.Constants.*;

/**
 * A spring service used to restore the PC on a PE using a replica PE.
 */
@Slf4j
@Service
public class PCRestoreServiceImpl implements PCRestoreService {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private RestoreDataService restoreDataService;
  @Autowired
  private PCTaskServiceHelper pcTaskServiceHelper;
  @Autowired
  private RestoreBasicTrustSubtask restoreBasicTrustSubtask;
  @Autowired
  private RestoreIdfDataSubtask restoreIdfDataSubtask;
  @Autowired
  private CleanIdfStaleValuesSubtask cleanIdfStaleValuesSubtask;
  @Autowired
  private RestoreAndReconcileSubtask restoreAndReconcileSubtask;
  @Autowired
  private RestartServicesSubtask restartServicesSubtask;
  @Autowired
  private ResetArithmosSyncSubTask resetArithmosSyncSubTask;
  @Autowired
  private ResumeBackupSubtask resumeBackupSubtask;
  @Autowired
  private IamRecoverySubTask iamRecoverySubTask;
  @Autowired
  ZookeeperServiceHelperPc zookeeperServiceHelper;

  // Lock to handle cases where it is possible that scheduler is in middle of
  // shutdown and another Api request is called.
  private final Object schedulerLock = new Object();

  private ScheduledFuture<?> scheduledPCRestoreTask;

  // Define in post-construct
  private final List<PcdrSubtaskHandler> pcdrSubtaskHandlers =
      new ArrayList<>();

  private final Object clusterStopServicesLock = new Object();

  @Autowired
  @Qualifier("adonisServiceScheduledThreadPool")
  private ScheduledExecutorService adonisServiceScheduledThreadPool;

  /**
   * Create a runnable PC restore scheduler.
   */
  private final Runnable pcRestoreRunnable = () -> {
    try {
      synchronized (schedulerLock) {
        executePCRestoreTasks();
      }
    } catch (Exception e) {
      log.error("Error encountered while executing the restore task, will try" +
                " again in next scheduled task.", e);
    }
  };

  @Value("${prism.pcdr.restore.scheduler.time:60000}")
  private int restoreSchedulerTime;
  @Value("${prism.pcdr.services.stop.wait.time:600}")
  private long servicesStopWaitTime;
  @Value("${prism.pcdr.restore.scheduler.initialDelay:60000}")
  private long initialDelay;


  /**
   * Initialises the zkClient and also checks whether any restore is in
   * progress or not. This runs whenever the service will crash so that the
   * restore can start from the place it got stopped.
   */
  @PostConstruct
  public void init() {
    createRecoveryHandlerMap();
    // When the container crashes and comes up again it checks and see if the
    // Ergon task is still in process, if that is the case then start the
    // task again.
    createExecutePCRestoreScheduler();
  }

  private void createRecoveryHandlerMap() {
    pcdrSubtaskHandlers.add(restoreBasicTrustSubtask);
    pcdrSubtaskHandlers.add(restoreIdfDataSubtask);
    pcdrSubtaskHandlers.add(cleanIdfStaleValuesSubtask);
    pcdrSubtaskHandlers.add(restoreAndReconcileSubtask);
    pcdrSubtaskHandlers.add(iamRecoverySubTask);
    pcdrSubtaskHandlers.add(restartServicesSubtask);
    pcdrSubtaskHandlers.add(resetArithmosSyncSubTask);
    pcdrSubtaskHandlers.add(resumeBackupSubtask);
  }

  @VisibleForTesting
  public void init(ScheduledFuture<?> scheduledPCRestoreTask) {
    this.scheduledPCRestoreTask = scheduledPCRestoreTask;
    createRecoveryHandlerMap();
  }

  /**
   * Initiates the restore task for PC. Starts the restore task in a
   * schedule, which will keep running until it marks the Root task as either
   * completed or failed.
   * @param replicaInfo - Info of Replica from which the PC data is required
   *                      to be restored.
   * @param authentication - authentication details from API Controller
   * @throws PCResilienceException - This exception is raised when the
   * restore task is already in progress and again the restore API is called.
   */
  @Override
  public ByteString restore(ReplicaInfo replicaInfo, Authentication authentication)
      throws PCResilienceException, ErgonException {
    ByteString rootTaskUuid;
    String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    DistributedLock distributedLock = instanceServiceFactory
        .getDistributedLockForRecoveryPCDR();
    // It is a blocking lock and will wait until the lock is not released.
    try {
      if (distributedLock.lock(true)) {
        // Create a pause backup zk node before proceeding further.
        try {
          Stat stat = zookeeperServiceHelper.exists(PAUSE_BACKUP_SCHEDULER_ZK_NODE, false);
          if (stat != null) {
            log.info("{} zk node already exists with stats {}.",
                     PAUSE_BACKUP_SCHEDULER_ZK_NODE, stat);
          } else {
            String zkNode = zookeeperServiceHelper.createZkNode(PAUSE_BACKUP_SCHEDULER_ZK_NODE,
                                                  null,
                                                  ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                                  CreateMode.PERSISTENT);
            log.info("Successfully created zk node - {}", zkNode);
          }
        }
        catch (Exception e) {
          log.error("Unable to create pause backup zk node due to the " +
                    "following exception - ", e);
          throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
        }
        TaskListRet taskListRet = ergonServiceHelper
            .fetchTaskListRet(
                TaskConstants.TaskOperationType.kPCRestore.name(), false, PCDR_TASK_COMPONENT_TYPE);
        if (taskListRet.getTaskUuidListCount() > 0) {
          log.info("Restore task already in progress. Exiting with exception.");
          throw new PCResilienceException(ErrorMessages.RECOVER_TASK_ALREADY_IN_PROGRESS_ERROR,
              ErrorCode.PCBR_RECOVERY_TASK_IN_PROGRESS, HttpStatus.SERVICE_UNAVAILABLE);
        }
        rootTaskUuid = pcTaskServiceHelper.createPCRestoreRootTask(replicaInfo, authentication);
        createAllSubtasksForRootTaskUuid(rootTaskUuid, pcClusterUuid, replicaInfo.getPeClusterId());
        log.info("Successfully created root task with uuid {}.",
                 UuidUtils.getUUID(rootTaskUuid));
      } else {
        log.info("Unable to acquire distributed lock.");
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
    } finally {
      distributedLock.unlock();
    }
    createExecutePCRestoreScheduler();
    return rootTaskUuid;
  }

  /**
   * Create all the subtasks of PCRestore root task.
   * @param rootTaskUuid - uuid of the root task.
   * @param pcClusterUuid - pcClusterUuid
   */
  public void createAllSubtasksForRootTaskUuid(ByteString rootTaskUuid, String pcClusterUuid, String peClusterUuid)
      throws ErgonException {
    for (int i = 0; i < pcdrSubtaskHandlers.size(); i++) {
      pcTaskServiceHelper.createSubtask(
          pcdrSubtaskHandlers.get(i).getTaskOperationType(),
          rootTaskUuid,
          i+1,
          pcClusterUuid,
          peClusterUuid);
    }
  }

  /**
   * @return - Returns the recoveryStatus by fetching overall percentage and
   * status of the task
   * @throws PCResilienceException - can throw an exception in case
   * fetching the root task from Ergon fails.
   */
  public RecoveryStatus getRecoveryStatus()
      throws PCResilienceException, ErgonException {
    RecoveryStatus recoveryStatus = new RecoveryStatus();
    Task rootTask = pcTaskServiceHelper.fetchPCRestoreRootTask();
    if (rootTask != null) {
      log.debug("Setting recovery status percentage as : {}",
                rootTask.getPercentageComplete());
      recoveryStatus.setOverallCompletionPercentage(
          rootTask.getPercentageComplete());
      List<ByteString> subtaskUuids = rootTask.getSubtaskUuidListList();
      if (subtaskUuids.isEmpty()) {
        log.info("Subtask uuid list is empty.");
        recoveryStatus.setRecoveryState(TaskOperationType.kInitialised.name());
        recoveryStatus.setRecoveryStateTitle(TaskOperationType.kInitialised
                                                 .getTaskName());
      } else if (subtaskUuids.size() > rootTask.getStepsCompleted()) {
        ByteString currentSubtaskUuid = subtaskUuids.get(
            (int) rootTask.getStepsCompleted());
        log.info("Current subtask uuid is : {}",
                 UuidUtils.getUUID(currentSubtaskUuid));
        Task currentSubtask = ergonServiceHelper.fetchTask(currentSubtaskUuid,
                                                           false);
        recoveryStatus.setRecoveryState(currentSubtask.getOperationType());
        recoveryStatus.setRecoveryStateTitle(currentSubtask.getDisplayName());
      } else {
        log.info("All subtasks are completed.");
        recoveryStatus.setRecoveryState(TaskOperationType
                                            .kRestoreTaskCompleted.name());
        recoveryStatus.setRecoveryStateTitle(TaskOperationType
                                                 .kRestoreTaskCompleted.getTaskName());
      }
    }
    else {
      // In-case the task is in completed state (either success, failure or
      // aborted) we will need to fetch that and show the status.
      log.info("The root task is not yet in progress, fetching the last root " +
               "task info.");
      TaskListRet taskListRet = ergonServiceHelper.fetchTaskListRet(
          TaskOperationType.kPCRestore.name(), true, PCDR_TASK_COMPONENT_TYPE);
      if (taskListRet.getTaskUuidListCount() > 0) {
        ByteString taskUuid = taskListRet.getTaskUuidList(taskListRet.getTaskUuidListCount() - 1);
        Task rootTaskFromList =
            ergonServiceHelper.getTaskByUuid(taskUuid).getTaskList(0);
        Task.Status taskStatus = rootTaskFromList.getStatus();
        recoveryStatus.setOverallCompletionPercentage(
            rootTaskFromList.getPercentageComplete());
        recoveryStatus.setRecoveryState(taskStatus.name());
        recoveryStatus.setRecoveryStateTitle(taskStatus.name());
        log.info("The previous root task was {}.", taskStatus.name());
      } else {
        log.info("No last root task found. Returning NoTaskFound.");
        recoveryStatus.setOverallCompletionPercentage(0);
        recoveryStatus.setRecoveryState(TaskOperationType.kNoTaskFound.name());
        recoveryStatus.setRecoveryStateTitle(TaskOperationType.kNoTaskFound
                                                 .getTaskName());
      }
    }
    log.debug("Current recovery status : {}", recoveryStatus);
    return recoveryStatus;
  }


  /**
   * Stop the list of services passed in the argument.
   * @param servicesToStop - name of the services to stop
   */
  public void stopClusterServices(List<String> servicesToStop) {
    synchronized(clusterStopServicesLock) {
      ExecutorService stopClusterServicesExecutor =
          Executors.newSingleThreadExecutor();
      stopClusterServicesExecutor.execute(new StopServicesRunnable(
          clusterStopServicesLock,
          servicesToStop,
          pcTaskServiceHelper,
          servicesStopWaitTime
      ));
      stopClusterServicesExecutor.shutdown();
    }
  }

  /**
   * Creates a scheduler and assign it to the scheduledPCRestoreTask which
   * runs on a fixed time delay to check the status of the task.
   */
  public void createExecutePCRestoreScheduler() {
    synchronized (schedulerLock) {
        log.info("Creating scheduler with Fixed delay.");
        scheduledPCRestoreTask = adonisServiceScheduledThreadPool.scheduleWithFixedDelay(
            pcRestoreRunnable, initialDelay, restoreSchedulerTime, TimeUnit.MILLISECONDS);
      }
    }


  /**
   * This is the main function to execute the restore task of the PE. It
   * checks at what subtask the root task is at and accordingly executes the
   * corresponding logic of that subtask. Every subtask decides the execution
   * in the same manner.
   */
  public void executePCRestoreTasks()
      throws PCResilienceException, ErgonException {
    DistributedLock distributedLock = instanceServiceFactory
        .getDistributedLockForRecoveryPCDR();
    Boolean isPcRestored = null;
    log.info("Entering executePCRestoreTasks functions.");
    // Do remember to cancel the scheduled task when required/returning
    // from this function.
    // Acquiring a non-blocking lock here. So that this is not executed in
    // parallel at rest of the PC vms.
    try {
      if (distributedLock.lock(false)) {
        Task rootPCRestoreTask;

        try {
          rootPCRestoreTask = pcTaskServiceHelper.fetchPCRestoreRootTask();
        }
        catch (ErgonException e) {
          log.error("Unable to fetch root restore task info from Ergon. Will " +
                    "retry in next run of scheduler.");
          return;
        }

        try {
          if (rootPCRestoreTask != null) {
            rootPCRestoreTask =
                pcTaskServiceHelper
                    .updateQueuedTaskStatusToRunning(rootPCRestoreTask,
                                                     true);

            log.info("Number of steps completed for root PC restore tasks {}",
                     rootPCRestoreTask.getStepsCompleted());
            rootPCRestoreTask = executeRecoverySubtasks(rootPCRestoreTask);
            if (rootPCRestoreTask == null) {
              return;
            }
            ergonServiceHelper.updateTaskStatus(rootPCRestoreTask,
                                                Task.Status.kSucceeded);
            isPcRestored = true;
            // Please note that parent path "/appliance/logical/prism/pcdr/"
            // would be already created by any of the previouse PCDR zkNodes

            try {
              zookeeperServiceHelper.createZkNode(PCDR_RECOVERY_COMPLETE_ZK_PATH, null,
                                    ZooDefs.Ids.OPEN_ACL_UNSAFE,
                                    CreateMode.PERSISTENT);
            } catch (final KeeperException.NodeExistsException e) {
              log.error("zkNode with path {} already exists", PCDR_RECOVERY_COMPLETE_ZK_PATH);
            }
            log.info("Successfully completed the PC restore task.");
          }
          else {
            log.info("No PC restore root task found. Nothing to do.");
          }
        }
        catch (Exception e) {
          ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
          isPcRestored = false;
          log.error(
              "Unknown exception encountered while restoring task. Failing " +
              "PC restore task", e);

          if (rootPCRestoreTask != null)
            updateRestoreRemainingSubtaskAndRootTask(rootPCRestoreTask,
                                              exceptionDetails);

        }
      }
      else {
        log.info("Another restore scheduled task running in parallel. " +
                 "Cancelling this thread.");
      }
    }
    finally {
      distributedLock.unlock();
    }
    log.info("Exiting executePCRestoreTask function.");
    // Cancel the task after success or failure.
    cancelScheduledRestore(isPcRestored);
  }

  public Task executeRecoverySubtasks(Task rootPCRestoreTask)
      throws ErgonException, PCResilienceException {
    while(rootPCRestoreTask.getStepsCompleted() < pcdrSubtaskHandlers.size()) {
      Task currentSubtask = fetchCurrentSubtaskAndUpdateToRunning(
          rootPCRestoreTask);
      int currentStep = (int) rootPCRestoreTask.getStepsCompleted();
      rootPCRestoreTask = pcdrSubtaskHandlers.get(currentStep)
                                             .execute(rootPCRestoreTask, currentSubtask);
      if (rootPCRestoreTask == null) {
        // If rootPCRestoreTask is null that means we are still
        // waiting for the reconciliation default time. So, return
        // and let the scheduler run next time to check for it.
        return null;
      }
    }
    return rootPCRestoreTask;
  }

  /**
   * Update QUEUED subtasks to ABORTED and mark the current root Task as failed.
   * @param rootPCRestoreTask - root PC restore task
   * @throws PCResilienceException - Can throw an exception while making an
   * update call.
   */
  public void updateRestoreRemainingSubtaskAndRootTask(Task rootPCRestoreTask,
                                                ExceptionDetailsDTO exceptionDetails)
      throws ErgonException, PCResilienceException {
    int lastSuccessStep = (int) rootPCRestoreTask.getStepsCompleted();

    // Update all the remaining subtasks to Aborted.
    for (int i = lastSuccessStep; i < pcdrSubtaskHandlers.size(); i++) {
      pcTaskServiceHelper.updateQueuedTaskStatusToAborted(
          rootPCRestoreTask.getSubtaskUuidList(i));
    }

    // After updating all the subtasks, update the root task as failed.
    ergonServiceHelper.updateTaskStatus(rootPCRestoreTask.getUuid(),
                                        Task.Status.kFailed,
                                        exceptionDetails,
                                        ErrorArgumentKey.RESTORE_DOMAIN_MANAGER);
  }

  /**
   * Cancelling the currently executing scheduler.
   */
  public void cancelScheduledRestore(Boolean isPcRestored) {
    synchronized (schedulerLock) {
      log.info("Cancelling scheduledPCRestoreTask scheduler.");
      if (isPcRestored != null) {
        // This is to not show connected in case restore task fails on Prism Central
        if (!isPcRestored) {
          try {
            restoreDataService
                .stopServicesUsingGenesis(Collections.singletonList("prism"));
            log.info("Successfully stopped prism_gateway on PC.");
          } catch (Exception e) {
            log.error("Unable to stop prism_gateway on PC at failure due to -", e);
          }
        }
        log.info("The pcRestoreTask is {}", Boolean.TRUE.equals(isPcRestored) ?
                                            "success" : "failure");
      }
      scheduledPCRestoreTask.cancel(true);
    }
  }

  /**
   * Get current subtask from root task subtaskuuid list. Fetch it on the
   * basis of subtask uuid, and updates the status of the subtask to running
   * if queued.
   * @param rootPCRestoreTask - root PC restore task
   * @return - returns Task of the current subtask
   * @throws PCResilienceException - can throw PCResilienceException
   */
  public Task fetchCurrentSubtaskAndUpdateToRunning(Task rootPCRestoreTask)
      throws PCResilienceException, ErgonException {
    int stepNum = (int) rootPCRestoreTask.getStepsCompleted();
    log.info("Executing {} subtask with step num {}.",
             pcdrSubtaskHandlers.get(stepNum).getTaskOperationType().name(), stepNum);
    ByteString subtaskUuid =
        rootPCRestoreTask.getSubtaskUuidList(stepNum);

    log.info("Fetching {} subtask from uuid {}",
             pcdrSubtaskHandlers.get(stepNum).getTaskOperationType().name(),
             UuidUtils.getUUID(subtaskUuid));
    Task subtask = ergonServiceHelper.fetchTask(
        subtaskUuid, false);

    if (subtask.getStatus() == Task.Status.kFailed ||
        subtask.getStatus() == Task.Status.kAborted) {
      log.error(String.format("Unable to complete the subtask %s. The subtask has failed",
              pcdrSubtaskHandlers.get(stepNum).getTaskOperationType().name()));
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }

    return pcTaskServiceHelper.updateQueuedTaskStatusToRunning(
        subtask, false);
  }
}