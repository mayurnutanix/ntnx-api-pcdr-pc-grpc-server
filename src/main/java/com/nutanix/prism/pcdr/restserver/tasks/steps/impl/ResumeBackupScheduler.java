package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.apache.zookeeper.KeeperException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static com.nutanix.prism.pcdr.constants.Constants.PAUSE_BACKUP_SCHEDULER_ZK_NODE;

@Slf4j
@Service
public class ResumeBackupScheduler implements PcdrStepsHandler {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Resuming backup using backup scheduler.", currentStep);
    try {
      // Start the backup using our scheduler.
      deletePauseBackupSchedulerZkNode();
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      log.error("Unable to resume backup scheduler due" +
                " to following - :", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    log.info("STEP {}: Successfully resumed backup using backup scheduler.",
             currentStep);
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        TaskConstants.RESUME_BACKUP_SCHEDULER_STEP_PERCENTAGE);
    return currentSubtask;
  }


  /**
   * Delete pause_backup_scehduler zk node and successfully let the backup
   * happen.
   * @throws ZkClientConnectException - can throw zkclient exception.
   * @throws InterruptedException - can thrown interrupted exception.
   * @throws PCResilienceException - can throw pcdr task exception.
   */
  private void deletePauseBackupSchedulerZkNode()
      throws ZkClientConnectException, InterruptedException, PCResilienceException {
    try {
      if (zookeeperServiceHelper.delete(PAUSE_BACKUP_SCHEDULER_ZK_NODE, -1)) {
        log.info("Delete {} successfully, backup will resume.",
                 PAUSE_BACKUP_SCHEDULER_ZK_NODE);
      } else {
        log.error("Unable to delete {}, aborting",
                  PAUSE_BACKUP_SCHEDULER_ZK_NODE);
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
    } catch (final KeeperException.NoNodeException e) {
      log.warn("{} node is not present, it is possible that backup is not " +
               "restored correctly. Please check.",
               PAUSE_BACKUP_SCHEDULER_ZK_NODE, e);
    }
  }
}
