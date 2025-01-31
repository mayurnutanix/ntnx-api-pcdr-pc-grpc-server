package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

import static com.nutanix.prism.pcdr.constants.Constants.IAM_ENABLED_NODE_PATH;
import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.RESTORE_IAM_WAIT_PERCENTAGE;

@Slf4j
@Service
public class WaitForIamRestore implements PcdrStepsHandler {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;

  @Value("${prism.pcdr.iam.restore.wait.duration.millis:1500000}")
  private int iamRestoreWaitDuration;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
          throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    // Check if zkNode iam_v2_enabled is present
    // if present then success,  Retry with exception
    log.info("STEP {}: Check if IAM restore zookeeper node exists " +
             "on PC.", currentStep);

    if (PCUtil.isCMSPEnabled(zookeeperServiceHelper) && !checkIamRestoreZkNode()) {
      // Subtask start time in microseconds from epoch. The task start time is automatically
      // set by Ergon when the task status changes to kRunning, but the caller can
      // override by setting this field.
      boolean isWaitDone = hasTaskExceededWaitTime(currentSubtask.getStartTimeUsecs() / 1000);

      // If timeout duration is not elapsed yet, retry the same operation in next iteration
      if (!isWaitDone) {
        return null;
      }
      else {
        // If timeout duration is elapsed, then throw timeout exception & mark the subtask as failure
        log.error("IAM Recovery did not happen within specific time limit, so aborting the task");
        throw new PCResilienceException(ErrorMessages.IAM_RECOVERY_TIME_EXCEEDED_ERROR,
                ErrorCode.PCBR_TIMED_IAM_RECOVERY_FAILURE, HttpStatus.INTERNAL_SERVER_ERROR);
      }
    }
    // if IAM restore task is successful
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep, RESTORE_IAM_WAIT_PERCENTAGE);
    log.info("STEP {}: Successfully completed. IAM data is restored.",
             currentStep);
    return currentSubtask;
  }

  /**
   * Checks if IAM restore zookeeper node exists or not.
   */
  public boolean checkIamRestoreZkNode() {
    try {
      Stat stat = zookeeperServiceHelper.exists(IAM_ENABLED_NODE_PATH, false);

      if (stat != null) {
        log.info("{} IAM restore zk node already exists with stats {}.",
                 IAM_ENABLED_NODE_PATH, stat);
        return true;
      }
      else {
        log.info("{} IAM restore zk node does not exists yet.",
                 IAM_ENABLED_NODE_PATH);
        return false;
      }
    }
    catch (Exception e) {
      log.error("Unable to fetch the status IAM restore zk node due to the " +
                "following exception - ", e);
      // returning false for retry in next iteration, throwing exception would abort the restore process
      return false;
    }
  }

  public boolean hasTaskExceededWaitTime(long taskStartTime) {
    LocalDateTime startTime = Instant.ofEpochMilli(taskStartTime).atZone(ZoneId.systemDefault()).toLocalDateTime();
    long duration = Duration.between(startTime,
                                     LocalDateTime.now()).toMillis();
    log.info("Current time spent {} milli seconds waiting for Iam Restore.", duration);
    return duration >= iamRestoreWaitDuration;
  }
}
