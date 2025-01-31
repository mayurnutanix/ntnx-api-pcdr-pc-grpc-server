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

import static com.nutanix.prism.pcdr.constants.Constants.CMSP_BASE_SERVICES_NODE;
import static com.nutanix.prism.pcdr.constants.Constants.STARTUP_NODE;

@Service
@Slf4j
public class DeleteServicesZkNode implements PcdrStepsHandler {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    // STEP: Starting all the genesis services using cluster start
    // and waiting for it to finish.
    log.info("STEP {}: Deleting startup and cmsp base services zk node.",
             currentStep);
    try {
      deleteZkNode(STARTUP_NODE);
      deleteZkNode(CMSP_BASE_SERVICES_NODE);
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      log.error("Unable to delete zk node due to following reason :",
                e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    log.info("STEP {}: Successfully deleted startup and cmsp base services zk" +
             " node.", currentStep);
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        TaskConstants.DELETE_SERVICES_ZK_NODE);
    return currentSubtask;
  }

  /**
   * Deletes the given zknode and logs success.
   * @throws ZkClientConnectException - zkclient connection exception
   * @throws InterruptedException - interrupted exception
   * @throws PCResilienceException -restore exception
   */
  public void deleteZkNode(String zkNode)
      throws ZkClientConnectException, InterruptedException, PCResilienceException {
    try {
      if (zookeeperServiceHelper.delete(zkNode, -1)) {
        log.info("Successfully deleted zk node {}", zkNode);
      } else {
        log.error(String.format("Unable to delete %s", zkNode));
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
    } catch (final KeeperException.NoNodeException e) {
      log.info("{} node already deleted, skipping this step.",
               zkNode);
    }
  }
}