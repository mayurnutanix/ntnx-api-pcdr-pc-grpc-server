package com.nutanix.prism.pcdr.restserver.tasks.subtasks.impl;

import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrSubtaskHandler;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

@Slf4j
public abstract class AbstractPcdrSubtaskHandler implements PcdrSubtaskHandler {

  private final ZookeeperServiceHelperPc zookeeperServiceHelper;

  protected AbstractPcdrSubtaskHandler(ZookeeperServiceHelperPc zookeeperServiceHelper) {
    this.zookeeperServiceHelper = zookeeperServiceHelper;
  }

  @Override
  public abstract ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException;

  @Override
  public abstract TaskConstants.TaskOperationType getTaskOperationType();

  protected void createSucceededZkNode()
      throws KeeperException.NodeExistsException, ZkClientConnectException,
             InterruptedException {
    String zkNodeName =
        Constants.PC_TASK_SUCCEEDED_ZK_PATH + Constants.ZK_NODE_DELIMITER +
        getTaskOperationType().getCompletedZkName();
    PCUtil.createAllParentZkNodes(zkNodeName, zookeeperServiceHelper);
    try {
      zookeeperServiceHelper.create(zkNodeName, "SUCCEEDED".getBytes(),
                      ZooDefs.Ids.OPEN_ACL_UNSAFE,
                      CreateMode.PERSISTENT);
      log.info("Successfully created zk node {}", zkNodeName);
    } catch (final KeeperException.NodeExistsException e) {
      log.warn("{} zk node already exists, skipping creation.", zkNodeName);
    }
  }
}
