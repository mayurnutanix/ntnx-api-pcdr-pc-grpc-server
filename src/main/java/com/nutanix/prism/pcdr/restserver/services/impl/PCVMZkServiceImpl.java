package com.nutanix.prism.pcdr.restserver.services.impl;

import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.insights.ifc.InsightsInterfaceProto.BatchUpdateEntitiesArg;
import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.messages.APIErrorMessages;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl.PathNode;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMZkService;
import com.nutanix.prism.pcdr.util.IDFUtil;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class PCVMZkServiceImpl implements PCVMZkService {

  @Autowired
  private PCDRYamlAdapterImpl pcdrYamlAdapter;
  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;

  /**
   * Add the UpdateEntityArg for zookeeper nodes.
   * @param batchUpdateEntitiesArgBuilder - param in which updateEntityArg is
   *                                     added.
   */
  public void addUpdateEntitiesArgForPCZkData(
    BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArgBuilder) {
    for (PathNode zookeeperNode : pcdrYamlAdapter.getZookeeperNodes()) {
      updateEntitiesArgListForZkNodeData(batchUpdateEntitiesArgBuilder,
        zookeeperNode.getPath());
    }
  }

  /**
   * Traversing the zk node tree and adding the UpdateEntityArg in
   * BatchUpdateEntitiesArgBuilder. The parent data is first added and then
   * the child's data is fed.
   *              RightLeafNode
   * rootNode ->
   *                        leafNode2
   *              LeftNode ->
   *                        leafNode3
   * UpdateEntityArg will be added in order [rootNode, Node1, leafNode2,
   * leafNode3, RightLeafNode] -> Preorder traversal.
   * @param batchUpdateEntitiesArgBuilder - Argument which needs to be
   *                                      updated with UpdateEntityArg.
   * @param znode - ZkNode path.
   */
  public void updateEntitiesArgListForZkNodeData(
    BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArgBuilder,
    final String znode) {
    // Make a list which will contain the children nodes of this zknode.
    List<String> childrenNodes;

    // Attributes value map will contain only non-list values.
    Map<String, Object> attributesValueMap = new HashMap<>();
    // Entity Id is pc_cluster_uuid::zk_fullpath.
    String entityId = String.format("%s%s%s",
      instanceServiceFactory.getClusterUuidFromZeusConfig(),
      Constants.SEPARATOR_ZK_ENTITY_ID,
      znode
    );

    // First add in BatchUpdate and then go to the child nodes.
    try {
      childrenNodes = zookeeperServiceHelper.getChildren(znode);
      final byte[] data;
      final Stat stat = new Stat();
      data = zookeeperServiceHelper.getData(znode, stat);
      attributesValueMap.put(Constants.VALUE, data);

      if (childrenNodes.isEmpty()) {
        attributesValueMap.put(Constants.IS_PARENT, false);
        attributesValueMap.put(Constants.ZK_VERSION, (long) stat.getVersion());
        batchUpdateEntitiesArgBuilder.addEntityList(IDFUtil.
          constructUpdateEntityArgBuilder(
            Constants.PC_ZK_DATA,
            entityId,
            attributesValueMap
          )
        );
        // Return in case of leaf node. There will be no further computation
        // for the children nodes, as the leaf has reached.
        return;
      } else {
        attributesValueMap.put(Constants.IS_PARENT, true);
        // List<?> type can't be added in attributesValueMap
        batchUpdateEntitiesArgBuilder.addEntityList(IDFUtil.
          constructUpdateEntityArgBuilder(
            Constants.PC_ZK_DATA,
            entityId,
            attributesValueMap
          ).addAttributeDataArgList(
            IDFUtil.buildAttributeDataArg(
              Constants.ZK_CHILDREN, childrenNodes,
              InsightsInterfaceProto.AttributeDataArg.AttributeOperation.kSET)
          )
        );
      }

      // Traverse every child node. And add it to batchUpdateEntitiesArgs.
      for (String node: childrenNodes) {
        String childZNode = znode + "/" + node;
        updateEntitiesArgListForZkNodeData(batchUpdateEntitiesArgBuilder,
          childZNode);
      }
    } catch (final KeeperException.NoNodeException nne) {
      log.debug(APIErrorMessages.DEBUG_START, nne);
      log.warn("The Zk node {} not found. " +
        "Ignoring, check if the zkNode is necessary.", znode);
    } catch (ZkClientConnectException e) {
      log.debug(APIErrorMessages.DEBUG_START, e);
      log.warn("There were some problem with connection while retrieving " +
          "zk node {}", znode);
    } catch (Exception e) {
      log.debug(APIErrorMessages.DEBUG_START, e);
      log.warn("Could not backup the given zk node {} due to exception {}", znode, e.getMessage());
    }
  }

}
