package com.nutanix.prism.pcdr.restserver.services.impl;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.nutanix.prism.base.zk.ICloseNullEnabledZkConnection;
import com.nutanix.prism.base.zk.ProtobufManagingZookeeperImpl;
import com.nutanix.prism.base.zk.ProtobufZNodeManagementException;
import com.nutanix.prism.base.zk.ZkClientConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.data.Stat;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * A pcdr specific class containing method to accept any proto class
 * and change the logical timestamp of it.
 * @param <T> - Can be any proto class.
 */
@Slf4j
public class PcdrProtobufManagingZookeeperImpl<T extends Message>
  extends ProtobufManagingZookeeperImpl<T> {

  private final ICloseNullEnabledZkConnection zkClient;

  /**
   * Constructor to feed zkClient.
   * @param zkClient - A zookeeper client to do basic zookeeper operation
   *                 like read, write etc.
   */
  public PcdrProtobufManagingZookeeperImpl(ICloseNullEnabledZkConnection zkClient) {
    super(zkClient);
    this.zkClient = zkClient;
  }

  /**
   * A function to create/update a Zookeeper node on a given path
   * corresponding to a proto. It is necessary to update the logicalTimestamp
   * of that proto while writing at the time of restore.
   * @param protoCls - The class of the proto.
   * @param protoBuilderClass - The class of the proto builder.
   * @param zkPath - Path of the zk node where this is supposed to be written.
   * @param protoBytes - Bytes of the proto which are supposed to be written
   *                   and modified.
   * @throws NoSuchMethodException - If there is no setLogicalTimestamp method
   * in the protobuilder class then this exception is raised.
   * @throws ZkClientConnectException - Zk exception is raised in case of
   * Zookeeper connection error.
   * @throws InterruptedException - If a Zookeeper client is interrupted then
   * an InterruptedException is raised.
   * @throws InvocationTargetException - At the time of function invocation
   * using invoke method this exception is raised if some error occurred.
   * @throws IllegalAccessException - The invoke function can also raise an
   * illegal access exception if method is called wrongly.
   * @throws InvalidProtocolBufferException - Invalid protobuf given
   * @throws ProtobufZNodeManagementException - Custom exception of
   * ProtobufManagingZookeeperImpl raised.
   */
  public void createProtoClassBasedZkNode(final Class<T> protoCls,
    final Class<?> protoBuilderClass, String zkPath,
    byte[] protoBytes) throws NoSuchMethodException,
    ZkClientConnectException, InterruptedException,
    InvocationTargetException, IllegalAccessException,
    InvalidProtocolBufferException, ProtobufZNodeManagementException {

    // Restoring cluster external state for replica PE.

    final Method newBuilderMethod =
      protoCls.getMethod("newBuilder", (Class<?>[]) null);
    final Method setLogicalTimestampMethod =
      protoBuilderClass.getMethod("setLogicalTimestamp", long.class);

    final Message.Builder msgBuilder =
      (Message.Builder) newBuilderMethod.invoke(null, (Object[]) null);

    msgBuilder.mergeFrom(protoBytes);

    Stat stat = this.zkClient.exists(zkPath, false);

    if (stat == null) {
      setLogicalTimestampMethod.invoke(msgBuilder, 0);
      this.createPersistent(zkPath, protoCls.cast(msgBuilder.build()));
    } else {
      setLogicalTimestampMethod.invoke(msgBuilder, stat.getVersion()+1);
      this.update(zkPath, protoCls.cast(msgBuilder.build()), stat.getVersion());
    }
  }
}
