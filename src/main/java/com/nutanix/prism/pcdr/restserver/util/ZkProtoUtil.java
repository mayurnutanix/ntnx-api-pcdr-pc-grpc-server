package com.nutanix.prism.pcdr.restserver.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.nutanix.atlas.AtlasTypes;
import com.nutanix.flow.FlowTypes.FlowConfig;
import com.nutanix.prism.auth.ce.protobuf.CEAuthProto.CommunityEditionUserRepository;
import com.nutanix.prism.auth.protobuf.PrismAuthProto.UserRepository;
import com.nutanix.prism.base.zk.ICloseNullEnabledZkConnection;
import com.nutanix.prism.base.zk.ProtobufZNodeManagementException;
import com.nutanix.prism.base.zk.ZkClientConnectException;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.ClusterDataState;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto.ClusterExternalState;
import com.nutanix.prism.cluster.protobuf.DirectoryRoleMappingProto.RoleMappingRepository;
import com.nutanix.prism.keys.protobuf.SecureKeyRepositoryProto.SecureKeyRepository;
import com.nutanix.prism.pcdr.restserver.services.impl.PcdrProtobufManagingZookeeperImpl;
import com.nutanix.prism.userdata.PrismUserDataProto.UserDataRepository;
import com.nutanix.serviceability.LicenseDef.LicenseConfiguration;
import com.nutanix.zeus.protobuf.Configuration.ConfigurationProto;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.InvocationTargetException;

@Slf4j
public class ZkProtoUtil {

  public enum ProtoEnum {
    USER_REPOSITORY,
    CLUSTER_EXTERNAL_STATE,
    CLUSTER_DATA_STATE,
    CONFIGURATION,
    ROLE_MAPPING,
    USER_DATA_REPOSITORY,
    FLOW_CONFIG,
    SECURE_KEY_REPOSITORY,
    CE_USER_REPOSITORY,
    ATLAS_CONFIG,
    LICENSE_CONFIG
  }

  public static void writeZkContentUsingProto(ProtoEnum protoEnum,
    String zkPath, byte[] zkBytes, ICloseNullEnabledZkConnection zkClient)
    throws NoSuchMethodException, InterruptedException, IllegalAccessException,
    InvalidProtocolBufferException, ProtobufZNodeManagementException,
    InvocationTargetException, ZkClientConnectException {
    switch (protoEnum) {
      case USER_REPOSITORY:
        (new PcdrProtobufManagingZookeeperImpl<UserRepository>(
          zkClient)).createProtoClassBasedZkNode(
          UserRepository.class, UserRepository.Builder.class,
          zkPath, zkBytes);
        break;
      case CLUSTER_EXTERNAL_STATE:
        (new PcdrProtobufManagingZookeeperImpl<ClusterExternalState>(
          zkClient)).createProtoClassBasedZkNode(
          ClusterExternalState.class, ClusterExternalState.Builder.class,
          zkPath, zkBytes);
        break;
      case CLUSTER_DATA_STATE:
        (new PcdrProtobufManagingZookeeperImpl<ClusterDataState>(
          zkClient)).createProtoClassBasedZkNode(
          ClusterDataState.class, ClusterDataState.Builder.class,
          zkPath, zkBytes);
        break;
      case CONFIGURATION:
        (new PcdrProtobufManagingZookeeperImpl<ConfigurationProto>(
            zkClient)).createProtoClassBasedZkNode(
            ConfigurationProto.class, ConfigurationProto.Builder.class,
            zkPath, zkBytes);
        break;
      case ROLE_MAPPING:
        (new PcdrProtobufManagingZookeeperImpl<RoleMappingRepository>(
            zkClient)).createProtoClassBasedZkNode(
            RoleMappingRepository.class, RoleMappingRepository.Builder.class,
            zkPath, zkBytes);
        break;
      case USER_DATA_REPOSITORY:
        (new PcdrProtobufManagingZookeeperImpl<UserDataRepository>(
            zkClient)).createProtoClassBasedZkNode(
            UserDataRepository.class, UserDataRepository.Builder.class,
            zkPath, zkBytes);
        break;
      case FLOW_CONFIG:
        (new PcdrProtobufManagingZookeeperImpl<FlowConfig>(
                zkClient)).createProtoClassBasedZkNode(
                FlowConfig.class, FlowConfig.Builder.class,
                zkPath, zkBytes);
        break;
      case SECURE_KEY_REPOSITORY:
        (new PcdrProtobufManagingZookeeperImpl<SecureKeyRepository>(
                zkClient)).createProtoClassBasedZkNode(
                SecureKeyRepository.class, SecureKeyRepository.Builder.class,
                zkPath, zkBytes);
        break;
      case CE_USER_REPOSITORY:
        (new PcdrProtobufManagingZookeeperImpl<CommunityEditionUserRepository>(
                zkClient)).createProtoClassBasedZkNode(
                CommunityEditionUserRepository.class, CommunityEditionUserRepository.Builder.class,
                zkPath, zkBytes);
        break;
      case ATLAS_CONFIG:
        (new PcdrProtobufManagingZookeeperImpl<AtlasTypes.AtlasConfig>(
                zkClient)).createProtoClassBasedZkNode(
                AtlasTypes.AtlasConfig.class, AtlasTypes.AtlasConfig.Builder.class,
                zkPath, zkBytes);
        break;
      case LICENSE_CONFIG:
        (new PcdrProtobufManagingZookeeperImpl<LicenseConfiguration>(
            zkClient)).createProtoClassBasedZkNode(
            LicenseConfiguration.class,LicenseConfiguration.Builder.class,
            zkPath, zkBytes);
        break;
      default:
        log.error("Invalid Zookeeper proto data type provided");
        throw new NoSuchMethodException();
    }
  }
}
