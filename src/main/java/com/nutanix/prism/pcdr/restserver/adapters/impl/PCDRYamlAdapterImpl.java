package com.nutanix.prism.pcdr.restserver.adapters.impl;

import com.nutanix.prism.pcdr.factory.YamlPropertySourceFactory;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.restserver.util.ZkProtoUtil;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.List;

@Configuration
@ConfigurationProperties(prefix = "pcdr")
@PropertySource(value = "classpath:pc_dr.yaml",
  factory = YamlPropertySourceFactory.class)
// Keep the getter and setters at the end.
@Getter
@Setter
public class PCDRYamlAdapterImpl {
  private List<String> servicesToRestartAfterBasicTrust;
  private List<String> servicesToRestartAfterRestoreData;
  private List<String> servicesToDisableOnStartupCMSP;
  private List<PathNode> zookeeperNodes;
  private List<MaxBackupEntityCountVsAOSVersion> maxBackupEntityCountVsAOSVersions;
  private FileSystemNodes fileSystemNodes;
  private List<String> servicesToDisableOnStartupGenesis;
  private List<String> portfolioServicesToReconcileAfterRestore;
  private List<LcmGenesisServiceMapping> lcmGenesisServiceMapping;

  public void setMaxBackupEntityCountVsAOSVersions(
      List<MaxBackupEntityCountVsAOSVersion> maxBackupEntityCountVsAOSVersions) {
    this.maxBackupEntityCountVsAOSVersions = maxBackupEntityCountVsAOSVersions;
    // Sort the max backup entity count map according to versions.
    sortMaxBackupEntityCountVsAOSVersions(this.maxBackupEntityCountVsAOSVersions);
  }

  /**
   * This class object holds minimum AOS version and the entity count map which
   * is supported for that AOS.
   */
  @Getter
  @Setter
  public static class MaxBackupEntityCountVsAOSVersion {
    private String minVersion;
    private long supportedCount;
  }

  @Getter
  @Setter
  public static class FileSystemNodes {
    private List<PathNode> pcvmSpecsNodes;
    private List<PathNode> generalNodes;
  }

  @Getter
  @Setter
  public static class LcmGenesisServiceMapping {
    private List<String> lcmService;
    private String genesisService;
  }

  @Getter
  @Setter
  public static class PathNode {
    private String path;
    private String backupFrequency;
    private Boolean secure = false;
    private Boolean watch = false;
    private ZkProtoUtil.ProtoEnum proto;
    private String dataType = "";
    private Boolean regex = false;

    public void setProto(String proto) {
      this.proto = ZkProtoUtil.ProtoEnum.valueOf(proto);
    }

    @Override
    public String toString() {
      return path;
    }
  }

  public PathNode findZkPathNodeUsingPath(String zkPath) {
    return zookeeperNodes.stream().filter(pathNode ->
      zkPath.startsWith(pathNode.getPath())).findAny().orElse(null);
  }

  public ZkProtoUtil.ProtoEnum findProtoEnumFromPath(String zkPath) {
    PathNode pathNode = findZkPathNodeUsingPath(zkPath);
    if (pathNode != null) {
      return pathNode.getProto();
    }
    return null;
  }

  public void sortMaxBackupEntityCountVsAOSVersions(
      List<MaxBackupEntityCountVsAOSVersion> maxBackupEntityCountVsAOSVersions) {
    maxBackupEntityCountVsAOSVersions.sort(
        (entityCountVersionMap1, entityCountVersionMap2) -> {
          if (PCUtil.comparePEVersions(entityCountVersionMap1.getMinVersion(),
                                       entityCountVersionMap2.getMinVersion())) {
            return 1;
          } else {
            return -1;
          }
        });
  }
}
