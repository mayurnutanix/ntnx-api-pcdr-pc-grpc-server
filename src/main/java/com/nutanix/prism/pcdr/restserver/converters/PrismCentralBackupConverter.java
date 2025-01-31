package com.nutanix.prism.pcdr.restserver.converters;

/**
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: shyam.sodankoor@nutanix.com
 *
 * Adapter for Prism Central Backup Service Controller
 */

import com.google.common.net.InternetDomainName;
import com.nutanix.api.utils.type.DateUtils;
import dp1.pri.common.v1.config.FQDN;
import dp1.pri.common.v1.config.IPAddressOrFQDN;
import dp1.pri.common.v1.config.IPv4Address;
import dp1.pri.common.v1.config.IPv6Address;
import dp1.pri.prism.v4.management.*;
import dp1.pri.prism.v4.protectpc.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.util.InetAddressUtils;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.util.ArrayList;
import java.util.List;

import static com.nutanix.prism.pcdr.restserver.constants.Constants.DEFAULT_OBJECTSTORE_BACKUP_RETENTION_IN_DAYS;


@Slf4j
@Service
public class PrismCentralBackupConverter {

  public IPAddressOrFQDN getIPAddressOrFQDNFromString(String iPAddressOrDomain) {
    if (!ObjectUtils.isEmpty(iPAddressOrDomain)) {
      if (InetAddressUtils.isIPv4Address(iPAddressOrDomain)) {
        IPv4Address iPv4Address = IPv4Address.IPv4AddressBuilder().value(iPAddressOrDomain).build();
        return IPAddressOrFQDN.IPAddressOrFQDNBuilder().ipv4(iPv4Address).build();
      } else if (InetAddressUtils.isIPv6Address(iPAddressOrDomain)) {
        IPv6Address iPv6Address = IPv6Address.IPv6AddressBuilder().value(iPAddressOrDomain).build();
        return IPAddressOrFQDN.IPAddressOrFQDNBuilder().ipv6(iPv6Address).build();
      } else if (InternetDomainName.isValid(iPAddressOrDomain)) {
        FQDN fqdn = FQDN.FQDNBuilder().value(iPAddressOrDomain).build();
        return IPAddressOrFQDN.IPAddressOrFQDNBuilder().fqdn(fqdn).build();
      }
    }
    return null;
  }

  public String getStringFromIPAddressOrFQDN(IPAddressOrFQDN ipAddressOrFQDN) {
    if (!ObjectUtils.isEmpty(ipAddressOrFQDN)){
      if (ipAddressOrFQDN.hasIpv4())
        return ipAddressOrFQDN.getIpv4().getValue();
      else if (ipAddressOrFQDN.hasIpv6())
        return ipAddressOrFQDN.getIpv6().getValue();
      else if (ipAddressOrFQDN.hasFqdn())
        return ipAddressOrFQDN.getFqdn().getValue();
    }
    return null;
  }

  public BackupTargets getBackupTargetsfromBackupTarget(BackupTarget backupTarget) {
    BackupTargets backupTargets = new BackupTargets();
    List<PcObjectStoreEndpoint> pcObjectStoreEndpointList = new ArrayList<>();
    if (backupTarget.getLocation() instanceof ClusterLocation){
      List<String> clusterUuidList = new ArrayList<>();
      ClusterLocation cluster = (ClusterLocation) backupTarget.getLocation();
      ClusterReference prismElementCluster = (ClusterReference) cluster.getConfig();
      clusterUuidList.add(prismElementCluster.getExtId());
      backupTargets.setClusterUuidList(clusterUuidList);
    } else {
      PcObjectStoreEndpoint pcObjectStoreEndpoint = getPcObjectStoreEndpointFromObjectStore(
              (ObjectStoreLocation) backupTarget.getLocation());
      pcObjectStoreEndpointList.add(pcObjectStoreEndpoint);
      backupTargets.setObjectStoreEndpointList(pcObjectStoreEndpointList);
    }

    return backupTargets;
  }
  public BackupTarget getBackupTargetFromPEInfo(PEInfo peInfo) {
    BackupTarget backupTarget = null;

    if (!ObjectUtils.isEmpty(peInfo)) {
      backupTarget = new BackupTarget();
      if (!ObjectUtils.isEmpty(peInfo.getLastSyncTimestamp()) &&
              peInfo.getLastSyncTimestamp() != 0)
        backupTarget.setLastSyncTime(DateUtils.fromEpochMicros(peInfo.getLastSyncTimestamp()));
      backupTarget.setIsBackupPaused(peInfo.getIsBackupPaused());
      backupTarget.setBackupPauseReason(peInfo.getPauseBackupMessage());
      backupTarget.setExtId(peInfo.getPeClusterId());

      if (!ObjectUtils.isEmpty(peInfo.getPeName()) || !ObjectUtils.isEmpty(peInfo.getPeClusterId())) {
        ClusterLocation cluster = new ClusterLocation();
        ClusterReference prismElementClusterReference = new ClusterReference();
        prismElementClusterReference.setExtId(peInfo.getPeClusterId());
        prismElementClusterReference.setName(peInfo.getPeName());
        cluster.setConfig(prismElementClusterReference);
        backupTarget.setLocationInWrapper(cluster);
      }
    }
    return backupTarget;
  }

  public PEInfo getPEInfoFromBackupTarget(BackupTarget backupTarget) {

    PEInfo peInfo = null;
    if (!ObjectUtils.isEmpty(backupTarget)) {
      peInfo = new PEInfo();
      peInfo.setIsBackupPaused(backupTarget.getIsBackupPaused());
      peInfo.setPauseBackupMessage(backupTarget.getBackupPauseReason());
      if (!ObjectUtils.isEmpty(backupTarget.getLastSyncTime()))
        peInfo.setLastSyncTimestamp(DateUtils.toEpochMicros(backupTarget.getLastSyncTime()));
      if (!ObjectUtils.isEmpty(backupTarget.getLocation())) {
        ClusterLocation cluster = (ClusterLocation) backupTarget.getLocation();
        ClusterReference prismElementCluster = (ClusterReference) cluster.getConfig();
        peInfo.setPeName(prismElementCluster.getName());
        peInfo.setPeClusterId(prismElementCluster.getExtId());
      }
    }

    return peInfo;
  }

  public AccessKeyCredentials getAccessKeyCredentialsFromPcEndpointCredentials(PcEndpointCredentials pcEndpointCredentials) {
    AccessKeyCredentials accessKeyCredentials = null;
    if (!ObjectUtils.isEmpty(pcEndpointCredentials)) {
      accessKeyCredentials = new AccessKeyCredentials();
      accessKeyCredentials.setAccessKeyId(pcEndpointCredentials.getAccessKey());
      accessKeyCredentials.setSecretAccessKey(pcEndpointCredentials.getSecretAccessKey());
    }
    return accessKeyCredentials;
  }

  public PcEndpointCredentials getPcEndpointCredentialsFromAccessKeyCredentials(AccessKeyCredentials accessKeyCredentials) {
    PcEndpointCredentials pcEndpointCredentials = null;
    if (!ObjectUtils.isEmpty(accessKeyCredentials)) {
      pcEndpointCredentials = new PcEndpointCredentials();
      pcEndpointCredentials.setAccessKey(accessKeyCredentials.getAccessKeyId());
      pcEndpointCredentials.setSecretAccessKey(accessKeyCredentials.getSecretAccessKey());
    }
    return pcEndpointCredentials;
  }

  public ObjectStoreLocation getObjectStoreFromPcObjectStoreEndpoint(PcObjectStoreEndpoint pcObjectStoreEndpoint) {
    ObjectStoreLocation objectStore = null;

    if (!ObjectUtils.isEmpty(pcObjectStoreEndpoint)) {
      objectStore = new ObjectStoreLocation();
      if (pcObjectStoreEndpoint.getEndpointFlavour() == PcEndpointFlavour.KS3) {
        AWSS3Config awsS3Config = new AWSS3Config(
                pcObjectStoreEndpoint.getBucket(),
                pcObjectStoreEndpoint.getRegion(),this.getAccessKeyCredentialsFromPcEndpointCredentials(
                pcObjectStoreEndpoint.getEndpointCredentials()));
        objectStore.setProviderConfig(awsS3Config);
      } // TODO : Uncomment once NutanixObjectStore is supported
            /* else {
                NutanixObjectsConfig nutanixObjectsConfig = new NutanixObjectsConfig();
                nutanixObjectsConfig.setPort(pcObjectStoreEndpoint.getPort()!=null ? Integer.parseInt(pcObjectStoreEndpoint.getPort()) : null);
                nutanixObjectsConfig.setRegion(pcObjectStoreEndpoint.getRegion());
                nutanixObjectsConfig.setIpAddressOrHostname(this.getIPAddressOrFQDNFromString(pcObjectStoreEndpoint.getIpAddressOrDomain()));
                nutanixObjectsConfig.setIsTLSEnabled(!pcObjectStoreEndpoint.getSkipTLS());
                nutanixObjectsConfig.setBucketName(pcObjectStoreEndpoint.getBucket());
                nutanixObjectsConfig.setCredentialsInWrapper(this.getAccessKeyCredentialsFromPcEndpointCredentials(
                    pcObjectStoreEndpoint.getEndpointCredentials()));
                objectStore.setProviderConfig(nutanixObjectsConfig);
            }*/
      objectStore.setBackupPolicy(new BackupPolicy(pcObjectStoreEndpoint.getRpoSeconds() != null
              ? pcObjectStoreEndpoint.getRpoSeconds() / 60 : null));
    }
    return objectStore;
  }

  public PcObjectStoreEndpoint getPcObjectStoreEndpointFromObjectStore(ObjectStoreLocation objectStore) {

    PcObjectStoreEndpoint pcObjectStoreEndpoint = null;

    if (!ObjectUtils.isEmpty(objectStore)) {
      pcObjectStoreEndpoint = new PcObjectStoreEndpoint();
      if (!ObjectUtils.isEmpty(objectStore.getProviderConfig())){
        if (objectStore.getProviderConfig() instanceof AWSS3Config) {
          AWSS3Config awss3Config = (AWSS3Config) objectStore.getProviderConfig();
          pcObjectStoreEndpoint.setBucket(awss3Config.getBucketName());
          pcObjectStoreEndpoint.setRegion(awss3Config.getRegion());
          pcObjectStoreEndpoint.setEndpointFlavour(PcEndpointFlavour.KS3);
          pcObjectStoreEndpoint.setEndpointCredentials(this.getPcEndpointCredentialsFromAccessKeyCredentials
                  ((AccessKeyCredentials)awss3Config.getCredentials()));
        } // TODO : Uncomment once NutanixObjectStore is supported
                /* else {
                    NutanixObjectsConfig nutanixObjectsConfig = (NutanixObjectsConfig) objectStore.getProviderConfig();
                    pcObjectStoreEndpoint.setBucket(nutanixObjectsConfig.getBucketName());
                    pcObjectStoreEndpoint.setRegion(nutanixObjectsConfig.getRegion());
                    pcObjectStoreEndpoint.setIpAddressOrDomain(this.getStringFromIPAddressOrFQDN(nutanixObjectsConfig.getIpAddressOrHostname()));
                    pcObjectStoreEndpoint.setPort(nutanixObjectsConfig.getPort()!=null ? Integer.toString(nutanixObjectsConfig.getPort()) : null);
                    pcObjectStoreEndpoint.setSkipTLS(!nutanixObjectsConfig.getIsTLSEnabled());
                    pcObjectStoreEndpoint.setEndpointCredentials(this.getPcEndpointCredentialsFromAccessKeyCredentials
                            ((AccessKeyCredentials) nutanixObjectsConfig.getCredentials()));
                }*/
      }

      if (!ObjectUtils.isEmpty(objectStore.getBackupPolicy())) {
        pcObjectStoreEndpoint.setBackupRetentionDays(DEFAULT_OBJECTSTORE_BACKUP_RETENTION_IN_DAYS);
        if (objectStore.getBackupPolicy().getRpoInMinutes() != null)
          pcObjectStoreEndpoint.setRpoSeconds(objectStore.getBackupPolicy().getRpoInMinutes()*60);
      }
    }
    return pcObjectStoreEndpoint;

  }

  public BackupTarget getBackupTargetFromObjectStoreEndpointInfo(ObjectStoreEndpointInfo objectStoreEndpointInfo) {
    BackupTarget backupTarget = null;

    if (!ObjectUtils.isEmpty(objectStoreEndpointInfo)) {
      backupTarget = new BackupTarget();
      backupTarget.setLocationInWrapper(this.getObjectStoreFromPcObjectStoreEndpoint(objectStoreEndpointInfo.getObjectStoreEndpoint()));
      backupTarget.setIsBackupPaused(objectStoreEndpointInfo.getIsBackupPaused());
      backupTarget.setExtId(objectStoreEndpointInfo.getEntityId());
      if (!ObjectUtils.isEmpty(objectStoreEndpointInfo.getLastSyncTimestamp()) &&
              objectStoreEndpointInfo.getLastSyncTimestamp() != 0)
        backupTarget.setLastSyncTime(DateUtils.fromEpochMicros(objectStoreEndpointInfo.getLastSyncTimestamp()));
      backupTarget.setBackupPauseReason(objectStoreEndpointInfo.getPauseBackupMessage());
      backupTarget.setExtId(objectStoreEndpointInfo.getEntityId());
    }

    return backupTarget;
  }

  public ObjectStoreEndpointInfo getObjectStoreEndpointInfoFromBackupTarget(BackupTarget backupTarget) {
    ObjectStoreEndpointInfo objectStoreEndpointInfo = null;

    if (!ObjectUtils.isEmpty(backupTarget)) {
      objectStoreEndpointInfo = new ObjectStoreEndpointInfo();
      objectStoreEndpointInfo.setObjectStoreEndpoint(this.getPcObjectStoreEndpointFromObjectStore((ObjectStoreLocation) backupTarget.getLocation()));
      objectStoreEndpointInfo.setIsBackupPaused(backupTarget.getIsBackupPaused());
      objectStoreEndpointInfo.setPauseBackupMessage(backupTarget.getBackupPauseReason());
      objectStoreEndpointInfo.setEntityId(backupTarget.getExtId());
      if (!ObjectUtils.isEmpty(backupTarget.getLastSyncTime()))
        objectStoreEndpointInfo.setLastSyncTimestamp(DateUtils.toEpochMicros(backupTarget.getLastSyncTime()));
    }

    return objectStoreEndpointInfo;
  }

  public List<BackupTarget> getListBackupTargetFromBackupTargetsInfo(BackupTargetsInfo backupTargetsInfo) {
    List<BackupTarget> backupTargetList = new ArrayList<>();
    List<PEInfo> peInfoList = backupTargetsInfo.getPeInfoList();
    for (PEInfo peInfo: backupTargetsInfo.getPeInfoList()) {
      backupTargetList.add(this.getBackupTargetFromPEInfo(peInfo));
    }
    for (ObjectStoreEndpointInfo objectStoreEndpointInfo: backupTargetsInfo.getObjectStoreEndpointInfoList()) {
      backupTargetList.add(this.getBackupTargetFromObjectStoreEndpointInfo(objectStoreEndpointInfo));
    }
    return backupTargetList;
  }

  public BackupTargetsInfo getBackupTargetsInfoFromListBackupTarget(List<BackupTarget> backupTargetList) {
    BackupTargetsInfo backupTargetsInfo = new BackupTargetsInfo();
    List<PEInfo> peInfoList = new ArrayList<>();
    List<ObjectStoreEndpointInfo> objectStoreEndpointInfoList = new ArrayList<>();

    for (BackupTarget backupTarget: backupTargetList) {
      if (backupTarget.getLocation() instanceof ClusterLocation) {
        peInfoList.add(this.getPEInfoFromBackupTarget(backupTarget));
      } else {
        objectStoreEndpointInfoList.add(this.getObjectStoreEndpointInfoFromBackupTarget(backupTarget));
      }
    }
    backupTargetsInfo.setPeInfoList(peInfoList);
    backupTargetsInfo.setObjectStoreEndpointInfoList(objectStoreEndpointInfoList);

    return backupTargetsInfo;
  }
}
