/*
 * Copyright (c) 2022 Nutanix Inc. All rights reserved.
 *
 * Author: kumar.gaurav@nutanix.com
 *
 */
package com.nutanix.prism.pcdr.restserver.util;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.BinaryUtils;
import com.amazonaws.util.Md5Utils;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.nutanix.api.utils.json.JsonUtils;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto;
import com.nutanix.prism.pcdr.PcBackupMetadataProto;
import com.nutanix.prism.pcdr.constants.ObjectStoreProviderEnum;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.util.*;
import com.nutanix.prism.pcdr.util.ObjectStoreClientInstanceFactory;
import dp1.pri.prism.v4.management.AWSS3Config;
import dp1.pri.prism.v4.management.NutanixObjectsConfig;
import dp1.pri.prism.v4.management.ObjectStoreLocation;
import dp1.pri.prism.v4.protectpc.PcEndpointCredentials;
import dp1.pri.prism.v4.protectpc.PcEndpointFlavour;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import java.util.Base64;
import java.util.List;

import static com.nutanix.prism.pcdr.constants.Constants.IS_PATH_STYLE_ENDPOINT_ADDRESS_STORED_IN_IDF;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;

@Slf4j
public class S3ServiceUtil {

  static ObjectMapper objectMapper = JsonUtils.getObjectMapper();

  static {
    objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,
            false);
  }

  /**
   * Preventing instantiation of utility classes
   */
  private S3ServiceUtil() {
  }

  /**
   * Helper method to calculate MD5 checksum
   * @param contentAsBytes
   * @return String: md5 checksum
   */
  // TODO: Check if can remove
  public static String calculateMD5Checksum(byte[] contentAsBytes) {
    byte[] destCalculatedMD5Hash = Md5Utils.computeMD5Hash(contentAsBytes);
    return BinaryUtils.toHex(destCalculatedMD5Hash);
  }

  /**
   * Helper method to get pcSeedData object key in s3
   * @param pcClusterUuid
   * @return String: pcSeedData object key
   */
  // TODO: Can move to common, PE also have same method
  public static String getPcSeedDataObjectKey(String pcClusterUuid) {
    return String.format(PC_SEED_DATA_OBJECT_KEY_STR, pcClusterUuid);
  }

  /**
   * Helper method to get endpoint address with base object key in s3
   * @param objectStoreEndpointAddress
   * @param pcClusterUuid
   * @return String: pcSeedData object key
   */
  public static String getEndpointAddressWithBaseObjectKey(
          String objectStoreEndpointAddress,String pcClusterUuid) {

    return String.format("%s/%s", objectStoreEndpointAddress,
            getBaseDirObjectKey(pcClusterUuid));
  }

  /**
   * Helper method to get base object key in s3
   * @param pcClusterUuid
   * @return String: pcSeedData object key
   */
  public static String getBaseDirObjectKey(String pcClusterUuid) {
    return String.format(PCDR_BASE_OBJECT_KEY_STR, pcClusterUuid);
  }

  /**
   * Helper method to fetch any existing metadata object for a PC based on it's
   * cluster-id.
   * @param objectStoreEndPointDto, pcclusterUuid
   */
  public static List<String> getExistingMetadataListForPc(
          final ObjectStoreEndPointDto objectStoreEndPointDto, final String pcClusterUuid,
          final String accessKey, final String secretAccessKey) {
    // Prefix of metadata object key which looks like -
    // pc_backup/pc_metadata!*<pc-cluster-uuid>
    final String keyPrefixWithClusterId =
            String.format("%s%s%s%s", OBJECTSTORE_PC_METADATA_BASE_PREFIX,
                    OBJECTSTORE_PC_METADATA_PREFIX,
                    OBJECTSTORE_PC_METADATA_SPLIT_REGEX, pcClusterUuid);
    String bucketName = objectStoreEndPointDto.getBucket();
    String error;
    try {

      // Fetch s3 client
      log.info(String.format("Fetching S3 client with bucketName: %s" +
              " and objectKey: %s", bucketName, keyPrefixWithClusterId));
      AmazonS3 s3Client =
              ObjectStoreClientInstanceFactory
                      .getS3Client(objectStoreEndPointDto, accessKey, secretAccessKey);
      final ObjectListing objectList =
              s3Client.listObjects(bucketName, keyPrefixWithClusterId);
      List<String> objectkeys = Lists.newArrayList();
      for (S3ObjectSummary s : objectList.getObjectSummaries()) {
        objectkeys.add(s.getKey());
      }
      return objectkeys;
    } catch (final AmazonS3Exception amazonS3Exception) {
      if (amazonS3Exception.getErrorCode().equals("NoSuchBucket")) {
        error = "Failed to fetch PC metadata from s3 as specified bucket doesn't exist in ObjectStore.";
        log.error(error, amazonS3Exception);
      }
      error = String.format(
              "Failed to fetch PC metadata from s3 due to exception: %s",
              amazonS3Exception.getMessage());
      log.error(error, amazonS3Exception);
    } catch (final AmazonClientException ex) {
      error = String.format(
              "Failed to fetch PC metadata from s3 due to exception: %s",
              ex.getMessage());
      log.error(error, ex);
    } catch (final Exception exception) {
      error = String.format("Failed to fetch PC metadata from s3 due to %s",
              exception.getMessage());
      log.error(error, exception);
    }
    return Lists.newArrayList();
  }

  /**
   * Helper method to access the metadata for a particular PC if already
   * present on s3.
   * @param objectkeys
   * @param pcClusterUuid
   * @return Null if doesn't exist, else the metadata object key string.
   * */
  public static String getPcMetadata(
          final List<String> objectkeys,
          final String pcClusterUuid) {
    // Iterate the summary list and match the pcClusterUuid.
    for (String key : objectkeys) {
      if (key.contains(pcClusterUuid)) {
        log.debug("PC metadata exist on s3 for cluster-id " + pcClusterUuid);
        return key;
      }
    }
    return null;
  }

  /**
   * Helper method to write PC metadata object string on S3 if not already
   * present.
   * @param metadataString, endPointAddress
   * @return Null if doesn't exist, else the metadata object key string.
   * */
  public static void writeMetadataObjectOnS3(
          final String metadataString,
          final ObjectStoreEndPointDto objectStoreEndPointDto,
          final String accessKey,
          final String secretAccessKey) {
    String bucketName = objectStoreEndPointDto.getBucket();
    String error;
    log.info("Writing metadata object on s3 "+ metadataString);
    try {
      // Skip bucket existence check since that would have been validated in
      // earlier steps.
      // Construct s3 put object request and create empty file with content 0
      final String checksum = ObjectStoreUtil.getChecksum(new byte[0]);
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength(0); // Set the content length to 0 for an empty object
      metadata.setContentMD5(checksum);

      final PutObjectRequest putObjectRequest =
              new PutObjectRequest(bucketName, metadataString,
                      new java.io.ByteArrayInputStream(new byte[0]), metadata);
      AmazonS3 s3Client =
              ObjectStoreClientInstanceFactory
                      .getS3Client(objectStoreEndPointDto, accessKey, secretAccessKey);
      s3Client.putObject(putObjectRequest);
    } catch (final AmazonServiceException ex) {
      error = String.format(
              "Failed to write PC metadata to s3 due to exception: %s",
              ex.getMessage());
      log.error(error, ex);
    } catch (final Exception exception) {
      error = String.format("Failed to write PC metadata to s3 due to %s",
              exception.getMessage());
      log.error(error, exception);
    }
  }

  /**
   * Helper method to delete PC metadata object string on S3 if not
   * present.
   * @param metadataObjectKey Key for PC backup metadata object.
   * @param objectStoreEndPointDto S3 endPoint Dto
   * @param accessKey S3 bucket access key
   * @param secretAccessKey S3 bucket secret access key
   * @return true if deletion is successful, false otherwise.
   * */
  public static boolean deleteMetadataObjectFromS3(
          final String metadataObjectKey,
          final ObjectStoreEndPointDto objectStoreEndPointDto,
          final String accessKey,
          final String secretAccessKey) {
    String bucketName = "";
    String error;
    log.info("Deleting object key on s3 "+ metadataObjectKey);
    try {
      // Parse region and bucket name from endpoint address.
      bucketName = objectStoreEndPointDto.getBucket();
      DeleteObjectsRequest.KeyVersion key =
              new DeleteObjectsRequest.KeyVersion(metadataObjectKey);
      List<DeleteObjectsRequest.KeyVersion> listofKeysToDelete =
              Lists.newArrayList();
      listofKeysToDelete.add(key);
      final DeleteObjectsRequest deleteObjectRequest =
              new DeleteObjectsRequest(bucketName);
      deleteObjectRequest.setKeys(listofKeysToDelete);
      AmazonS3 s3Client =
              ObjectStoreClientInstanceFactory
                      .getS3Client(objectStoreEndPointDto, accessKey, secretAccessKey);
      DeleteObjectsResult deleteResponse = s3Client.deleteObjects(deleteObjectRequest);
      // Check if the deleted objects count is 1.
      if (deleteResponse.getDeletedObjects().size() > 0) {
        log.info(String.format("Successfully deleted metadata object %s from s3 " +
                "bucket %s", metadataObjectKey, bucketName));
        return true;
      }
    } catch (final AmazonServiceException ex) {
      error = String.format(
              "Failed to delete PC metadata %s from s3 bucket %s: %s",
              metadataObjectKey, bucketName, ex.getMessage());
      log.error(error, ex);
      return false;
    } catch (final Exception exception) {
      error = String.format("Failed to write PC metadata to s3 due to %s",
              exception.getMessage());
      log.error(error, exception);
      return false;
    }
    log.warn(String.format("Unable to delete metadata object %s from s3 bucket %s",
            metadataObjectKey, bucketName));
    return false;
  }

  public static ObjectStoreProviderEnum getObjectStoreEndpointFlavour(ObjectStoreLocation objectStoreLocation) {

    if (objectStoreLocation.getProviderConfig() instanceof AWSS3Config){
      return ObjectStoreProviderEnum.AWS_S3;
    } else if (objectStoreLocation.getProviderConfig() instanceof NutanixObjectsConfig) {
      return ObjectStoreProviderEnum.NUTANIX_OBJECTS;
    }
    return null;
  }

  public static PcObjectStoreEndpoint getPcObjectStoreEndpointFromProtoPcBackupConfig(
          ClusterExternalStateProto.PcBackupConfig pcBackupConfig, PcBackupMetadataProto.PCVMBackupTargets pcvmBackupTargets) throws PCResilienceException {
    PcObjectStoreEndpoint pcObjectStoreEndpoint = new PcObjectStoreEndpoint();
    ClusterExternalStateProto.PcBackupConfig.ObjectStoreEndpoint objectStoreEndpoint
            = pcBackupConfig.getObjectStoreEndpoint();
    pcObjectStoreEndpoint.setEndpointAddress(objectStoreEndpoint.getEndpointAddress());
    pcObjectStoreEndpoint.setBucket(objectStoreEndpoint.getBucketName());
    pcObjectStoreEndpoint.setRegion(objectStoreEndpoint.getRegion());
    if (objectStoreEndpoint.getEndpointFlavour() == ClusterExternalStateProto.PcBackupConfig.ObjectStoreEndpoint.EndpointFlavour.kOBJECTS) {
      pcObjectStoreEndpoint.setIpAddressOrDomain(S3ObjectStoreUtil.getHostnameOrIPFromEndpointAddress(
              objectStoreEndpoint.getEndpointAddress(), IS_PATH_STYLE_ENDPOINT_ADDRESS_STORED_IN_IDF));
      pcObjectStoreEndpoint.setSkipCertificateValidation(objectStoreEndpoint.getSkipCertificateValidation());
      pcObjectStoreEndpoint.setHasCustomCertificate(ObjectUtils.isNotEmpty(objectStoreEndpoint.getCertificatePath()));
    }
    pcObjectStoreEndpoint.setEndpointFlavour(PcEndpointFlavour.fromString(
            objectStoreEndpoint.getEndpointFlavour().toString()));
    pcObjectStoreEndpoint.setBackupRetentionDays(objectStoreEndpoint.getBackupRetentionDays());
    pcObjectStoreEndpoint.setRpoSeconds(pcBackupConfig.getRpoSecs());
    pcObjectStoreEndpoint.setEndpointCredentials(new PcEndpointCredentials());

    if (!ObjectUtils.isEmpty(objectStoreEndpoint.getCertificatePath()) && !objectStoreEndpoint.getSkipCertificateValidation()) {
      PcBackupMetadataProto.PCVMBackupTargets.ObjectStoreBackupTarget objectStoreBackupTarget = pcvmBackupTargets.getObjectStoreBackupTargetsList().stream()
              .filter(objectStoreBackupTarget1 ->
                      objectStoreBackupTarget1.getCertificatePath().equals(objectStoreEndpoint.getCertificatePath()))
              .findFirst().orElse(null);
      if (!ObjectUtils.isEmpty(objectStoreBackupTarget)) {
        pcObjectStoreEndpoint.getEndpointCredentials().setCertificate(
                CertificatesUtility.getCertificateStringFromByteString(objectStoreBackupTarget.getCertificateContent()));
      } else {
        log.info("pc_backup_metadata and pc_backup_config inconsistent. Reading certificate content from file instead of pc_backup_metadata");
        pcObjectStoreEndpoint.getEndpointCredentials().setCertificate(
                CertificatesUtility.readCertificateFromPath(objectStoreEndpoint.getCertificatePath()));
      }
    }

    return pcObjectStoreEndpoint;
  }

  // extract from pc_backup_metadata
  public static PcObjectStoreEndpoint getPcObjectStoreEndpointFromPCVMObjectStoreBackupTarget(
          PcBackupMetadataProto.PCVMBackupTargets.ObjectStoreBackupTarget objectStoreBackupTarget) throws PCResilienceException {
    PcObjectStoreEndpoint pcObjectStoreEndpoint = new PcObjectStoreEndpoint();
    pcObjectStoreEndpoint.setEndpointAddress(objectStoreBackupTarget.getEndpointAddress());
    pcObjectStoreEndpoint.setBucket(objectStoreBackupTarget.getBucketName());
    pcObjectStoreEndpoint.setRegion(objectStoreBackupTarget.getRegion());
    PcEndpointCredentials pcEndpointCredentials = new PcEndpointCredentials();
    if (objectStoreBackupTarget.getEndpointFlavour() == PcBackupMetadataProto.PCVMBackupTargets.ObjectStoreBackupTarget.EndpointFlavour.kOBJECTS) {
      pcObjectStoreEndpoint.setIpAddressOrDomain(S3ObjectStoreUtil.getHostnameOrIPFromEndpointAddress(
              objectStoreBackupTarget.getEndpointAddress(), IS_PATH_STYLE_ENDPOINT_ADDRESS_STORED_IN_IDF));
      pcObjectStoreEndpoint.setSkipCertificateValidation(objectStoreBackupTarget.getSkipCertificateValidation());
      pcObjectStoreEndpoint.setHasCustomCertificate(ObjectUtils.isNotEmpty(objectStoreBackupTarget.getCertificatePath()));
      if (ObjectUtils.isNotEmpty(objectStoreBackupTarget.getCertificatePath())) {
        pcEndpointCredentials.setCertificate(
                CertificatesUtility.getCertificateStringFromByteString(objectStoreBackupTarget.getCertificateContent())
        );
      }
    }
    pcObjectStoreEndpoint.setEndpointFlavour(PcEndpointFlavour.fromString(
            objectStoreBackupTarget.getEndpointFlavour().toString()));
    pcObjectStoreEndpoint.setBackupRetentionDays(objectStoreBackupTarget.getBackupRetentionDays());
    pcObjectStoreEndpoint.setRpoSeconds(objectStoreBackupTarget.getRpoSecs());
    pcObjectStoreEndpoint.setEndpointCredentials(pcEndpointCredentials);
    return pcObjectStoreEndpoint;
  }

  public static ClusterExternalStateProto.ObjectStoreCredentials getCredentialsProtoFromEndpointCredentials(PcEndpointCredentials pcEndpointCredentials) throws PCResilienceException {
    ClusterExternalStateProto.ObjectStoreCredentials.Builder objectStoreEndPointCredentialBuilder = ClusterExternalStateProto.
            ObjectStoreCredentials.newBuilder();
    ObjectStoreUtil.setObjectToProto(pcEndpointCredentials,
            objectStoreEndPointCredentialBuilder);
    ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto = objectStoreEndPointCredentialBuilder.build();
    return objectStoreCredentialsProto;
  }

}
