package com.nutanix.prism.pcdr.restserver.util;

/**
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: shyam.sodankoor@nutanix.com
 *
 * Helper Class to Validate Object Store
 *
 */

import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.factory.ObjectStoreHelperFactory;
import com.nutanix.prism.pcdr.services.ObjectStoreHelper;
import com.nutanix.prism.pcdr.util.CertificatesUtility;
import com.nutanix.prism.pcdr.util.S3ObjectStoreUtil;
import dp1.pri.prism.v4.management.AWSS3Config;
import dp1.pri.prism.v4.management.AccessKeyCredentials;
import dp1.pri.prism.v4.management.BackupTarget;
import dp1.pri.prism.v4.management.ObjectStoreLocation;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

import static com.nutanix.prism.pcdr.constants.Constants.ENDPOINT_FLAVOR_OBJECTS;


@Slf4j
@Component
public class ObjectStoreValidator implements BackupTargetValidator {

    private BackupTargetUtil backupTargetUtil;

    @Value("${prism.pcdr.rpo.min.value.seconds:3600}")
    private int rpoMinValueSecs;

    @Value("${prism.pcdr.rpo.max.value.seconds:86400}")
    private int rpoMaxValueSecs;

    @Autowired
    public ObjectStoreValidator(BackupTargetUtil backupTargetUtil) {
        this.backupTargetUtil = backupTargetUtil;
    }

    @Override
    public void validateBackupTarget(BackupTarget backupTarget) throws PCResilienceException {
        validateObjectStore((ObjectStoreLocation) backupTarget.getLocation());
    }

    public void validateObjectStore(ObjectStoreLocation objectStore) throws PCResilienceException {
        if (objectStore.getProviderConfig() instanceof AWSS3Config){
            AWSS3Config awss3Config = (AWSS3Config) objectStore.getProviderConfig();
            if (!ObjectUtils.isEmpty(awss3Config.getCredentials())) {
                AccessKeyCredentials accessKeyCredentials = (AccessKeyCredentials) awss3Config.getCredentials();
                if (StringUtils.isEmpty(accessKeyCredentials.getAccessKeyId()) ||
                        StringUtils.isEmpty(accessKeyCredentials.getSecretAccessKey())) {
                    throw new PCResilienceException(ErrorMessages.EMPTY_CREDENTIAL_KEY_ERROR,
                            ErrorCode.PCBR_INVALID_BUCKET_DETAILS, HttpStatus.BAD_REQUEST);
                }
            }
        }

        if(ObjectUtils.isEmpty(objectStore.getBackupPolicy())) {
            throw new PCResilienceException(ErrorMessages.BACKUP_POLICY_MISSING_IN_PAYLOAD,
                    ErrorCode.PCBR_BACKUP_POLICY_NOT_PRESENT_IN_PAYLOAD, HttpStatus.BAD_REQUEST);
        }
    }

    public void validateObjectStoreForUpdate(ObjectStoreLocation objectStore, String extId) throws PCResilienceException {

        // Validate the AccessCredentials for update as well
        validateObjectStore(objectStore);
        BackupTarget currentBackupTarget = backupTargetUtil.getBackupTarget(extId);
        if(currentBackupTarget.getLocation() instanceof ObjectStoreLocation) {
            ObjectStoreLocation currentObjectStore = (ObjectStoreLocation) currentBackupTarget.getLocation();
            ObjectStoreEndPointDto providedObjectStoreDto = getObjectStoreDtoFromObjectStoreLocation(objectStore);
            ObjectStoreEndPointDto currentObjectStoreDto = getObjectStoreDtoFromObjectStoreLocation(currentObjectStore);
            ObjectStoreHelper objectStoreHelper = ObjectStoreHelperFactory.getObjectStoreHelper(
                    S3ServiceUtil.getObjectStoreEndpointFlavour(objectStore).toString());
            objectStoreHelper.validateParamsWithProvidedObjectStoreEndpoint(providedObjectStoreDto, currentObjectStoreDto);
        }
        else {
            throw new PCResilienceException(ErrorMessages.INVALID_EXTID_FOR_OBJECT_STORE,
                    ErrorCode.PCBR_BACKUP_TARGET_IDENTIFIER_MATCH_FAILURE, HttpStatus.BAD_REQUEST);
        }
    }

    public void validateRpo(Integer rpoSeconds) throws PCResilienceException {

        if (rpoSeconds == null || rpoSeconds < rpoMinValueSecs
                || rpoSeconds > rpoMaxValueSecs) {
            Map<String, String> errorArguments = new HashMap<>();
            errorArguments.put(ErrorCodeArgumentMapper.ARG_ERROR,
                    String.format("minimum allowed value is %s sec and maximum allowed value is %s sec", rpoMinValueSecs, rpoMaxValueSecs));
            throw new PCResilienceException(ErrorMessages.getInvalidRPOConfigMessage(rpoMinValueSecs, rpoMaxValueSecs),
                    ErrorCode.PCBR_INVALID_RPO_DETAILS, HttpStatus.BAD_REQUEST,errorArguments);
        }
    }

    public void validatePcObjectStoreEndpoint(PcObjectStoreEndpoint pcObjectStoreEndpoint) throws PCResilienceException {
      this.validateRpo(pcObjectStoreEndpoint.getRpoSeconds());
      if (pcObjectStoreEndpoint.getSkipTLS()) {
        log.error("Skip TLS not supported");
        throw new PCResilienceException("SkipTLS not supported",
            ErrorCode.PCBR_SKIPTLS_UNSUPPORTED, HttpStatus.BAD_REQUEST);
      }
      S3ObjectStoreUtil.isValidBucketName(pcObjectStoreEndpoint.getBucket(),
          pcObjectStoreEndpoint.getEndpointFlavour().toString());
      if (pcObjectStoreEndpoint.getEndpointFlavour().toString().equalsIgnoreCase(ENDPOINT_FLAVOR_OBJECTS)) {
        if (ObjectUtils.isEmpty(pcObjectStoreEndpoint.getEndpointCredentials()) ||
            ObjectUtils.isEmpty(pcObjectStoreEndpoint.getEndpointCredentials().getAccessKey()) ||
            ObjectUtils.isEmpty(pcObjectStoreEndpoint.getEndpointCredentials().getSecretAccessKey())) {
          throw new PCResilienceException(ErrorMessages.EMPTY_CREDENTIAL_KEY_ERROR,
              ErrorCode.PCBR_INVALID_BUCKET_DETAILS, HttpStatus.BAD_REQUEST);
        }
        if (ObjectUtils.isEmpty(pcObjectStoreEndpoint.getIpAddressOrDomain())) {
          String error = "Neither IP address nor fqdn provided";
          throw new PCResilienceException(error,
              ErrorCode.PCBR_EMPTY_IPADDRESS_OR_HOSTNAME, HttpStatus.BAD_REQUEST);
        }
        if (!S3ObjectStoreUtil.isValidHostnameOrIP(pcObjectStoreEndpoint.getIpAddressOrDomain())) {
          String error = String.format("Provided hostname or IP: %s is invalid", pcObjectStoreEndpoint.getIpAddressOrDomain());
          throw new PCResilienceException(error,
              ErrorCode.PCBR_INVALID_HOSTNAME, HttpStatus.BAD_REQUEST);
        }
        if (pcObjectStoreEndpoint.getEndpointCredentials().getCertificate() != null) {
          pcObjectStoreEndpoint.getEndpointCredentials().setCertificate(
              CertificatesUtility.normalizeCertificateContent(pcObjectStoreEndpoint.getEndpointCredentials().getCertificate()));
          CertificatesUtility.validateCertificates(pcObjectStoreEndpoint.getEndpointCredentials().getCertificate());
        }
      }
    }

    private ObjectStoreEndPointDto getObjectStoreDtoFromObjectStoreLocation(ObjectStoreLocation objectStoreLocation) {

        ObjectStoreEndPointDto objectStoreEndPointDto = null;
        if (objectStoreLocation.getProviderConfig() instanceof AWSS3Config) {
            AWSS3Config awss3Config = (AWSS3Config) objectStoreLocation.getProviderConfig();
            objectStoreEndPointDto = new ObjectStoreEndPointDto(awss3Config.getBucketName(), awss3Config.getRegion());
        }
        return objectStoreEndPointDto;
    }
}
