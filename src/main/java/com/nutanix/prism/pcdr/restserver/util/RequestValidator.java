/**
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 */
package com.nutanix.prism.pcdr.restserver.util;

import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
import dp1.pri.prism.v4.management.BackupTarget;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.LocaleResolver;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;


/**
 * Validates the incoming request with pre-conditions.
 */
@Slf4j
@Component
public class RequestValidator {

    private PCBackupService pcBackupService;
    private LocaleResolver localeResolver;

    @Autowired
    public RequestValidator(PCBackupService pcBackupService, LocaleResolver localeResolver){
        this.pcBackupService = pcBackupService;
        this.localeResolver = localeResolver;
    }

    /**
     * Checks if the etag from the request matches with the generated etag from the backup target. If the tag does not
     * match, throws 412 - Precondition failed.
     *
     * @param etag         etag from incoming request.
     * @param backupTarget backup target object.
     * @throws PCResilienceException
     */
    public void checkIfEtagMatch(String etag, BackupTarget backupTarget)
            throws PCResilienceException {
        final String generatedEtag = pcBackupService.generateEtag(backupTarget);
        boolean isMatch = StringUtils.equals(etag, generatedEtag);
        if (!isMatch) {
            if (log.isDebugEnabled()) {
                String message = String.format("Generated Etag %s is not matching.", generatedEtag);
                log.debug(message);
            }
            // todo: check if correct value passed for old and new etag, old -> generatedEtag, new -> etag
            throw new PCResilienceException(ErrorMessages.getEtagMisMatchErrorMessage("backup target", generatedEtag, etag),
                    ErrorCode.PCBR_RESOURCE_ETAG_MISMATCH, HttpStatus.PRECONDITION_FAILED);
        }
    }

    public String resolveLocale(HttpServletRequest request) {
        Locale locale = localeResolver.resolveLocale(request);
        return locale.toLanguageTag();
    }

    public void validatePcClusterUuid(String pcClusterUuidFromRequest, String pcClusterUuidFromZeus) throws PCResilienceException {

        if (!pcClusterUuidFromRequest.equals(pcClusterUuidFromZeus)){
            Map<String,String> errorArguments = new HashMap<>();
            errorArguments.put(ErrorCodeArgumentMapper.ARG_EXT_ID, pcClusterUuidFromRequest);
            throw new PCResilienceException("PC Cluster UUID provided is not of the current PC",
                    ErrorCode.PCBR_PRISM_CENTRAL_NOT_FOUND, HttpStatus.NOT_FOUND, errorArguments);
        }
    }
}
