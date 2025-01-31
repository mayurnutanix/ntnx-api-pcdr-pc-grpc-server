/**
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 */
package com.nutanix.prism.pcdr.restserver.handlers;

import com.google.protobuf.ByteString;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
import com.nutanix.prism.pcdr.restserver.services.impl.PCTaskServiceHelper;
import com.nutanix.prism.pcdr.restserver.util.BackupTargetUtil;
import com.nutanix.prism.pcdr.restserver.util.RequestValidator;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.zklock.DistributedLock;
import dp1.pri.prism.v4.management.BackupTarget;
import dp1.pri.prism.v4.protectpc.BackupTargets;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.HashMap;

/**
 * This class handles all the aysnc messages from the queue. based on the type of action, takes a corresponding action.
 */
@Component
@Slf4j
public class AsyncMessageHandler {

    private PCBackupService pcBackupService;

    private InstanceServiceFactory instanceServiceFactory;

    private PCTaskServiceHelper pcTaskServiceHelper;

    private ErgonServiceHelper ergonServiceHelper;

    private RequestValidator requestValidator;

    private BackupTargetUtil backupTargetUtil;

    @Autowired
    public AsyncMessageHandler(PCBackupService pcBackupService,
                               InstanceServiceFactory instanceServiceFactory, PCTaskServiceHelper pcTaskServiceHelper,
                               ErgonServiceHelper ergonServiceHelper, RequestValidator requestValidator, BackupTargetUtil backupTargetUtil) {
        this.pcBackupService = pcBackupService;
        this.instanceServiceFactory = instanceServiceFactory;
        this.pcTaskServiceHelper = pcTaskServiceHelper;
        this.ergonServiceHelper = ergonServiceHelper;
        this.requestValidator = requestValidator;
        this.backupTargetUtil = backupTargetUtil;
    }

    ExceptionDetailsDTO exceptionDetails = new ExceptionDetailsDTO(ErrorMessages.INTERNAL_SERVER_ERROR,
            ErrorCode.PCBR_INTERNAL_SERVER_ERROR, HttpStatus.INTERNAL_SERVER_ERROR, new HashMap<>());

    /**
     * Executes action for delete backup target.
     */
    @Async("adonisServiceThreadPool")
    public void deleteBackupTargetAsync(ByteString taskId, String eTag, String extId) throws ErgonException {

        DistributedLock distributedLock =
                instanceServiceFactory.getDistributedLockForBackupPCDR();
        try {
            // Do we need to make blocking: false
            if (!distributedLock.lock(true)) {
                log.warn(Constants.DISTRIBUTED_LOCKING_FAILED);
                ergonServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kFailed,
                        exceptionDetails, ErrorArgumentKey.DELETE_BACKUP_TARGET_OPERATION);
                return;
            }
            BackupTarget backupTarget = backupTargetUtil.getBackupTarget(extId);
            requestValidator.checkIfEtagMatch(eTag, backupTarget);
            pcBackupService.removeReplica(extId, taskId);
        } catch (Exception pe) {
            log.error("Exception while deleting Backup Target : ", pe);
            ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(pe);
            ergonServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kFailed,
                    exceptionDetails, ErrorArgumentKey.DELETE_BACKUP_TARGET_OPERATION);
        } finally {
            distributedLock.unlock();
        }
    }


    @Async("adonisServiceThreadPool")
    public void createBackupTargetAsync(BackupTargets backupTargets, ByteString taskId) throws ErgonException {

        DistributedLock distributedLock =
                instanceServiceFactory.getDistributedLockForBackupPCDR();
        try {
            // Do we need to make blocking: false
            if (!distributedLock.lock(true)) {
                log.warn(Constants.DISTRIBUTED_LOCKING_FAILED);
                ergonServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kFailed,
                        exceptionDetails, ErrorArgumentKey.CREATE_BACKUP_TARGET_OPERATION);
                return;
            }
            pcBackupService.createBackupTarget(taskId, backupTargets);
        } catch (Exception pe) {
            log.error("Exception while create Backup Target : ", pe);
            ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(pe);
            ergonServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kFailed,
                    exceptionDetails, ErrorArgumentKey.CREATE_BACKUP_TARGET_OPERATION);
        } finally {
            distributedLock.unlock();
        }
    }

    @Async("adonisServiceThreadPool")
    public void updateBackupTargetAsync(BackupTargets backupTargets, ByteString taskId, String eTag, String extId) throws ErgonException {

        DistributedLock distributedLock =
                instanceServiceFactory.getDistributedLockForBackupPCDR();
        try {
            if (!distributedLock.lock(true)) {
                log.warn(Constants.DISTRIBUTED_LOCKING_FAILED);
                ergonServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kFailed,
                        exceptionDetails, ErrorArgumentKey.UPDATE_BACKUP_TARGET_OPERATION);
                return;
            }
            BackupTarget backupTarget = backupTargetUtil.getBackupTarget(extId);
            requestValidator.checkIfEtagMatch(eTag, backupTarget);
            pcBackupService.updateBackupTarget(taskId, backupTargets, extId);
        }  catch (Exception pe) {
            log.error("Exception while updating Backup Target : ", pe);
            ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(pe);
            ergonServiceHelper.updateTaskStatus(taskId, ErgonTypes.Task.Status.kFailed,
                    exceptionDetails, ErrorArgumentKey.UPDATE_BACKUP_TARGET_OPERATION);
        } finally {
            distributedLock.unlock();
        }
    }
}
