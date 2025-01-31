package com.nutanix.prism.pcdr.restserver.factory;

/**
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: shyam.sodankoor@nutanix.com
 *
 * Helper to get the type of BackupTarget based on
 * the input received
 */

import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.restserver.util.BackupTargetValidator;
import com.nutanix.prism.pcdr.restserver.util.ClusterValidator;
import com.nutanix.prism.pcdr.restserver.util.ObjectStoreValidator;
import dp1.pri.prism.v4.management.BackupTarget;
import dp1.pri.prism.v4.management.ClusterLocation;
import dp1.pri.prism.v4.management.ObjectStoreLocation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class BackupTargetHelper {

    private final ClusterValidator clusterValidator;

    private final ObjectStoreValidator objectStoreValidator;

    @Autowired
    public BackupTargetHelper(ClusterValidator clusterValidator, ObjectStoreValidator objectStoreValidator) {
        this.clusterValidator = clusterValidator;
        this.objectStoreValidator = objectStoreValidator;
    }


    public BackupTargetValidator getBackupTargetValidator(BackupTarget backupTarget) throws PCResilienceException {

        if (backupTarget.getLocation() instanceof ClusterLocation){
            return clusterValidator;
        }
        else if (backupTarget.getLocation() instanceof ObjectStoreLocation){
            return objectStoreValidator;
        }
        else{
            log.error("Unable to get the right validator object");
            throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
        }
    }

}
