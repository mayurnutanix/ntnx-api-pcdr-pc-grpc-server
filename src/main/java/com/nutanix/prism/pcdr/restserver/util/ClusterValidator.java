package com.nutanix.prism.pcdr.restserver.util;

/**
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: shyam.sodankoor@nutanix.com
 *
 * Helper Class to Validate Cluster
 *
 */

import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import dp1.pri.prism.v4.management.BackupTarget;
import dp1.pri.prism.v4.management.ClusterLocation;
import dp1.pri.prism.v4.management.ClusterReference;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.Map;

@Component
public class ClusterValidator implements BackupTargetValidator{
    @Override
    public void validateBackupTarget(BackupTarget backupTarget) throws PCResilienceException {
        ClusterLocation cluster = (ClusterLocation) backupTarget.getLocation();
        ClusterReference prismElementClusterReference = (ClusterReference) cluster.getConfig();
        if (StringUtils.isEmpty(prismElementClusterReference.getExtId())){
            Map<String,String> errorArguments = new HashMap<>();
            errorArguments.put(ErrorCodeArgumentMapper.ARG_CLUSTER_EXT_ID,prismElementClusterReference.getExtId());
            throw new PCResilienceException(ErrorMessages.getInvalidInputProvidedMessage(ErrorArgumentKey.CLUSTER_EXT_ID),
                    ErrorCode.PCBR_FETCH_CLUSTER_FAILURE,HttpStatus.BAD_REQUEST,errorArguments);
        }
    }
}
