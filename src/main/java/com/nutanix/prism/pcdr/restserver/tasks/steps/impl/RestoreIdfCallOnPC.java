/*
 *  Copyright (c) 2021 Nutanix Inc. All rights reserved.
 *  Author:kumar.gaurav@nutanix.com
 *  Description: This class is used when user is restoring PC from objectStore.
 */
package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.SerializationUtils;

import java.util.Objects;

import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.RESTORE_IDF_API_PERCENTAGE;

@Service
@Slf4j
public class RestoreIdfCallOnPC implements PcdrStepsHandler {
    @Autowired
    private EntityDBProxyImpl entityDBProxy;
    @Autowired
    private ErgonServiceHelper ergonServiceHelper;
    @Autowired
    private InstanceServiceFactory instanceServiceFactory;

    @Override
    public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                   ErgonTypes.Task currentSubtask)
            throws ErgonException, PCResilienceException {

    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    RestoreInternalOpaque restoreInternalOpaque =
      (RestoreInternalOpaque) Objects.requireNonNull(SerializationUtils.
              deserialize(rootTask.getInternalOpaque().toByteArray()));
    if (ObjectUtils.isEmpty(restoreInternalOpaque.getObjectStoreEndpoint())) {
        log.info("Restore from Replica PE so skipping this step and marking" +
                " it complete");
        currentSubtask = ergonServiceHelper.updateTask(
                currentSubtask, currentStep,
                currentSubtask.getPercentageComplete());
        log.info("STEP {}: Successfully completed restore IDF call on PC.",
                currentStep);
        // If this one is successful execute next task.
        return currentSubtask;
    }
    PCUtil.updateRestoreConfigOnPC(restoreInternalOpaque, entityDBProxy,
            instanceServiceFactory.getClusterUuidFromZeusConfig());
   currentSubtask = ergonServiceHelper.updateTask(
      currentSubtask, currentStep, RESTORE_IDF_API_PERCENTAGE);
    log.info("STEP {}: Successfully completed restore IDF call on PC.",
             currentStep);
    // If this one is successful execute next task.
    return currentSubtask;
    }
}
