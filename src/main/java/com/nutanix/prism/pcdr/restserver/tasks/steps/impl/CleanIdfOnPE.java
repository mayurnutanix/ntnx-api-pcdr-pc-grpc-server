
package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.clients.RecoverPcProxyClient;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.SerializationUtils;
import org.springframework.util.StringUtils;

import java.util.Objects;

import static com.nutanix.prism.pcdr.restserver.util.PCUtil.makeCleanIDFApiCallOnReplicaPE;

@Service
@Slf4j
public class CleanIdfOnPE implements PcdrStepsHandler {

  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private RecoverPcProxyClient recoverPcProxyClient;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    // Call Clean IDF on replica PE.
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Making a remote request for clean IDF.", currentStep);
    RestoreInternalOpaque restoreInternalOpaque =
      (RestoreInternalOpaque) Objects.requireNonNull(SerializationUtils.
              deserialize(rootTask.getInternalOpaque().toByteArray()));
    if (ObjectUtils.isEmpty(restoreInternalOpaque.getPeIps()) &&
            StringUtils.isEmpty(restoreInternalOpaque.getPeUuid())) {

      log.info("Restore from S3 so skipping this step and marking it complete");
      currentSubtask = ergonServiceHelper.updateTask(
              currentSubtask, currentStep,
              TaskConstants.CLEAN_IDF_API_PERCENTAGE);
      log.info("STEP {}: Successfully completed restore IDF call on PE.",
              currentStep);
      // If this one is successful execute next task.
      return currentSubtask;
    }
    makeCleanIDFApiCallOnReplicaPE(
        Objects.requireNonNull(restoreInternalOpaque),
        instanceServiceFactory.getClusterUuidFromZeusConfigWithRetry(), recoverPcProxyClient);
    currentSubtask = ergonServiceHelper.updateTask(
      currentSubtask, currentStep,
      TaskConstants.CLEAN_IDF_API_PERCENTAGE);
    log.info("STEP {}: Successfully completed the remote request for " +
      "clean IDF.", currentStep);
    return currentSubtask;
  }
}