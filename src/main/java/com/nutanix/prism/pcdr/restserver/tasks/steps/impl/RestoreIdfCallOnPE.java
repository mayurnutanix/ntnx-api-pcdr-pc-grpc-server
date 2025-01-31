package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.adapter.exception.RetriesExhaustedException;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.clients.RecoverPcProxyClient;
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
import org.springframework.util.StringUtils;

import java.util.Objects;

import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.RESTORE_IDF_API_PERCENTAGE;

@Service
@Slf4j
public class RestoreIdfCallOnPE implements PcdrStepsHandler {

  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private RecoverPcProxyClient recoverPcProxyClient;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;

    RestoreInternalOpaque restoreInternalOpaque =
         (RestoreInternalOpaque) Objects.requireNonNull(
                 SerializationUtils.deserialize(
                         rootTask.getInternalOpaque().toByteArray()));
    if (ObjectUtils.isEmpty(restoreInternalOpaque.getPeIps())&&
        StringUtils.isEmpty(restoreInternalOpaque.getPeUuid())) {

        log.info("Restore from S3 so skipping this step and marking it complete");
        currentSubtask = ergonServiceHelper.updateTask(
                currentSubtask, currentStep,
                currentSubtask.getPercentageComplete());
        log.info("STEP {}: Successfully completed restore IDF call on PE.",
                currentStep);
        // If this one is successful execute next task.
        return currentSubtask;
    }
    // Remote call to PE to restoreIDF
    log.info("STEP {}: Creating remote call to PE for restore IDF.", currentStep);
    String clusterUuid = "";
    try {
      log.info("Attempt to extract Cluster Uuid from zeus_config");
      clusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfigWithRetry();
    } catch (RetriesExhaustedException e) {
      log.warn("Exhausted maximum number of retries to extract Cluster Uuid " +
               "from zeus_config");
      // Please note that we are returning null here so that this same step can
      // be re-attempted in next iteration of recovery after interval of 60 secs
      return null;
    }
    PCUtil.restoreIDF(Objects.requireNonNull(restoreInternalOpaque,
      "Restore Internal " +
        "Opaque was found null. " +
        "It should be present " +
        "for the task to continue" +
        "."),
      clusterUuid, recoverPcProxyClient);
    currentSubtask = ergonServiceHelper.updateTask(
      currentSubtask, currentStep, RESTORE_IDF_API_PERCENTAGE);
    log.info("STEP {}: Successfully completed restore IDF call on PE.",
             currentStep);
    // If this one is successful execute next task.
    return currentSubtask;
  }
}
