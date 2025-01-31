package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.util.PCRetryHelper;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.SerializationUtils;

import java.util.Objects;

@Slf4j
@Service
public class CleanIdfOnPC implements PcdrStepsHandler {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private PCRetryHelper pcRetryHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    // Call Clean IDF on PC.
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Clean IDF stale values on PC.", currentStep);
    RestoreInternalOpaque restoreInternalOpaque =
      (RestoreInternalOpaque) Objects.requireNonNull(SerializationUtils.
              deserialize(rootTask.getInternalOpaque().toByteArray()));
    try {
      pcRetryHelper.deleteAdminUserForAuth();
      if (!ObjectUtils.isEmpty(restoreInternalOpaque.getObjectStoreEndpoint())) {
        pcRetryHelper.clearRestoreConfigInIdf();
      }
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      String errorMessage = "Unable to delete stale admin values" +
                            " on Prism Central.";
      log.error(errorMessage, e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        TaskConstants.CLEAN_IDF_PRISM_CENTRAL_STALE_VALUES);
    log.info("STEP {}: Successfully cleaned stale values of IDF " +
             " on PC.", currentStep);
    // Always return null and let it go to the next cycle
    // so that there is a wait for 60s.
    return null;
  }
}
