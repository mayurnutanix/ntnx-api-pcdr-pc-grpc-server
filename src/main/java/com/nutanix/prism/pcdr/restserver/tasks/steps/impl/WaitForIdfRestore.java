package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.IDFUtil;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.RESTORE_IDF_WAIT_PERCENTAGE;

@Slf4j
@Service
public class WaitForIdfRestore implements PcdrStepsHandler {

  @Autowired
  private EntityDBProxyImpl entityDBProxy;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    // Check if marker pc_restore_idf_sync_marker
    // is present, if present then success Retry with
    // exception
    log.info("STEP {}: Check if pc_restore_idf_sync_marker table exists " +
      "on PC.", currentStep);
    if (!checkPCSyncMarkerTable()) {
      return null;
    }
    // if table exists then task is successful
    currentSubtask = ergonServiceHelper.updateTask(
      currentSubtask, currentStep, RESTORE_IDF_WAIT_PERCENTAGE);
    log.info("STEP {}: Successfully completed. IDF data is restored.",
             currentStep);
    return currentSubtask;
  }

  /**
   * Checks if table pc_restore_idf_sync_marker table has non zero entities.
   */
  public boolean checkPCSyncMarkerTable() {
    try {
      // Check if table exists or not.
      List<InsightsInterfaceProto.Entity> entities;
      entities = IDFUtil.getEntitiesAsList(entityDBProxy,
        "pc_restore_idf_sync_marker");
      return !entities.isEmpty();
    }
    catch (InsightsInterfaceException iEx) {
      log.info("pc_restore_idf_sync_marker table has zero entities");
      return false;
    }
  }
}
