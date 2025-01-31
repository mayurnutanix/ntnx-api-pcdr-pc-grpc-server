package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class RestorePcDataFromIdf implements PcdrStepsHandler {

  @Autowired
  private RestoreDataService restoreDataService;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    // STEP: Restore the pc data like zookeeper nodes and PCVM files
    // in IDF.
    log.info("STEP {}: Restoring pcvm files and zk nodes from IDF.",
             currentStep);
    restoreDataService.restorePCDataFromIDF();
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        TaskConstants.RESTORE_ZK_NODES_AND_FILES_PERCENTAGE);
    log.info("STEP {}: Successfully restored zk nodes and pcvm files.",
             currentStep);
    return currentSubtask;
  }
}