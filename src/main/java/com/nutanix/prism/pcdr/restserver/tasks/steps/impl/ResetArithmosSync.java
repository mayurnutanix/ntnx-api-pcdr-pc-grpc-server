package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.schedulers.ClusterDataStateCleanupScheduler;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.services.api.RestorePEService;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

import static com.nutanix.prism.pcdr.restserver.constants.TaskConstants.RESET_ARITHMOS_SYNC_PERCENTAGE;

@Slf4j
@Service
public class ResetArithmosSync implements PcdrStepsHandler {

  @Autowired
  private RestorePEService restorePEService;
  @Autowired
  private PCVMDataService pcvmDataService;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private ClusterDataStateCleanupScheduler clusterDataStateCleanupScheduler;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    log.info("STEP {}: Reset arithmos sync marker on the PEs registered.", currentStep);
    List<String> peListToRestore = pcvmDataService.getPeUuidsOnPCClusterExternalState();
    log.info("PE list found from zk cluster external state for resetting" +
      " arithmos sync" + peListToRestore);

    restorePEService.setPeUuidsInPCDRZkNode(peListToRestore);
    clusterDataStateCleanupScheduler.scheduleClusterDataStateCleanup();

    currentSubtask = ergonServiceHelper.updateTask(
      currentSubtask, currentStep, RESET_ARITHMOS_SYNC_PERCENTAGE);
    log.info("STEP {}: Successfully initiated reset arithmos sync on registered PEs.",
      currentStep);
    return currentSubtask;
  }
}