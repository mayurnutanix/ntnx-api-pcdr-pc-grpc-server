package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
import com.nutanix.prism.pcdr.restserver.services.impl.PCTaskServiceHelper;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RestartServicesOnPC implements PcdrStepsHandler {

  @Autowired
  PCTaskServiceHelper pcTaskServiceHelper;
  @Autowired
  ErgonServiceHelper ergonServiceHelper;
  @Autowired
  RestoreDataService restoreDataService;

  @Value("${prism.pcdr.services.stop.wait.time:600}")
  private long servicesStopWaitTime;
  @Value("${prism.pcdr.services.start.wait.time:1200}")
  private long servicesStartWaitTime;

  @Setter
  private int percentageCompleted;
  @Setter
  private List<String> servicesToStop;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int currentStepNum = (int) (currentSubtask.getStepsCompleted() + 1);
    // STEP: Restart services on Prism central
    log.info("STEP {}: Starting step of restarting services on Prism Central.",
             currentStepNum);
    try {
      if (servicesToStop != null && !servicesToStop.isEmpty()) {
        restoreDataService.stopServicesUsingGenesis(servicesToStop);
      }
      Thread.sleep(servicesStopWaitTime * 1000L);
      currentSubtask = startClusterOnPC(currentSubtask);
    } catch (Exception e) {
      log.error("Exception encountered while restarting services.", e);
      throw new PCResilienceException(ErrorMessages.RESTART_SERVICES_ERROR,
              ErrorCode.PCBR_RESTART_SERVICES_FAILURE,HttpStatus.INTERNAL_SERVER_ERROR);
    }
    log.info("STEP {}: Successfully restarted services on Prism Central.",
             currentStepNum);
    return currentSubtask;
  }

  public ErgonTypes.Task startClusterOnPC(ErgonTypes.Task currentSubtask)
      throws PCResilienceException, ErgonException {
    int currentStepNum = (int) (currentSubtask.getStepsCompleted() + 1);
    log.info("Attempting to execute 'cluster start' command on Prism Central");
    long startTimeInMillis = System.currentTimeMillis();
    try {
      Process genesisClusterStartProcess =
              pcTaskServiceHelper.runBashClusterStart();
      boolean clusterStartCompleted =
              genesisClusterStartProcess.waitFor(servicesStartWaitTime,
                      TimeUnit.SECONDS);
      log.info("waitFor on process executing 'cluster start' command returned " +
              "status : {}", clusterStartCompleted);
      if (!clusterStartCompleted) {
        log.warn("Process executing 'cluster start' command did not finish within " +
                "the specified time limit of {} seconds", servicesStartWaitTime);
      } else {
        log.info("Exit value of process executing 'cluster start' command: {}",
                genesisClusterStartProcess.exitValue());
      }
    }
    catch (Exception e) {
      log.error("The following exception encountered while" +
              " starting cluster services.", e);
      throw new PCResilienceException("Unable to start cluster services on prism central within" +
              " set time interval of " + servicesStartWaitTime + " seconds",
              ErrorCode.PCBR_TIMED_RESTART_SERVICES_FAILURE,HttpStatus.INTERNAL_SERVER_ERROR);
    } finally {
      float timeTakenInSec = (float) (System.currentTimeMillis() - startTimeInMillis) /1000;
      log.info("Time taken to execute 'cluster start' command on PC : {}s", timeTakenInSec);
    }
    return ergonServiceHelper.updateTask(
            currentSubtask, currentStepNum, percentageCompleted);
  }
}