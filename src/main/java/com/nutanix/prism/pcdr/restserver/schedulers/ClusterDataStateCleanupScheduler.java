package com.nutanix.prism.pcdr.restserver.schedulers;

import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.services.api.RestorePEService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.*;

/**
 * This class schedules call to cleanIdfAfterRestore on all the PEs after
 * restore is complete, to enable matrix data sync from PE to PC for past 90
 * days.
 *
 */
@Slf4j
@Service
public class ClusterDataStateCleanupScheduler {
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> scheduledTask;
  @Autowired
  private RestorePEService restorePEService;
  @Value("${prism.pcdr.peClusterDataCleanup.scheduler.frequency.hours:24}")
  private int schedulerFrequencyHours;

  @PostConstruct
  public void initializeScheduler() {
    ExecutorService createCDSCleanupExecutorService =
        Executors.newSingleThreadExecutor();
    // Adding it as part of a separate thread so that it does not block bean
    // creation of ClusterDataStateCleanupScheduler when Zk is in connecting
    // state. During functional tests the bean creation was stuck because of
    // this (BasicConnectionManagingZKClient waits till zk is connected).
    // There is an issue with VPN where when on mac it tries to connect to
    // any hostname and port given for zk, and zk shows connecting.
    createCDSCleanupExecutorService.execute(() -> {
      try {
        createClusterDataStateScheduler();
      } catch (Exception e) {
        log.error("Error encountered while starting cluster data state " +
                  "cleanup scheduler.", e);
      }
    });
    createCDSCleanupExecutorService.shutdown();
  }

  /**
   * Creates cluster data state scheduler.
   */
  public void createClusterDataStateScheduler() {
    // Scheduling the Cleanup task.
    List<String> peCleanupList = restorePEService.getPeUuidsInPCDRZkNode();
    if (!peCleanupList.isEmpty()) {
      log.info(
          "Found the following PEs for which arithmos sync has to be reset: "
          + peCleanupList);
      scheduleClusterDataStateCleanup();
    }
    else {
      log.info("No such PE found for which arithmos sync has to be reset.");
    }
  }

  public void scheduleClusterDataStateCleanup(){
    log.info("Starting reset arithmos sync scheduler for PC DR.");
    scheduler = Executors.newSingleThreadScheduledExecutor();
    scheduledTask = scheduler.scheduleAtFixedRate(
      this::executePeCleanup, 0, schedulerFrequencyHours, TimeUnit.HOURS);
  }

  public void cancelScheduler(){
    log.info("Stopping reset arithmos sync scheduler for PC DR.");
    scheduledTask.cancel(true);
    scheduler.shutdown();
  }

  public void executePeCleanup(){
    log.info("Executing PE cleanup.");
    try {
      restorePEService.cleanIdfOnRegisteredPEs();
    } catch (PCResilienceException e) {
      log.error("Exception while running the scheduled PE cleanup", e);
    }
    List<String> peCleanupList = restorePEService.getPeUuidsInPCDRZkNode();
    if(peCleanupList.isEmpty()) {
      log.info("Stopping reset arithmos sync scheduler as" +
        " PE Cleanup list is empty.");
      cancelScheduler();
    }
  }
}