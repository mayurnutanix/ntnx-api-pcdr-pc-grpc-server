package com.nutanix.prism.pcdr.restserver.services.impl;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.concurrent.TimeUnit;


@Slf4j
public class StopServicesRunnable implements Runnable {

  private final Object clusterStopServicesLock;
  private final List<String> serviceNameList;
  private final PCTaskServiceHelper pcTaskServiceHelper;
  private final long servicesStopWaitTime;


  public StopServicesRunnable(final Object lock,
                              final List<String> serviceNameList,
                              final PCTaskServiceHelper pcTaskServiceHelper,
                              final long servicesStopWaitTime) {
    this.clusterStopServicesLock = lock;
    this.serviceNameList = serviceNameList;
    this.pcTaskServiceHelper = pcTaskServiceHelper;
    this.servicesStopWaitTime = servicesStopWaitTime;
  }

  @Override
  public void run() {
    synchronized(clusterStopServicesLock) {
      try {
        if (serviceNameList != null && !serviceNameList.isEmpty()) {
          Process processGenesisStop = pcTaskServiceHelper
              .stopServicesUsingGenesis(serviceNameList);
          boolean genesisStopCompleted = processGenesisStop
              .waitFor(servicesStopWaitTime, TimeUnit.SECONDS);
          log.info("waitFor on process executing 'genesis stop' command " +
                   "returned status : {}", genesisStopCompleted);
          if (!genesisStopCompleted) {
            log.warn("Process executing 'genesis stop' command did not finish " +
                     "within specified time limit of {} seconds", servicesStopWaitTime);
          } else {
            log.info("Exit value of process executing 'genesis stop' command: {}",
                     processGenesisStop.exitValue());
          }
        }
      }
      catch (Exception e) {
        log.error("The following exception encountered while" +
                  " stopping cluster services.", e);
      }
    }
  }
}