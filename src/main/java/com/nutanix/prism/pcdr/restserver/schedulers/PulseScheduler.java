package com.nutanix.prism.pcdr.restserver.schedulers;

import com.nutanix.prism.pcdr.restserver.services.api.PulsePublisherService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *  Service to create a scheduler for pulse data update for PCDR workflow
 */
@Slf4j
@Service
public class PulseScheduler {

  @Autowired
  private PulsePublisherService pulsePublisherService;

  @Autowired
  @Qualifier("adonisServiceScheduledThreadPool")
  private ScheduledExecutorService adonisServiceScheduledThreadPool;

   @Value("${prism.pcdr.pulse.start.delay}")
  private long initialDelay;

  @Value("${prism.pcdr.pulse.schedule.frequency}")
  private long fixedDelay;

  @PostConstruct
  public void init() {
       adonisServiceScheduledThreadPool.scheduleWithFixedDelay(
            this::scheduledPulseUpdate, initialDelay, fixedDelay, TimeUnit.MILLISECONDS );
  }

  /**
   *  Function to send regular pulse data to the pulse db
   */
  void scheduledPulseUpdate(){
    pulsePublisherService.sendMetricDataToPulse();
  }

}
