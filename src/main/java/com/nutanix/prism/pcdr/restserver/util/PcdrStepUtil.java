package com.nutanix.prism.pcdr.restserver.util;

/*
 * Copyright (c) 2021 Nutanix Inc. All rights reserved.
 *
 * Author: kapil.tekwani@nutanix.com
 */

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/*
 * This class would be acting as container for PCDR steps related utility methods/functions.
 *
 */

@Slf4j
public class PcdrStepUtil {
  /**
   * Prevent initialization of utility class
   */
  private PcdrStepUtil(){}

  public static boolean hasStepExceededWaitTime(String taskName, long taskStartTime, long maxWaitTimeInMilliSeconds) {
    LocalDateTime startTime = Instant.ofEpochMilli(taskStartTime).atZone(ZoneId.systemDefault()).toLocalDateTime();
    long duration = Duration.between(startTime, LocalDateTime.now()).toMillis();
    log.info(String.format("Current time spent %s milli seconds waiting for %s step", duration, taskName));
    return duration >= maxWaitTimeInMilliSeconds;
  }
}