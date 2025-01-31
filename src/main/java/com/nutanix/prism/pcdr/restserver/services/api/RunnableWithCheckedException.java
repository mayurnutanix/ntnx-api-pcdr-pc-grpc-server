package com.nutanix.prism.pcdr.restserver.services.api;
/*
 * Copyright (c) 2023 Nutanix Inc. All rights reserved.
 *
 * Author: mayur.ramavat@nutanix.com
 */

import com.nutanix.prism.pcdr.exceptions.PCResilienceException;

/**
 * Functional interface for bucket validation runnable with checked Exception
 */
@FunctionalInterface
public interface RunnableWithCheckedException {

  void run() throws PCResilienceException;
}
