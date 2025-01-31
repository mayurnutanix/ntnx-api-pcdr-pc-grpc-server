package com.nutanix.prism.pcdr.restserver.services.api;

/*
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: shivom.taank@nutanix.com
 *
 */
public interface ClusterEligibilityUpdateHelper {

    void updateClustersEligibility();

    boolean isTimeToUpdateClusterEligibility();
}
