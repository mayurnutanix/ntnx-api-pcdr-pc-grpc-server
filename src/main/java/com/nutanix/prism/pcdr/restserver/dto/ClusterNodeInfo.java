package com.nutanix.prism.pcdr.restserver.dto;
/*
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: shivom.taank@nutanix.com
 *
 */
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ClusterNodeInfo {

    private String extId;
    private Long numCvmNodes;

}