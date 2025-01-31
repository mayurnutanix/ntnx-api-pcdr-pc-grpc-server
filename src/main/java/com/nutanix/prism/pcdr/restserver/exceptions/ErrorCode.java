package com.nutanix.prism.pcdr.restserver.exceptions;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public enum ErrorCode {

  RUNTIME_GENERIC_RPC(1202, "Service is down."),
  INSIGHTS_SERVICE_RETRY(1301, "Failed while retrying to reach" +
                             " Insights service."),
  INSIGHTS_SERVICE_RPC(1302, "Insights service error."),
  INSIGHTS_SERVICE_UNKNOWN_ENTITY(1303, "Insights service can't find entity");


  @Getter
  private final int code;
  @Getter
  private final String description;

  @Override
  public String toString() {
    return this.getCode() + " : " + this.getDescription();
  }
}