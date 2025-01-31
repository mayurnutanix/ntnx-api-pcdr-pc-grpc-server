package com.nutanix.prism.pcdr.restserver.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

/**
 * An internal DTO to store the list of disabled services on startup during
 * recovery.
 */
@Getter
@Setter
public class CmspDisabledServices {
  @JsonProperty("disabled_services")
  private List<String> disabledServices;
}
