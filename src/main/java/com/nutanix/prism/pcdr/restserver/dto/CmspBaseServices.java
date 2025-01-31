package com.nutanix.prism.pcdr.restserver.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class CmspBaseServices {
  private boolean enable;
  @JsonProperty("enabled_services")
  private List<String> enabledServices;
}
