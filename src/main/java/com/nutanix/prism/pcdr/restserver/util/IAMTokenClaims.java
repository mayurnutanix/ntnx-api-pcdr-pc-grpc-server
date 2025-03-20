package com.nutanix.prism.pcdr.restserver.util;

import java.util.List;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class IAMTokenClaims {
  private String subject;
  private String userUUID;
  private String username;
}
