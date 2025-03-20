package com.nutanix.prism.pcdr.restserver.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.interceptors.RequestContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GrpcAuthUtils {

  @Autowired
  private ObjectMapper backupObjectMapper;

  public IAMTokenClaims getIamTokenClaims() throws PCResilienceException {
    String tokenClaims = (String) RequestContext.IAM_TOKEN_CLAIMS_VALUE.get();
    log.info("Parsing token claims {}", tokenClaims);
    if (StringUtils.isNotEmpty(tokenClaims)) {
      try {
        IAMTokenClaims iamTokenClaims =
                backupObjectMapper.readValue(tokenClaims, IAMTokenClaims.class);
        return iamTokenClaims;
      } catch (JsonProcessingException e) {
        log.error("Unable to parse IAM token claims", e);
      }
    }
    log.info("Parsed token claims");
    throw new PCResilienceException("IAM Token Parsing Error");
  }

  public String getUserUuid() throws PCResilienceException {
    IAMTokenClaims iamTokenClaims = getIamTokenClaims();
    String userUuid = iamTokenClaims.getUserUUID();
    if (StringUtils.isEmpty(userUuid)) {
      throw new PCResilienceException("IAM Token Parsing Error");
    }
    log.debug("Parsed user uuid {}", userUuid);
    return userUuid;
  }

  public String getUserName() throws PCResilienceException {
    IAMTokenClaims iamTokenClaims = getIamTokenClaims();
    String userName = iamTokenClaims.getUsername();
    if (StringUtils.isEmpty(userName)) {
      throw new PCResilienceException("IAM Token Parsing Error");
    }
    log.debug("Parsed user uuid {}", userName);
    return userName;
  }
}

