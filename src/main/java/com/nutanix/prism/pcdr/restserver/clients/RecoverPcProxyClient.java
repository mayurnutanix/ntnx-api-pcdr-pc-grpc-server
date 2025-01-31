package com.nutanix.prism.pcdr.restserver.clients;

/*
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: ruchil.prajapati@nutanix.com
 *
 * Client for which calls PE apis using either V4 SDK or rest template for V2 apis
 */

import com.fasterxml.jackson.core.JsonProcessingException;
import com.nutanix.dp1.pri.prism.v4.recoverpc.*;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.services.impl.ApiClientGenerator;
import com.nutanix.prism.pcdr.util.HttpHelper;
import com.nutanix.prism.pcdr.util.RequestUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.ObjectUtils;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClientException;

import java.util.Collections;

@Slf4j
@Component
public class RecoverPcProxyClient {

  public static final String PC_BACKUP_STATUS_V2_URL = "/api/prism/v2.0.a1/recoverpc/pc-backup-status";
  public static final String RESET_ARITHMOS_SYNC_V2_URL = "/api/prism/v2.0.a1/recoverpc/reset-arithmos-sync";
  public static final String PC_RESTORE_DATA_V2_URL = "/api/prism/v2.0.a1/recoverpc/pc-restore-data";
  public static final String CLEAN_IDF_AFTER_RESTORE_V2_URL = "/api/prism/v2.0" +
      ".a1/recoverpc/clean-idf-after-restore/{clusterUuid}";
  public static final String RESTORE_IDF_V2_URL = "/api/prism/v2.0" +
      ".a1/recoverpc/restore-idf";

  @Autowired
  private ApiClientGenerator apiClientGenerator;

  @Autowired
  private HttpHelper httpHelper;

  @Value("${prism.pcdr.api.readTimeout.milliseconds:30000}")
  private int readTimeoutMs;

  /**
   * Method to set request headers
   */
  private HttpHeaders getHttpRequestHeaders() {
    return httpHelper.getServiceAuthHeaders(Constants.PRISM_SERVICE,
        Constants.PRISM_KEY_PATH, Constants.PRISM_CERT_PATH);
  }

  /**
   * Fetch last sync time from the PE by first calling V4 api using
   * PE sdk. If it fails with 404 then calling V2 api using rest template
   * @param ip PE IP
   * @return PcBackupStatusApiResponse
   * @throws RestClientException, JsonProcessingException - In case a request fails because of a server error response
   */
  public PcBackupStatusOnPe pcBackupStatusAPICallOnPE(final String ip)
      throws RestClientException, JsonProcessingException, PCResilienceException {

    try {
      return (PcBackupStatusOnPe) apiClientGenerator.getRecoverPcApiInstance(ip, readTimeoutMs)
          .getPCBackupStatus().getData();
    } catch (HttpClientErrorException e) {
      if (!ObjectUtils.isEmpty(e.getMessage()) && e.getStatusCode()==HttpStatus.NOT_FOUND){
        log.info("PE does not have V4 API, trying V2 API");
        HttpEntity<?> httpEntity = new HttpEntity<>(Collections.emptyMap(),
            getHttpRequestHeaders());
        String fullUrlPath = RequestUtil.makeFullUrl(ip, PC_BACKUP_STATUS_V2_URL,
            Constants.RECOVER_PORT, Collections.emptyMap(), Collections.emptyMap(), true);
        return httpHelper.invokeV2API(fullUrlPath, httpEntity, HttpStatus.OK,
            HttpMethod.GET, PcBackupStatusOnPe.class);
      } else {
        throw e;
      }
    }

  }

  /**
   * Invokes reset-arithmos-sync on PE. First tries using V4 PE SDK.
   * If it returns 404 then try V2 api using rest template
   * @param ip PE IP
   * @throws RestClientException
   */
  public ApiSuccess resetArithmosSyncAPICallOnPE(final String ip)
      throws RestClientException {

    try {
      return (ApiSuccess) apiClientGenerator.getRecoverPcApiInstance(ip, readTimeoutMs)
          .resetArithmosSync().getData();
    } catch (HttpClientErrorException e) {
      if (!ObjectUtils.isEmpty(e.getMessage()) && e.getStatusCode()==HttpStatus.NOT_FOUND){
        log.info("PE does not have V4 API, trying V2 API");
        HttpEntity<?> httpEntity = new HttpEntity<>(Collections.emptyMap(),
            getHttpRequestHeaders());
        String fullUrlPath = RequestUtil.makeFullUrl(ip, RESET_ARITHMOS_SYNC_V2_URL,
            Constants.RECOVER_PORT, Collections.emptyMap(), Collections.emptyMap(), true);
        try {
          return httpHelper.invokeV2API(fullUrlPath, httpEntity, HttpStatus.OK,
              HttpMethod.POST, ApiSuccess.class);
        } catch (JsonProcessingException ex) {
          throw new RuntimeException("Error parsing model from V2 to V4 version ", ex);
        }

      } else {
        throw e;
      }
    } catch (PCResilienceException e) {
      log.error("Unable to get RecoverPcApi Instance for invoking PE apis", e);
      throw new RuntimeException("Unable to get RecoverPcApi Instance for invoking PE apis", e);
    }

  }

  /**
   * Invokes pc-restore-data on PE. First tries using V4 PE SDK.
   * If it returns 404 then try V2 api using rest template
   * @param ip PE IP
   * @param pcRestoreDataQuery post body object for pc-restore-data api call
   * @throws RestClientException - In case a request fails because of a server error response
   */
  public PCRestoreData pcRestoreDataAPICallOnPE(final String ip,
                                                           final PCRestoreDataQuery pcRestoreDataQuery)
      throws RestClientException, JsonProcessingException, PCResilienceException {

    try {
      return (PCRestoreData) apiClientGenerator.getRecoverPcApiInstance(ip, readTimeoutMs)
          .getRestoreData(pcRestoreDataQuery).getData();
    } catch (HttpClientErrorException e) {
      if (!ObjectUtils.isEmpty(e.getMessage()) && e.getStatusCode()==HttpStatus.NOT_FOUND){
        log.info("PE does not have V4 API, trying V2 API");
        HttpEntity<?> httpEntity = new HttpEntity<>(pcRestoreDataQuery,
            getHttpRequestHeaders());
        String fullUrlPath = RequestUtil.makeFullUrl(ip, PC_RESTORE_DATA_V2_URL,
            Constants.RECOVER_PORT, Collections.emptyMap(), Collections.emptyMap(), true);
        return httpHelper.invokeV2API(fullUrlPath, httpEntity, HttpStatus.OK,
            HttpMethod.POST, PCRestoreData.class);
      } else {
        throw e;
      }
    }

  }

  /**
   * Invokes clean-idf-after-restore on PE. First tries using V4 PE SDK.
   * If it returns 404 then try V2 api using rest template
   * @param ip PE IP
   * @throws RestClientException
   */
  public ApiSuccess cleanIDFAfterRestoreAPICallOnPE(final String ip, String clusterUuid)
      throws RestClientException, PCResilienceException, JsonProcessingException {

    try {
      return (ApiSuccess) apiClientGenerator.getRecoverPcApiInstance(ip, readTimeoutMs)
          .cleanIdfAfterRestore(clusterUuid).getData();
    } catch (HttpClientErrorException e) {
      if (!ObjectUtils.isEmpty(e.getMessage()) && e.getStatusCode()==HttpStatus.NOT_FOUND){
        log.info("PE does not have V4 API, trying V2 API");
        String fullUrlPath = RequestUtil.makeFullUrl(ip, CLEAN_IDF_AFTER_RESTORE_V2_URL,
            Constants.RECOVER_PORT, Collections.emptyMap(), Collections.singletonMap("clusterUuid", clusterUuid), true);
        HttpEntity<?> httpEntity = new HttpEntity<>(Collections.emptyMap(),
            getHttpRequestHeaders());
        return httpHelper.invokeV2API(fullUrlPath, httpEntity, HttpStatus.OK,
            HttpMethod.DELETE, ApiSuccess.class);
      } else {
        throw e;
      }
    }

  }

  /**
   * Invokes restore-idf on PE. First tries using V4 PE SDK.
   * If it returns 404 then try V2 api using rest template
   * @param ip PE IP
   * @throws RestClientException - In case a request fails because of a server error response
   */
  public ApiSuccess restoreIDFAPICallOnPE(final String ip,
                                                     RestoreIDF restoreIDF)
      throws RestClientException, JsonProcessingException, PCResilienceException {

    try {
      return (ApiSuccess) apiClientGenerator.getRecoverPcApiInstance(ip, readTimeoutMs)
          .restoreIDF(restoreIDF).getData();
    } catch (HttpClientErrorException e) {
      if (!ObjectUtils.isEmpty(e.getMessage()) && e.getStatusCode()==HttpStatus.NOT_FOUND){
        log.info("PE does not have V4 API, trying V2 API");
        String fullUrlPath = RequestUtil.makeFullUrl(ip, RESTORE_IDF_V2_URL,
            Constants.RECOVER_PORT, Collections.emptyMap(), Collections.emptyMap(), true);
        HttpEntity<?> httpEntity = new HttpEntity<>(restoreIDF,
            getHttpRequestHeaders());
        return httpHelper.invokeV2API(fullUrlPath, httpEntity, HttpStatus.OK,
            HttpMethod.POST, ApiSuccess.class);
      } else {
        throw e;
      }
    }

  }

}
