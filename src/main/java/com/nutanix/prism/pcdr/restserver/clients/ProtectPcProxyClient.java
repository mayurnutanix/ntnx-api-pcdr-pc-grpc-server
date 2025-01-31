package com.nutanix.prism.pcdr.restserver.clients;


import dp1.pri.prism.v4.protectpc.PcvmRestoreFiles;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.util.HttpHelper;
import com.nutanix.prism.pcdr.util.RequestUtil;
import dp1.pri.prism.v4.protectpc.RestoreFilesApiResponse;
import dp1.pri.prism.v4.protectpc.StopServicesApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.util.Collections;
import java.util.List;

import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;

@Slf4j
@Component
public class ProtectPcProxyClient {

    @Autowired
    @Qualifier("serviceTemplate")
    private RestTemplate restTemplate;

    @Autowired
    private HttpHelper httpHelper;

    /**
     * Method to set request headers
     */
    private HttpHeaders getHttpRequestHeaders() {
      return httpHelper.getServiceAuthHeaders(Constants.ADONIS_SERVICE,
          Constants.ADONIS_KEY_PATH, Constants.ADONIS_CERT_PATH);
    }
    /**
     * API call to stop the Service
     * @param svmIp clusterId
     * @param stopServiceList  list of services
     * @throws RestClientException - In case a request fails because of a server error response
     */
    public StopServicesApiResponse stopServicesApi(String svmIp, List<String> stopServiceList) throws RestClientException{

        if (svmIp == null) {
            throw new RuntimeException("Missing the required parameter 'svmIp' when calling stopServicesApi");
        }
        Object serviceList = stopServiceList;
        log.info("Printing body Of stopServiceAPI in ProtectPCClient {}",
                serviceList);
        // verify the required parameter 'body' is set
        if (serviceList == null) {
            throw new RuntimeException("Missing the required parameter 'serviceList' when " +
                    "calling stopServicesApi");
        }
        String apiPath =
                UriComponentsBuilder.fromPath(String.format("/api/prism/%s/protectpc/stop-services", Constants.PC_VERSION))
                    .build().toUriString();
        HttpEntity<?> httpEntity = new HttpEntity<>(serviceList,
                getHttpRequestHeaders());

        ParameterizedTypeReference<StopServicesApiResponse> responseType =
                new ParameterizedTypeReference<StopServicesApiResponse>() {
                };
        restTemplate.setRequestFactory(RequestUtil.getClientHttpRequestFactory
                (CONNECTION_TIMEOUT, READ_TIMEOUT));
        String fullUrlPath = RequestUtil.makeFullUrl(svmIp, apiPath, PC_MERCURY_PORT,
                Collections.emptyMap(), Collections.emptyMap(), false);
        log.info("Fetching stopServicesApi by invoking API {}", fullUrlPath);
        ResponseEntity<StopServicesApiResponse> stopServicesApiResponse =
                restTemplate.exchange(fullUrlPath, HttpMethod.POST, httpEntity, responseType);
        return stopServicesApiResponse.getBody();
    }

    /**
     * API call to restore files
     * @param svmIp clusterId
     * @param pcvmRestoreFiles  PcvmRestoreFiles request
     * @throws RestClientException - In case a request fails because of a server error response
     */
    public RestoreFilesApiResponse restoreFilesApi(String svmIp, PcvmRestoreFiles pcvmRestoreFiles) throws RestClientException
             {
        if (svmIp == null) {
            throw new RuntimeException(
                    "Missing the required parameter 'svmIp' when calling restoreFilesApi");
        }
        // verify the required parameter 'body' is set
        if (pcvmRestoreFiles == null) {
            throw new RuntimeException("Missing the required parameter 'pcvmRestoreFiles' when " +
                    "calling restoreFilesApi");
        }
        Object postBody = pcvmRestoreFiles;

        String apiPath =
                UriComponentsBuilder.fromPath(String.format("/api/prism/%s/protectpc/restore-files", Constants.PC_VERSION))
                    .build().toUriString();
        HttpEntity<?> httpEntity = new HttpEntity<>(postBody,
                getHttpRequestHeaders());
        ParameterizedTypeReference<RestoreFilesApiResponse> responseType =
                new ParameterizedTypeReference<RestoreFilesApiResponse>() {
                };
        restTemplate.setRequestFactory(RequestUtil.getClientHttpRequestFactory
                (CONNECTION_TIMEOUT, READ_TIMEOUT));
        String fullUrlPath = RequestUtil.makeFullUrl(svmIp, apiPath, PC_MERCURY_PORT,
                Collections.emptyMap(), Collections.emptyMap(), false);
        log.info("Fetching restoreFilesApi by invoking API {}", fullUrlPath);
        ResponseEntity<RestoreFilesApiResponse> restoreFilesApiResponse =
                restTemplate.exchange(fullUrlPath, HttpMethod.POST, httpEntity, responseType);
        return restoreFilesApiResponse.getBody();
    }

}
