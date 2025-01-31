package com.nutanix.prism.pcdr.restserver.services.impl;

import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.util.AuthenticationUtil;
import com.nutanix.prism.pcdr.util.RequestUtil;
import com.nutanix.prism.util.common.CaCertificateChainUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

@Slf4j
@Service
public class ApiClientGenerator {

  @Autowired
  private CaCertificateChainUtil caCertificateChainUtil;

  /**
   * Helper method to get Recover Api object. This will be handy while writing
   * unit tests as we can directly mock this method and get Recover Api object.
   * @param ip - Ip address of the PE for which api client object is needed
   * @return Recover Api object for the passed Ip address.
   */
  public com.nutanix.pri.java.client.api.RecoverPcApi getRecoverPcApiInstance(
      String ip, int apiReadTimeout) throws PCResilienceException {
    com.nutanix.pri.java.client.ApiClient apiClient =
        createApiClientPE(ip, Constants.JGW_PORT, apiReadTimeout);
    return new com.nutanix.pri.java.client.api.RecoverPcApi(apiClient);
  }

  public com.nutanix.pri.java.client.ApiClient createApiClientPE(
      String ip, int port, int readTimeout) throws PCResilienceException {
    RestTemplate restTemplate = new RestTemplate(RequestUtil
      .getClientHttpRequestFactory(1000, readTimeout));
    com.nutanix.pri.java.client.ApiClient apiClient =
      new com.nutanix.pri.java.client.ApiClient(restTemplate);

    apiClient.setHost(ip);
    apiClient.setPort(port);
    //x5c header containing all public certificates.
    try {
      apiClient.addDefaultHeader("x5c",
                                 String.join(",", caCertificateChainUtil
                                     .createFilteredCertChainList(Constants.ADONIS_CERT_PATH)));
    } catch (final Exception ex) {
      log.error("Error filtering CA Certificate chain bytes due to exception: ", ex);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    //X-NTNX-SERVICE-TOKEN header containing jwtToken.
    apiClient.addDefaultHeader("X-NTNX-SERVICE-TOKEN",
      AuthenticationUtil.createJwtTokenUsingCerts(Constants.ADONIS_KEY_PATH,
        Constants.ADONIS_SERVICE));

    return apiClient;
  }

  public com.nutanix.lcmroot.java.client.ApiClient createLcmApiClient() throws PCResilienceException {

    com.nutanix.lcmroot.java.client.ApiClient apiClient =
            new com.nutanix.lcmroot.java.client.ApiClient();

    //x5c header containing all public certificates.
    try {
      apiClient.setVerifySsl(false);
      apiClient.addDefaultHeader("x5c",
              String.join(",", caCertificateChainUtil
                      .createFilteredCertChainList(Constants.ADONIS_CERT_PATH)));
    } catch (KeyStoreException | NoSuchAlgorithmException exception){
      log.error("Failed to set the SSL connection while creating LCM api client");
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    } catch (final Exception ex) {
      log.error("Error filtering CA Certificate chain bytes due to exception: ", ex);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    //X-NTNX-SERVICE-TOKEN header containing jwtToken.
    apiClient.addDefaultHeader("X-NTNX-SERVICE-TOKEN",
            AuthenticationUtil.createJwtTokenUsingCerts(Constants.ADONIS_KEY_PATH,
                    Constants.ADONIS_SERVICE));

    return apiClient;

  }

  public com.nutanix.lcmroot.java.client.api.EntitiesApi getLcmEntitiesApiInstance() throws PCResilienceException {

    com.nutanix.lcmroot.java.client.ApiClient apiClient = createLcmApiClient();
    return new com.nutanix.lcmroot.java.client.api.EntitiesApi(apiClient);
  }
}
