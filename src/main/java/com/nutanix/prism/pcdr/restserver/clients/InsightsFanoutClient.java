package com.nutanix.prism.pcdr.restserver.clients;

import com.google.protobuf.Message;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.net.RpcProto;
import com.nutanix.prism.pcdr.restserver.exceptions.ErrorCode;
import com.nutanix.prism.pcdr.restserver.exceptions.InsightsServiceException;
import com.nutanix.prism.pcdr.restserver.exceptions.InsightsServiceRetryException;
import com.nutanix.prism.pcdr.restserver.exceptions.InsightsServiceRpcException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 *  InsightsFanoutClient class will be used to make mercury fanout calls to
 *  invoke remote RPC's on services running on PE.
 */
@Slf4j
@Component
public class InsightsFanoutClient extends MercuryFanoutRpcClient {

  protected static final int DEFAULT_NUM_RETRIES = 2;
  protected static final int DEFAULT_RETRY_DELAY_MILLISECONDS = 500;
  protected static final int INSIGHTS_SVC_RPC_TIMEOUT_MILLI_SECS = 10000;
  private static final String INSIGHTS_SERVICE_NAME = "InsightsRpcSvc";

  private static final String FULL_INSIGHTS_SERVICE_NAME =
      "nutanix.insights.interface.InsightsRpcSvc";


  @Autowired
  public InsightsFanoutClient(@Qualifier("PcdrRestTemplate") RestTemplate restTemplate,
                              @Value("${insights.proxy.port:2027}") int insightsProxyPort) {
    super(restTemplate);
    this.setRemoteRequestUrlPathPrefix(FULL_INSIGHTS_SERVICE_NAME, insightsProxyPort);
  }

  /**
   * This method is used to generate remote Request Url which will later be
   * inject into mercury fanout request.
   *
   * @param serviceName - Remote Service name
   * @param servicePort - Remote Service port number
   */
  public void setRemoteRequestUrlPathPrefix(final String serviceName,
                                     final int servicePort) {
    this.remoteRequestUrlPathPrefix = REMOTE_REQUEST_URL_PATH + "?" +
                                      "service_name=" + serviceName +
                                      "&port=" + servicePort +
                                      "&timeout_ms=" + INSIGHTS_SVC_RPC_TIMEOUT_MILLI_SECS +
                                      "&base_url=" + "/rpc";
  }

  /**
   * This method is used to invoke the RPC call with given set of
   * function arguments
   *
   * @param rpcName       -  Remote Procedure call function name
   * @param rpcArg        -  Remote Procedure call Arguments
   * @param responseType  -  Remote Procedure call Response Type
   * @param clusterId     -  Cluster Uuid of PE
   *
   * @return              -  Remote Procedure call Response Object
   *
   * @throws InsightsServiceException
   */
  public <R extends Message> R invokeFanoutCall(final RpcName rpcName,
                                                final Message rpcArg,
                                                R.Builder responseType,
                                                final String clusterId)
      throws InsightsServiceException {

    int retry = 0;
    String errorMessage = "";
    while (retry < DEFAULT_NUM_RETRIES) {
      try {
        rpcStatus = RpcProto.RpcResponseHeader.RpcStatus.kNoError;
        rpcErrorDetail = "";

        byte[] requestPayloadByteArray = createRequestPayload(rpcName, rpcArg);
        URI fanoutUrl = getFanoutUrl(clusterId, HttpMethod.POST);

        HttpEntity<byte[]> requestEntity = obtainRequestEntityWithHeaders(requestPayloadByteArray);
        ResponseEntity<byte[]> responseEntity = invokeFanoutCall(fanoutUrl,
                                                                 HttpMethod.POST,
                                                                 requestEntity);

        return populateResponse(responseEntity.getBody(), responseType);
      } catch (final InsightsServiceRetryException re) {
        errorMessage = re.getMessage();
        retry++;
        try {
          MILLISECONDS.sleep(DEFAULT_RETRY_DELAY_MILLISECONDS);
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
        }
      }
    }
    throw new InsightsServiceRpcException(
        String.format("Request failed when executing remote operation %s: "
                      + "%s", rpcName, errorMessage),
        ErrorCode.INSIGHTS_SERVICE_RPC);
  }

  @Override
  byte[] createRequestPayload(final RpcName rpcName, final Message rpcArg) throws InsightsServiceRpcException  {

    return createRequestPayload(INSIGHTS_SERVICE_NAME, rpcName.toString(), rpcArg,
                                INSIGHTS_SVC_RPC_TIMEOUT_MILLI_SECS);
  }

  @Override
  void handleAppError(final RpcProto.RpcResponseHeader.Builder rpcResponseHeader)
      throws InsightsServiceRetryException, InsightsServiceRpcException {

    InsightsInterfaceProto.InsightsErrorProto.Type insightsErrorType =
        InsightsInterfaceProto.InsightsErrorProto.Type.forNumber(rpcResponseHeader.getAppError());

    log.error("kAppError:{} recorded in response RPC as status",
           insightsErrorType);
    switch (insightsErrorType) {
      case kRetry:
        throw new InsightsServiceRetryException(rpcErrorDetail);
      case kNotFound:
        throw new InsightsServiceRpcException(
            rpcErrorDetail, ErrorCode.INSIGHTS_SERVICE_UNKNOWN_ENTITY,
            insightsErrorType.name()
        );
      default:
        final String errorString = String.format(
            "Application error %s raised: %s", insightsErrorType.name(),
            rpcErrorDetail);
        log.info(errorString);
        throw new InsightsServiceRpcException(
            errorString, insightsErrorType.name()
        );
    }
  }
}
