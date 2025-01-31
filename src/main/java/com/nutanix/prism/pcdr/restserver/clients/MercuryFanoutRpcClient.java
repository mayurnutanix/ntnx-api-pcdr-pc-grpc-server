package com.nutanix.prism.pcdr.restserver.clients;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.nutanix.net.RpcProto;
import com.nutanix.prism.pcdr.restserver.exceptions.InsightsServiceRetryException;
import com.nutanix.prism.pcdr.restserver.exceptions.InsightsServiceRpcException;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public abstract class MercuryFanoutRpcClient {

  private static final String FANOUT_URL_PREFIX =
      "http://localhost:9444/v3/fanout_proxy";

  static final String REMOTE_REQUEST_URL_PATH = "remote_rpc_request";

  // The number of bytes the header size tag is corresponding to v1 RPC.
  private static final int TAG_SIZE_BYTES_V1 = 4;

  // The unique client id associated with this RpcClient.
  private final long clientId;

  // Monotonically increasing request id counter. Used when generating a
  // RPC's rpcId.
  private final AtomicInteger nextRequestId = new AtomicInteger(0);

  // Client id generator.
  private static final Random generator = new SecureRandom();
  /*
     The URL which needs to be invoked for making the remote request.
     Example -
     remote_rpc_request?base_url=/rpc&set_content_length=True
     &timeout_ms=60000&service_name=nutanix.insights.uploader.InsightsReceiverRpcSvc
     &port=2028
     */
  String remoteRequestUrlPathPrefix;

  // A REST client to invoke HTTP requests.
  private final RestTemplate restTemplate;

  // The status of the RPC.
  RpcProto.RpcResponseHeader.RpcStatus rpcStatus;

  // The detail corresponding to the RPC error.
  String rpcErrorDetail;

  MercuryFanoutRpcClient(final RestTemplate restTemplate) {
    synchronized (generator) {
      // clientID should be within 32 bit limit
      this.clientId = generator.nextLong() & 0x7FFFFFFF;
    }
    this.restTemplate = restTemplate;
  }

  @AllArgsConstructor
  public enum RpcName {
    DELETE_CLUSTER_REPLICATION_STATE("DeleteClusterReplicationState");

    @Getter
    private final String name;

    @Override
    public String toString() {
      return this.getName();
    }
  }

  /**
   * Returns the percent encoded fanout URL which is of the form:-
   * http://127.0.0.1:9444/v3/fanout_proxy?remote_cluster_uuid=
   * 0005b083-be6e-5449-0000-000000005949&method=POST&url_path=
   * remote_rpc_request%3Fservice_name%3Dnutanix.acropolis.AcropolisRpcSvc
   * %26port%3D2030%26base_url%3D%2Frpc%26timeout_ms%3D3000
   * &set_content_length=True&content_type=application/octet-stream
   *
   * @param clusterUuid    the UUID of the remote cluster.
   * @param httpMethodType the HTTP method type to perform the REST operation.
   * @return the percent encoded fanout URL path prefix.
   */
  URI getFanoutUrl(final String clusterUuid,
                   final HttpMethod httpMethodType) {

    String fanoutUrlString = FANOUT_URL_PREFIX + "?" +
                             "remote_cluster_uuid=" + clusterUuid +
                             "&method=" + httpMethodType.name() +
                             "&url_path=" + URLEncoder.encode(remoteRequestUrlPathPrefix) +
                             "&set_content_length=True" +
                             "&content_type=application/octet-stream";
    return UriComponentsBuilder.fromUriString(fanoutUrlString).build(
        true /* encoded */).toUri();
  }

  /**
   * This method is used to generate Http Request entity object in byte array
   * format accepting request payload in byte array format.
   *
   * @param requestPayloadByteArray - Request payload in byte array format.
   *
   * @return HttpEntity<byte[]>
   */
  HttpEntity<byte[]> obtainRequestEntityWithHeaders(final byte[] requestPayloadByteArray) {
    HttpHeaders httpHeaders = new HttpHeaders();
    httpHeaders.setAccept(Arrays.asList(MediaType.APPLICATION_OCTET_STREAM,
                                        MediaType.APPLICATION_JSON));
    return new HttpEntity<>(requestPayloadByteArray, httpHeaders);
  }

  /**
   * Method to serialize protobuf for rpc request, add headers and
   * make data ready for request.
   * RPC requests are encoded on the wire as follows:
   * (1) First four bytes indicating the size of the RPC header in network byte order.
   * (2) RPC header for the request.
   * (3) RPC request protobuf.
   * The RPC header itself contains the size of (3).
   *
   * @param serviceName  The name of the rpc service.
   * @param methodName   The rpc method that needs to be invoked.
   * @param message      The input protobuf object for method.
   * @param timeoutMsecs The time period in which the request must time out.
   * @return a byte array - serialized data ready for RPC.
   */
  byte[] createRequestPayload(final String serviceName,
                              final String methodName,
                              final Message message,
                              final int timeoutMsecs) throws InsightsServiceRpcException {

    // Serialize the request message.
    byte[] messageByteArray = message.toByteArray();

    RpcProto.RpcRequestContext.Builder requestContext =
        RpcProto.RpcRequestContext.newBuilder();

    // Create the request header and set necessary arguments.
    RpcProto.RpcRequestHeader.Builder requestHeader =
        RpcProto.RpcRequestHeader.newBuilder();
    final int requestId = nextRequestId.getAndAdd(1);
    final long rpcId = (clientId << 32) | requestId;
    requestHeader.setRpcId(rpcId);
    requestHeader.setMethodName(methodName);
    requestHeader.setRequestContext(requestContext);
    requestHeader.setSendTimeUsecs(System.currentTimeMillis() * 1000);
    requestHeader.setDeadlineUsecs(requestHeader.getSendTimeUsecs() + timeoutMsecs * 1000L);
    requestHeader.setProtobufSize(message.getSerializedSize());
    requestHeader.setServiceName(serviceName);

    log.debug("Sending RPC Request with header : {}", requestHeader);

    byte[] headerByteArray = requestHeader.build().toByteArray();
    // Compute the request header size.
    final int headerSize = requestHeader.build().getSerializedSize();

    // The first four bytes in the request must comprise the size
    // of the request header.
    byte[] tag = new byte[TAG_SIZE_BYTES_V1];
    for (int idx = 0; idx < TAG_SIZE_BYTES_V1; ++idx) {
      tag[idx] = (byte) (headerSize >> ((TAG_SIZE_BYTES_V1 - idx - 1) * 8) & 0xFF);
    }

    // Concatenate the byte arrays as - tag array + header array + message array.
    byte[] requestByteArray = ArrayUtils.addAll(tag, headerByteArray);
    requestByteArray = ArrayUtils.addAll(requestByteArray, messageByteArray);

    return requestByteArray;
  }

  /**
   * Method to interpret data sent by server in response.
   * RPC responses are encoded on the wire as follows:
   * (1) First four bytes indicating the size of the RPC header in network byte
   * order.
   * (2) RPC header for the response.
   * (3) RPC response protobuf.
   * The RPC header itself contains the size of (3).
   *
   * @param receivedByteArray Complete RPC response to be deserialized.
   * @param responseType      The type into which the response should be deserialized.
   * @return Protobuf message filled with deserialized data.
   * @throws InsightsServiceRpcException   on rpc service exception.
   * @throws InsightsServiceRetryException on retry exception.
   */
  <R extends Message> R populateResponse(@Nullable final byte[] receivedByteArray,
                                         R.Builder responseType)
      throws InsightsServiceRpcException, InsightsServiceRetryException {

    if (receivedByteArray == null) {
      throw new InsightsServiceRetryException("Malformed RPC response. Received " +
                                              "an empty byte array");
    }

    // The first 4 bytes of the response contain the size of the response
    // header in network byte order.
    if (receivedByteArray.length < TAG_SIZE_BYTES_V1) {
      throw new InsightsServiceRpcException(
          String.format("Malformed RPC response. Response size: %d",
                        receivedByteArray.length));
    }

    int headerSize = 0;
    for (int idx = 0; idx < TAG_SIZE_BYTES_V1; ++idx) {
      final int value = receivedByteArray[idx];
      if (value < 0) {
        throw new InsightsServiceRpcException("Failed to extract the header " +
                                            "size while deserializing the RPC response.");
      }
      headerSize <<= 8;
      headerSize += value;
    }

    if (receivedByteArray.length < headerSize + TAG_SIZE_BYTES_V1) {
      throw new InsightsServiceRpcException(
          String.format("Malformed RPC response. Response Size: %d, Header " +
                        "Size: %d", receivedByteArray.length, headerSize));
    }

    // Extract the header out of receivedByteArray.
    byte[] headerByteArray = Arrays.copyOfRange(
        receivedByteArray,
        TAG_SIZE_BYTES_V1, /* the initial index of the range to be copied */
        TAG_SIZE_BYTES_V1 + headerSize /* the final index of the range to be copied, exclusive */);
    RpcProto.RpcResponseHeader.Builder rpcResponseHeader =
        RpcProto.RpcResponseHeader.newBuilder();
    try {
      rpcResponseHeader.mergeFrom(headerByteArray);
    }
    catch (InvalidProtocolBufferException e) {
      throw new InsightsServiceRpcException(
          "Unable to parse RPC response header: " + e.getMessage());
    }

    log.debug("Received RPC Response with header : {}", rpcResponseHeader);

    throwIfError(rpcResponseHeader);

    if (!rpcResponseHeader.hasProtobufSize()) {
      throw new InsightsServiceRpcException("RPC response header doesn't have response size");
    }

    // Parse the response protobuf. First extract the corresponding byte array
    // out of receivedByteArray.
    final int totalPayloadSize =
        TAG_SIZE_BYTES_V1 + headerSize + rpcResponseHeader.getProtobufSize();
    if (receivedByteArray.length < totalPayloadSize) {
      throw new InsightsServiceRpcException(
          String.format("Malformed RPC response. Response Size: %d, Header " +
                        "Size: %d, Protobuf Size: %d", receivedByteArray.length,
                        headerSize, rpcResponseHeader.getProtobufSize()));
    }

    byte[] responseByteArray = Arrays.copyOfRange(
        receivedByteArray,
        TAG_SIZE_BYTES_V1 + headerSize, /* the initial index of the range to be copied */
        TAG_SIZE_BYTES_V1 + headerSize + /* the final index of the range to be copied, exclusive */
        rpcResponseHeader.getProtobufSize());

    try {
      responseType.mergeFrom(responseByteArray);
    }
    catch (InvalidProtocolBufferException e) {
      throw new InsightsServiceRpcException(
          "Unable to parse RPC response protobuf: " + e.getMessage());
    }

    log.debug("Received RPC Response with protobuf : {}", responseType);
    return (R) responseType.build();
  }

  /**
   * Method to raise retry or service error exception.
   *
   * @param rpcResponseHeader The rpc response header returned after the rpc call.
   * @throws InsightsServiceRetryException on retry exception.
   * @throws InsightsServiceRpcException on rpc service exception.
   */
  void throwIfError(final RpcProto.RpcResponseHeader.Builder rpcResponseHeader)
      throws InsightsServiceRetryException, InsightsServiceRpcException {

    rpcStatus = rpcResponseHeader.getRpcStatus();
    rpcErrorDetail = rpcResponseHeader.getErrorDetail();

    if (rpcStatus != RpcProto.RpcResponseHeader.RpcStatus.kNoError) {
      log.info("{} recorded in response RPC as status", rpcStatus);
    }
    switch (rpcStatus) {
      case kNoError:
        return;
      case kAppError:
        handleAppError(rpcResponseHeader);
        return;
      case kTransportError:
        throw new InsightsServiceRetryException(rpcErrorDetail);
      default:
        final String errorString = String.format(
            "RPC error %s raised: %s", rpcStatus.name(),
            rpcErrorDetail);
        log.error(errorString);
        throw new InsightsServiceRpcException(
            errorString, rpcStatus.name()
        );
    }
  }

  /**
   * An abstract method to handle kAppError scenarios. This method must be
   * overridden by the service related fanout clients which inherit this class.
   *
   * @param rpcResponseHeader The rpc response header returned after the rpc call.
   * @throws InsightsServiceRetryException on retry exception.
   * @throws InsightsServiceRpcException   on rpc service exception.
   */
  abstract void handleAppError(final RpcProto.RpcResponseHeader.Builder rpcResponseHeader)
      throws InsightsServiceRetryException, InsightsServiceRpcException;


  abstract byte[] createRequestPayload(final RpcName rpcName, final Message rpcArg) throws InsightsServiceRpcException;

  /**
   * Method to invoke the fanout call using RestTemplate client.
   *
   * @param fanoutUrl      the URL to be invoked.
   * @param httpMethodType the HTTP method type to perform the REST operation.
   * @param requestEntity  the request HTTP object with headers set.
   * @return a ResponseEntity object encapsulating the response byte array.
   * @throws InsightsServiceRpcException on rpc service exception.
   */
  ResponseEntity<byte[]> invokeFanoutCall(final URI fanoutUrl,
                                          final HttpMethod httpMethodType,
                                          final HttpEntity<byte[]> requestEntity)
      throws InsightsServiceRpcException {

    ResponseEntity<byte[]> responseEntity;
    try {
      responseEntity = restTemplate.exchange(fanoutUrl,
                                             httpMethodType, requestEntity, byte[].class);
    }
    catch (Exception e) {
      log.error("Unable to make the fanout proxy call to mercury: ", e);
      throw new InsightsServiceRpcException("Unable to make the fanout proxy " +
                                          "call to mercury: " + e.getMessage());
    }
    return responseEntity;
  }
}
