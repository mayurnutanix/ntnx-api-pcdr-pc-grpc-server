package com.nutanix.prism.pcdr.restserver.exceptions;

public class InsightsServiceRpcException extends InsightsServiceException {

  private static final String SERVICE_ERROR =
      "Error encountered while accessing Insight service";

  private static final String K_APP_ERROR = "kAppError";

  public InsightsServiceRpcException() {
    super(SERVICE_ERROR, ErrorCode.INSIGHTS_SERVICE_RPC, K_APP_ERROR);
  }

  public InsightsServiceRpcException(final String message,
                                   final ErrorCode errorCode) {
    super(message, errorCode, K_APP_ERROR);
  }

  public InsightsServiceRpcException(final String message,
                                   final ErrorCode errorCode,
                                   final String rpcError) {
    super(message, errorCode, rpcError);
  }

  public InsightsServiceRpcException(final String message,
                                   final String rpcError) {
    super(message, ErrorCode.INSIGHTS_SERVICE_RPC, rpcError);
  }

  public InsightsServiceRpcException(final String message) {
    super(message, ErrorCode.INSIGHTS_SERVICE_RPC, K_APP_ERROR);
  }
}
