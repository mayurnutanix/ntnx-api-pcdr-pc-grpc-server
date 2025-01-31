package com.nutanix.prism.pcdr.restserver.exceptions;

public class InsightsServiceException extends Exception{

  final ErrorCode errorCode;
  final String rpcError;


  public InsightsServiceException(final String message,
                                  final ErrorCode errorCode,
                                  final String rpcError) {
    super(message);
    this.errorCode = errorCode;
    this.rpcError = rpcError;
  }

  public InsightsServiceException(final String message,
                                  final Throwable cause,
                                  final ErrorCode errorCode,
                                  final String rpcError) {
    super(message, cause);
    this.errorCode = errorCode;
    this.rpcError = rpcError;
  }
}
