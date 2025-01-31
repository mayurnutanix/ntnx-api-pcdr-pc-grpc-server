package com.nutanix.prism.pcdr.restserver.exceptions;

public class InsightsServiceRetryException extends InsightsServiceException {

  private static final String RETRY_REQUESTED =
      "Error encountered accessing Insight service, retry requested";

  private static final String K_RETRY_ERROR = "kRetry";

  public InsightsServiceRetryException() {
    super(RETRY_REQUESTED, ErrorCode.INSIGHTS_SERVICE_RETRY, K_RETRY_ERROR);
  }

  public InsightsServiceRetryException(final String message) {
    super(message, ErrorCode.INSIGHTS_SERVICE_RETRY, K_RETRY_ERROR);
  }
}
