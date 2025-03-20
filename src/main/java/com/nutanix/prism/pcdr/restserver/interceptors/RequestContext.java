package com.nutanix.prism.pcdr.restserver.interceptors;

import io.grpc.*;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.interceptor.GrpcGlobalServerInterceptor;

@GrpcGlobalServerInterceptor
@Slf4j
public class RequestContext implements ServerInterceptor {
  //Adonis cookies
  private static final String COOKIE = "cookie";
  private static final String FULL_VERSION = "full-version";
  private static final String X_FORWARDED_HOST = "x-forwarded-host";

  private static final String IF_MATCH = "if-match";
  private static final String IAM_TOKEN_CLAIMS = "token_claims";
  private static final String NTNX_REQUEST_ID = "NTNX-Request-Id";
  private static final String X_LICENSE_KEY = "X-License-Key";

  /**
   * In Context keys are accessed using reference comparison, hence it is
   * necessary to make them public here and access at required places.
   */
  public static final Context.Key<String> COOKIE_CONTEXT = Context.key(COOKIE);
  public static final Context.Key<String> FULL_VERSION_CONTEXT =
          Context.key(FULL_VERSION);
  public static final Context.Key<String> HOST_CONTEXT = Context.key(
          X_FORWARDED_HOST);

  public static final Context.Key<String> ETAG_VALUE = Context.key(IF_MATCH);

  /**
   * {"subject":"admin","userUUID":"00000000-0000-0000-0000-000000000000",
   * "groups":[],"tenant":{"uuid":"00000000-0000-0000-0000-000000000000"}
   */
  public static final Context.Key<String> IAM_TOKEN_CLAIMS_VALUE =
          Context.key(IAM_TOKEN_CLAIMS);

  public static final Context.Key<String> NTNX_REQUEST_ID_VALUE =
          Context.key(NTNX_REQUEST_ID);

  public static final Context.Key<String> X_LICENSE_KEY_VALUE =
          Context.key(X_LICENSE_KEY);


  @Override
  public <I, R> ServerCall.Listener<I> interceptCall(
          ServerCall<I, R> serverCall, Metadata metadata,
          ServerCallHandler<I, R> serverCallHandler) {
    log.debug("Metadata from Adonis {}", metadata);
    Context context = Context.current()
            .withValue(COOKIE_CONTEXT, metadata.get(Metadata.Key.of(
                    COOKIE, Metadata.ASCII_STRING_MARSHALLER)))
//            .withValue(FULL_VERSION_CONTEXT, metadata.get(Metadata.Key.of(
//                    FULL_VERSION, Metadata.ASCII_STRING_MARSHALLER)))
//            .withValue(HOST_CONTEXT, metadata.get(Metadata.Key.of(
//                    X_FORWARDED_HOST, Metadata.ASCII_STRING_MARSHALLER)))
            .withValue(ETAG_VALUE, metadata.get(Metadata.Key.of(
                    IF_MATCH, Metadata.ASCII_STRING_MARSHALLER)))
            .withValue(IAM_TOKEN_CLAIMS_VALUE, metadata.get(Metadata.Key.of(
                    IAM_TOKEN_CLAIMS, Metadata.ASCII_STRING_MARSHALLER)))
            .withValue(NTNX_REQUEST_ID_VALUE, metadata.get(Metadata.Key.of(
                    NTNX_REQUEST_ID, Metadata.ASCII_STRING_MARSHALLER)));
//            .withValue(X_LICENSE_KEY_VALUE, metadata.get(Metadata.Key.of(
//                    X_LICENSE_KEY, Metadata.ASCII_STRING_MARSHALLER)));
    return Contexts
            .interceptCall(context, serverCall, metadata, serverCallHandler);
  }
}
