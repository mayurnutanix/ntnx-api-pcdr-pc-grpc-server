package com.nutanix.prism.pcdr.restserver.util;


import com.nutanix.api.utils.error.AppMessageBuilder;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.v4.*;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import dp1.pri.common.v1.config.MessageSeverity;
import dp1.pri.prism.v4.error.AppMessage;
import dp1.pri.prism.v4.error.ErrorResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Collections;
import java.util.Map;


@Slf4j
@Component
public class PCBRResponseFactory {

    private static final String DEFAULT_ARTIFACT_BASE_PATH = "/home/nutanix/api_artifacts";

    private static final String DEFAULT_ACCEPT_LANGUAGE_HEADER = "en-US";

    private static String getBasePath() {
        return DEFAULT_ARTIFACT_BASE_PATH;
    }

    public static ErrorResponse getDefaultStandardErrorResponse (String operation,final String acceptLanguageHeader) {
        final String locale = acceptLanguageHeader != null ? acceptLanguageHeader : DEFAULT_ACCEPT_LANGUAGE_HEADER;
        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.INTERNAL_SERVER_ERROR,
                ErrorCode.PCBR_INTERNAL_SERVER_ERROR,HttpStatus.INTERNAL_SERVER_ERROR,null);
        return createStandardErrorResponse(operation,exceptionDetails, locale);
    }

    public static ErrorResponse createStandardErrorResponse(
            String operation,final ExceptionDetailsDTO exceptionDetails, final String acceptLanguageHeader) {
        com.nutanix.devplatform.messages.models.AppMessage standardAppMessage =
                buildAppMessageFromException(operation, exceptionDetails, acceptLanguageHeader);
        final AppMessage appMessage = new AppMessage();
        appMessage.setMessage(standardAppMessage.getMessage());
        appMessage.setCode(standardAppMessage.getCode());
        appMessage.setSeverity(MessageSeverity.ERROR);
        final ErrorResponse errorResponse = new ErrorResponse();
        errorResponse.setErrorInWrapper(Collections.singletonList(appMessage));

        return errorResponse;
    }
    private static com.nutanix.devplatform.messages.models.AppMessage buildAppMessageFromException(String operation, ExceptionDetailsDTO exceptionDetails, String acceptLanguageHeader) {
        int errorCode = exceptionDetails.getErrorCode().getCode();
        Map<String, String> arguments = exceptionDetails.getArguments();
        if(!StringUtils.isEmpty(operation))
            arguments.put("op",operation);
        final String locale = acceptLanguageHeader != null ? acceptLanguageHeader : DEFAULT_ACCEPT_LANGUAGE_HEADER;
        return AppMessageBuilder.buildAppMessage(locale, getBasePath(), "prism", errorCode + "", arguments);
    }
}