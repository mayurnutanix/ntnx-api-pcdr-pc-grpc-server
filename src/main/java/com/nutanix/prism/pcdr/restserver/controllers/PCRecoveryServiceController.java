//package com.nutanix.prism.pcdr.restserver.controllers;
//
//import com.google.protobuf.ByteString;
//import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
//import com.nutanix.prism.pcdr.restserver.services.api.PCRestoreService;
//import com.nutanix.prism.pcdr.restserver.services.api.RestoreDataService;
//import com.nutanix.prism.pcdr.restserver.util.PCBRResponseFactory;
//import com.nutanix.prism.pcdr.restserver.util.PCUtil;
//import com.nutanix.prism.pcdr.restserver.util.RequestValidator;
//import com.nutanix.prism.pcdr.util.ExceptionUtil;
//import com.nutanix.util.base.UuidUtils;
//import dp1.pri.prism.v4.error.ErrorResponse;
//import dp1.pri.prism.v4.protectpc.*;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//import org.springframework.http.converter.json.MappingJacksonValue;
//import org.springframework.security.core.Authentication;
//import org.springframework.security.core.context.SecurityContext;
//import org.springframework.security.core.context.SecurityContextHolder;
//import org.springframework.util.ObjectUtils;
//import org.springframework.web.bind.annotation.RestController;
//import prism.v4.protectpc.PCRecoveryApiControllerInterface;
//
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.util.List;
//import java.util.Map;
//
//@RestController
//@Slf4j
//public class PCRecoveryServiceController
//    implements PCRecoveryApiControllerInterface {
//
//  private final PCRestoreService pcRestoreService;
//  private final RestoreDataService restoreDataService;
//  private final RequestValidator requestValidator;
//
//  @Autowired
//  public PCRecoveryServiceController(PCRestoreService pcRestoreService,
//                                     RestoreDataService restoreDataService,
//                                     RequestValidator requestValidator) {
//    this.pcRestoreService = pcRestoreService;
//    this.restoreDataService = restoreDataService;
//    this.requestValidator = requestValidator;
//  }
//
//  @Override
//  public ResponseEntity<MappingJacksonValue> recoverPcApi(
//    ReplicaInfo replicaInfo, Map<String, String> allQueryParams,
//    HttpServletRequest request, HttpServletResponse resp) {
//    SecurityContext securityContext = SecurityContextHolder.getContext();
//    Authentication authentication = securityContext.getAuthentication();
//    RecoverPcApiResponse response = new RecoverPcApiResponse();
//    String resolvedLocale = requestValidator.resolveLocale(request);
//    // Creating a response
//    // Check only one recovery source is selected as a time.
//    log.info("recoverPcApi called with replicaInfo : {} && allQueryParams : {}",
//            replicaInfo,allQueryParams);
//    if (!ObjectUtils.isEmpty(replicaInfo.getPeClusterIpList()) &&
//        replicaInfo.getPeClusterIpList().size() > 0 &&
//        !ObjectUtils.isEmpty(replicaInfo.getObjectStoreEndpoint())) {
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(
//              ErrorMessages.MULTIPLE_RECOVERY_POINTS_SELECTED_ERROR,
//              ErrorCode.PCBR_MULTIPLE_RECOVERY_SOURCE_PROVIDED, HttpStatus.BAD_REQUEST, null);
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//          ErrorArgumentKey.RECOVER_PC_API_OPERATION, exceptionDetails,resolvedLocale);
//      response.setDataInWrapper(errorResponse);
//        return ResponseEntity.status(
//                HttpStatus.BAD_REQUEST).body(
//                new MappingJacksonValue(response)
//        );
//    }
//    if (replicaInfo.getPeClusterIpList()== null ||
//      replicaInfo.getPeClusterIpList().isEmpty() ||
//      replicaInfo.getPeClusterId() == null ||
//      replicaInfo.getPeClusterId().isEmpty()) {
//
//      if(ObjectUtils.isEmpty(replicaInfo.getObjectStoreEndpoint()) ||
//         StringUtils.isEmpty(replicaInfo.getObjectStoreEndpoint().getEndpointAddress()) ||
//         replicaInfo.getObjectStoreEndpoint().getEndpointFlavour() == null ||
//              !(replicaInfo.getObjectStoreEndpoint().getEndpointFlavour() == PcEndpointFlavour.KS3 ||
//              replicaInfo.getObjectStoreEndpoint().getEndpointFlavour() == PcEndpointFlavour.KOBJECTS )) {
//
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.NOT_VALID_PAYLOAD_TO_CONTACT_BACKUP_TARGET,
//                ErrorCode.PCBR_NOT_VALID_BACKUP_TARGET_PAYLOAD, HttpStatus.BAD_REQUEST, null);
//        ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//            ErrorArgumentKey.RECOVER_PC_API_OPERATION, exceptionDetails,resolvedLocale);
//        response.setDataInWrapper(errorResponse);
//        return ResponseEntity.status(
//            HttpStatus.BAD_REQUEST).body(
//            new MappingJacksonValue(response)
//        );
//      }
//    }
//    try {
//      ByteString taskUuid =
//        pcRestoreService.restore(replicaInfo, authentication);
//      PcRestoreRootTask pcRestoreRootTask = new PcRestoreRootTask(
//        UuidUtils.getUUID(taskUuid));
//      response.setDataInWrapper(pcRestoreRootTask);
//      return ResponseEntity.status(HttpStatus.ACCEPTED).body(
//        new MappingJacksonValue(response));
//    } catch (Exception e) {
//      log.error("Error encountered while calling recover PC API", e);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.RECOVER_PC_API_OPERATION, exceptionDetails, resolvedLocale);
//      response.setDataInWrapper(errorResponse);
//      return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(
//        new MappingJacksonValue(response));
//    }
//  }
//
//  @Override
//  public ResponseEntity<MappingJacksonValue> restoreFilesApi(PcvmRestoreFiles pcvmRestoreFiles,
//    Map<String,String> allQueryParams, HttpServletRequest request,
//    HttpServletResponse resp) {
//    RestoreFilesApiResponse response = new RestoreFilesApiResponse();
//    String resolvedLocale = requestValidator.resolveLocale(request);
//    // Creating a response
//    try {
//      restoreDataService.restorePCVMFiles(pcvmRestoreFiles);
//      return ResponseEntity.status(HttpStatus.NO_CONTENT).body(new MappingJacksonValue(response));
//    } catch (Exception e) {
//      log.error("Unable to restore files because of the following issue:", e);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.RESTORE_FILES_OPERATION, exceptionDetails, resolvedLocale);
//      response.setDataInWrapper(errorResponse);
//      return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(
//        new MappingJacksonValue(response));
//    }
//  }
//
//  /**
//   * Function mapping to the API defined in protectpcapi.yaml
//   * --- get-recovery-status ---
//   * @param r - The received request argument.
//   * @param resp - Response argument.
//   * @return - Returns a response entity containing ApiSuccess in case of
//   * success else ApiError object.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> getRecoveryStatusAPI(
//    Map<String, String> allQueryParams,
//    HttpServletRequest r, HttpServletResponse resp) {
//    RecoveryStatusApiResponse response =
//      new RecoveryStatusApiResponse();
//    String resolvedLocale = requestValidator.resolveLocale(r);
//    try {
//      response.setDataInWrapper(pcRestoreService.getRecoveryStatus());
//      return ResponseEntity.ok(new MappingJacksonValue(response));
//    } catch (Exception e) {
//      log.error("The following error encountered ", e);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.GET_RECOVERY_STATUS_OPERATION, exceptionDetails, resolvedLocale);
//      response.setDataInWrapper(errorResponse);
//      return ResponseEntity.status(exceptionDetails.getHttpStatus())
//        .body(new MappingJacksonValue(response));
//    }
//  }
//
//  /**
//   * Function mapping to the API defined in recoverPcApi.yaml
//   * --- stop-services ---
//   * @param r - The received request argument.
//   * @param resp - Response argument.
//   * @return - Returns a response entity containing taskUuid in case of
//   * success else ApiError object.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> stopServicesApi(
//      List<String> body, Map<String, String> allQueryParams,
//      HttpServletRequest r, HttpServletResponse resp) {
//    StopServicesApiResponse response = new StopServicesApiResponse();
//    String resolvedLocale = requestValidator.resolveLocale(r);
//    try {
//      pcRestoreService.stopClusterServices(body);
//      String successMessage = "Successfully accepted the task" +
//                              " of stopping services.";
//      response.setDataInWrapper(PCUtil.getApiSuccess(successMessage));
//      return ResponseEntity.status(HttpStatus.ACCEPTED)
//                           .body(new MappingJacksonValue(response));
//    } catch (Exception e) {
//      log.error("Following exception encountered while creating " +
//                "task for stopping cluster services.", e);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.STOP_SERVICES_OPERATION, exceptionDetails, resolvedLocale);
//      response.setDataInWrapper(errorResponse);
//      return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(
//          new MappingJacksonValue(response));
//    }
//  }
//
//}
