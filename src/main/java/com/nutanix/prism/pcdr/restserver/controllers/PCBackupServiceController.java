//package com.nutanix.prism.pcdr.restserver.controllers;
//
//import com.nutanix.prism.pcdr.constants.Constants;
//import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
//import com.nutanix.prism.pcdr.messages.APIErrorMessages;
//import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
//import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
//import com.nutanix.prism.pcdr.restserver.util.*;
//import com.nutanix.prism.pcdr.util.ExceptionUtil;
//import com.nutanix.prism.pcdr.zklock.DistributedLock;
//import dp1.pri.prism.v4.error.ErrorResponse;
//import dp1.pri.prism.v4.protectpc.*;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.ObjectUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//import org.springframework.http.converter.json.MappingJacksonValue;
//import org.springframework.web.bind.annotation.RestController;
//import prism.v4.protectpc.PCBackupApiControllerInterface;
//
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.util.*;
//import java.util.stream.Collectors;
//
//
//@RestController
//@Slf4j
//public class PCBackupServiceController
//    implements PCBackupApiControllerInterface {
//
//  private final PCBackupService pcBackupService;
//  private final InstanceServiceFactory instanceServiceFactory;
//
//  private final ObjectStoreValidator objectStoreValidator;
//
//  private final RequestValidator requestValidator;
//
//  @Autowired
//  public PCBackupServiceController(PCBackupService pcBackupService,
//                                   InstanceServiceFactory instanceServiceFactory, ObjectStoreValidator objectStoreValidator,
//                                   RequestValidator requestValidator) {
//    this.pcBackupService = pcBackupService;
//    this.instanceServiceFactory = instanceServiceFactory;
//    this.objectStoreValidator = objectStoreValidator;
//    this.requestValidator = requestValidator;
//  }
//
//  /**
//   * Function mapping to the API defined in protectpcapi.yaml
//   * --- eligible-cluster-list ---
//   *
//   * @param r    - The received request argument.
//   * @param resp - Response argument.
//   * @return - Returns a response entity containing EligibleClusterList object.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> getEligibleClusterListAPI(
//    Map<String, String> allQueryParams, HttpServletRequest r,
//    HttpServletResponse resp) {
//    EligibleClusterListApiResponse response = new EligibleClusterListApiResponse();
//    String resolvedLocale = requestValidator.resolveLocale(r);
//    try {
//      // Receives the eligibleCLusterListDTO from pcBackupService and then
//      // setting data in response.
//      response.setDataInWrapper(pcBackupService.getEligibleClusterList(true));
//    }
//    catch (Exception e) {
//      log.error(APIErrorMessages.DEBUG_START, e);
//      // If an error occurred send an ApiError Message object.
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.GET_ELIGIBLE_CLUSTER_LIST_OPERATION, exceptionDetails, resolvedLocale);
//      response.setDataInWrapper(errorResponse);
//      return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(new MappingJacksonValue(response));
//    }
//    MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);
//    // Returning response
//    return ResponseEntity.ok(mappingJacksonValue);
//  }
//
//  @Override
//  public ResponseEntity<MappingJacksonValue> getFailedRecoveryPoints(
//      String entityId, Integer $page, Integer $limit,
//      Map<String, String> allQueryParams, HttpServletRequest request,
//      HttpServletResponse response) {
//    return null;
//  }
//
//
//  /**
//   * Function mapping to the API defined in protectpcapi.yaml
//   * --- remove-replica ---
//   *
//   * @param backupTargetID - UUID of the backup target from which replica
//   *                      will be removed.
//   * @param request       - The received request argument.
//   * @param resp          - Response argument.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> removeReplicaAPI(
//      String backupTargetID, Map<String, String> allQueryParams,
//      HttpServletRequest request,
//      HttpServletResponse resp) {
//    RemoveReplicaResponse response = new RemoveReplicaResponse();
//    DistributedLock distributedLock =
//      instanceServiceFactory.getDistributedLockForBackupPCDR();
//    ResponseEntity<MappingJacksonValue> responseEntity;
//    String resolvedLocale = requestValidator.resolveLocale(request);
//    try {
//      if (!distributedLock.lock(true)) {
//        log.warn(Constants.DISTRIBUTED_LOCKING_FAILED);
//        ErrorResponse errorResponse = PCBRResponseFactory.getDefaultStandardErrorResponse(
//                ErrorArgumentKey.REMOVE_REPLICA_OPERATION,resolvedLocale);
//        response.setDataInWrapper(errorResponse);
//        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);
//        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//                             .body(mappingJacksonValue);
//      }
//
//      pcBackupService.removeReplica(backupTargetID);
//      String successMessage = "DELETED SUCCESSFULLY";
//      MappingJacksonValue mappingJacksonValue =
//          new MappingJacksonValue(successMessage);
//      return ResponseEntity.status(HttpStatus.NO_CONTENT)
//                           .body(mappingJacksonValue);
//    }
//    catch (Exception e) {
//      log.error("Runtime error encountered while executing" +
//                " removeReplica API, stacktrace: ", e);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.REMOVE_REPLICA_OPERATION, exceptionDetails, resolvedLocale);
//      response.setDataInWrapper(errorResponse);
//      MappingJacksonValue mappingJacksonValue =
//          new MappingJacksonValue(response);
//      responseEntity = ResponseEntity.status(exceptionDetails.getHttpStatus()).body(mappingJacksonValue);
//    }
//    finally {
//      distributedLock.unlock();
//    }
//    return responseEntity;
//  }
//
//  /**
//   * Function mapping to the API defined in protectpcapi.yaml
//   * --- add-replicas ---
//   * @param backupTargets - An object containing Backup targets on which
//   *                         PC data is required to be backed up.
//   * @param r - The received request argument.
//   * @param resp - Response argument.
//   * @return - Returns a response entity containing ApiSuccess in case of
//   * success else ApiError object.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> addReplicasAPI(
//    BackupTargets backupTargets, Map<String, String> allQueryParams,
//    HttpServletRequest r, HttpServletResponse resp) {
//
//    AddReplicasApiResponse response = new AddReplicasApiResponse();
//    Set<String> clusterUuidSet = new HashSet<>();
//    String resolvedLocale = requestValidator.resolveLocale(r);
//    if (backupTargets.getClusterUuidList() != null) {
//      // Converting it into a set so that there are no duplicate values.
//      clusterUuidSet = new HashSet<>(
//          backupTargets.getClusterUuidList());
//    }
//    // It is possible that a null object store endpoint list is passed as
//    // part of input, so we are defining the object store endpoint list as
//    // empty list and if the input is not null assigning the value.
//    List<PcObjectStoreEndpoint> objectStoreEndpointList = new ArrayList<>();
//    String pcClusterUuid =
//            instanceServiceFactory.getClusterUuidFromZeusConfig();
//    if (backupTargets.getObjectStoreEndpointList() != null) {
//      // Append base object key ie. /pcdr/<pc_cluster_uuid> to endpoint address
//      for (PcObjectStoreEndpoint pcObjectStoreEndpoint :
//              backupTargets.getObjectStoreEndpointList()) {
//        try {
//          objectStoreValidator.validatePcObjectStoreEndpoint(pcObjectStoreEndpoint);
//
//          pcBackupService.generateEndpointAddress(pcObjectStoreEndpoint, pcClusterUuid);
//        } catch (Exception e) {
//          ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//          ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//                  exceptionDetails.getArguments(), e.getMessage());
//          ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                  ErrorArgumentKey.ADD_REPLICA_OPERATION, exceptionDetails, resolvedLocale);
//          response.setDataInWrapper(errorResponse);
//          MappingJacksonValue mappingJacksonValue =
//                  new MappingJacksonValue(response);
//          return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(mappingJacksonValue);
//        }
//        objectStoreEndpointList.add(pcObjectStoreEndpoint);
//
//        if(pcObjectStoreEndpoint.getEndpointCredentials() == null){
//          pcObjectStoreEndpoint.setEndpointCredentials(new PcEndpointCredentials());
//        }
//      }
//      // This is to ensure that the endpoint address is uniquely passed as
//      // part of object store endpoint in input.
//      objectStoreEndpointList =
//          new ArrayList<>(objectStoreEndpointList.stream().collect(
//              Collectors.toCollection(() -> new TreeSet<>(
//                  Comparator.comparing(
//                      PcObjectStoreEndpoint::getEndpointAddress)))));
//
//    }
//    int clusterUuidListLength = clusterUuidSet.size();
//    DistributedLock distributedLock =
//      instanceServiceFactory.getDistributedLockForBackupPCDR();
//    try {
//      if (!distributedLock.lock(true)) {
//        log.warn(Constants.DISTRIBUTED_LOCKING_FAILED);
//        ErrorResponse errorResponse = PCBRResponseFactory.getDefaultStandardErrorResponse(
//                ErrorArgumentKey.ADD_REPLICA_OPERATION,resolvedLocale);
//        response.setDataInWrapper(errorResponse);
//        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(response);
//        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(mappingJacksonValue);
//      }
//      // ClusterUuidList can't be empty and should be less than allowed backup
//      // clusters limit.
//      String errorMessage = "";
//      String errorCodeErrMsg = "";
//      if (clusterUuidListLength > Constants.MAX_ALLOWED_REPLICA_PE) {
//        errorMessage =
//            "Invalid number of clusters (" + clusterUuidListLength +
//            ") provided as part of input. Max Clusters " +
//            "supported count is :" + Constants.MAX_ALLOWED_REPLICA_PE;
//        errorCodeErrMsg = clusterUuidListLength + " clusters provided but only " + Constants.MAX_ALLOWED_REPLICA_PE + " are allowed";
//      } else if (objectStoreEndpointList.size() > Constants.MAX_ALLOWED_REPLICA_OBJECT_STORE) {
//        errorMessage = "Invalid number of object store endpoints (" +
//                              objectStoreEndpointList.size() +
//                              ") provided as part of input. Max endpoints " +
//                              "supported count is :" + Constants.MAX_ALLOWED_REPLICA_OBJECT_STORE;
//        errorCodeErrMsg = objectStoreEndpointList.size() + " object store endpoint provided but only " + Constants.MAX_ALLOWED_REPLICA_OBJECT_STORE + " are allowed";
//      } else if (clusterUuidSet.isEmpty() && objectStoreEndpointList.isEmpty()) {
//        errorMessage = "Empty cluster uuid and object store endpoint " +
//                              "provided as input.";
//        errorCodeErrMsg = "empty cluster uuid and object store endpoint";
//      }
//      if (!errorMessage.isEmpty()) {
//        Map<String,String> errorArguments = new HashMap<>();
//        errorArguments.put(ErrorCodeArgumentMapper.ARG_ERROR,errorCodeErrMsg);
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(errorMessage,
//                ErrorCode.PCBR_NOT_VALID_BACKUP_TARGET_PAYLOAD_OLD, HttpStatus.BAD_REQUEST, errorArguments);
//        ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                ErrorArgumentKey.ADD_REPLICA_OPERATION,exceptionDetails, resolvedLocale);
//        response.setDataInWrapper(errorResponse);
//        MappingJacksonValue mappingJacksonValue =
//            new MappingJacksonValue(response);
//        return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(mappingJacksonValue);
//      }
//      try {
//        // Add replicas to the PC.
//        pcBackupService.addReplicas(clusterUuidSet, objectStoreEndpointList);
//        String successMessage = "Successfully triggered backup for the " +
//          "given backup targets.";
//        response.setDataInWrapper(PCUtil.getApiSuccess(successMessage));
//        MappingJacksonValue mappingJacksonValue =
//          new MappingJacksonValue(response);
//        return ResponseEntity.ok(mappingJacksonValue);
//      } catch (Exception e) {
//        // TODO: Runtime exceptions must not be caught. Write a generic
//        //  exception handler that handles all exceptions which escapes our
//        //  catches.
//        log.error("Runtime error encountered while executing" +
//          " addReplicas API, stacktrace: ", e);
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//        ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//                exceptionDetails.getArguments(), e.getMessage());
//        ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                ErrorArgumentKey.ADD_REPLICA_OPERATION, exceptionDetails, resolvedLocale);
//        response.setDataInWrapper(errorResponse);
//        MappingJacksonValue mappingJacksonValue =
//          new MappingJacksonValue(response);
//        return ResponseEntity.status(
//                exceptionDetails.getHttpStatus()).body(mappingJacksonValue);
//      }
//    } finally {
//      distributedLock.unlock();
//    }
//  }
//
//  /**
//   * Function mapping to the API defined in protectpcapi.yaml
//   *  --- get-replicas ---
//   * @param r - The received request argument.
//   * @param resp - Response argument.
//   * @return - Returns a response entity containing EligibleClusterList object.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> getReplicasAPI(
//    Map<String, String> allQueryParams,
//    HttpServletRequest r, HttpServletResponse resp) {
//    GetReplicasApiResponse response = new GetReplicasApiResponse();
//    String resolvedLocale = requestValidator.resolveLocale(r);
//    // Creating a response
//    try {
//      // Receives the BackupTargetsInfo from pcBackupService and then
//      // setting data in response.
//      response.setDataInWrapper(pcBackupService.getReplicas());
//      MappingJacksonValue mappingJacksonValue =
//        new MappingJacksonValue(response);
//      return ResponseEntity.ok(mappingJacksonValue);
//    }
//    catch (Exception e) {
//      log.error("The following error encountered: ", e);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.GET_REPLICA_OPERATION, exceptionDetails, resolvedLocale);
//      response.setDataInWrapper(errorResponse);
//      return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(new MappingJacksonValue(response));
//    }
//  }
//
//  /**
//   * Function mapping to the API defined in protectpcapi.yaml
//   * --- reset-credentials ---
//   * @param pcEndpointCredentials - An object containing credential and certificate
//   *                                                  Info on for a given entityId.
//   * @param entityId - UniqueId for the object store endpoint.
//   * @param request - The received request argument.
//   * @param response - Response argument.
//   * @return - Returns a response entity containing ApiSuccess in case of
//   * success else ApiError object.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> updateCredentials(
//    PcEndpointCredentials pcEndpointCredentials,
//    String entityId, Map<String, String> allQueryParams,
//    HttpServletRequest request, HttpServletResponse response) {
//
//    UpdateCredentialApiResponse apiResponse = new UpdateCredentialApiResponse();
//    String resolvedLocale = requestValidator.resolveLocale(request);
//
//    DistributedLock distributedLock =
//        instanceServiceFactory.getDistributedLockForBackupPCDR();
//    try {
//      if (!distributedLock.lock(true)) {
//        log.warn(Constants.DISTRIBUTED_LOCKING_FAILED);
//        ErrorResponse errorResponse = PCBRResponseFactory.getDefaultStandardErrorResponse(
//                ErrorArgumentKey.UPDATE_CREDENTIAL_OPERATION,resolvedLocale);
//        apiResponse.setDataInWrapper(errorResponse);
//        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(apiResponse);
//        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(mappingJacksonValue);
//      }
//      try {
//        // Update credentials.
//        pcBackupService.updateCredentials(entityId,pcEndpointCredentials);
//        String successMessage = "Successfully updated the credentials.";
//        apiResponse.setDataInWrapper(PCUtil.getApiSuccess(successMessage));
//        MappingJacksonValue mappingJacksonValue =
//            new MappingJacksonValue(apiResponse);
//        return ResponseEntity.ok(mappingJacksonValue);
//      } catch (Exception e) {
//        log.error("Runtime Exception encountered while updating" +
//            " credentials API, stacktrace: ", e);
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//        ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//                exceptionDetails.getArguments(), e.getMessage());
//        ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                ErrorArgumentKey.UPDATE_CREDENTIAL_OPERATION, exceptionDetails, resolvedLocale);
//        apiResponse.setDataInWrapper(errorResponse);
//        MappingJacksonValue mappingJacksonValue =
//            new MappingJacksonValue(apiResponse);
//        return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(mappingJacksonValue);
//      }
//    } finally {
//      distributedLock.unlock();
//    }
//  }
//
//  /**
//   * Function mapping to the API defined in protectpcapi.yaml
//   * --- update-rpo ---
//   * @param rpoConfig - An object containing rpo configuration for a
//   *                         given entityId.
//   * @param entityId - UniqueId for the object store endpoint.
//   * @param request - The received request argument.
//   * @param response - Response argument.
//   * @return - Returns a response entity containing ApiSuccess in case of
//   * success else ApiError object.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> updateRpoConfiguration(
//    RpoConfig rpoConfig, String entityId,
//    Map<String, String> allQueryParams,
//    HttpServletRequest request, HttpServletResponse response) {
//    UpdateRpoApiResponse updateRpoApiResponse = new UpdateRpoApiResponse();
//    DistributedLock distributedLock =
//        instanceServiceFactory.getDistributedLockForBackupPCDR();
//    ResponseEntity<MappingJacksonValue> responseEntity;
//    String resolvedLocale = requestValidator.resolveLocale(request);
//    try {
//      int rpo = ObjectUtils.isNotEmpty(rpoConfig) ? rpoConfig.getRpoSeconds() : 0;
//      log.debug("Validating rpo {}",rpo);
//      objectStoreValidator.validateRpo(rpo);
//    } catch (Exception e) {
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.UPDATE_RPO_CONFIG_OPERATION, exceptionDetails, resolvedLocale);
//      updateRpoApiResponse.setDataInWrapper(errorResponse);
//      MappingJacksonValue mappingJacksonValue =
//          new MappingJacksonValue(updateRpoApiResponse);
//      return ResponseEntity.status(exceptionDetails.getHttpStatus()).body(mappingJacksonValue);
//    }
//    try {
//      if (!distributedLock.lock(true)) {
//        log.warn(Constants.DISTRIBUTED_LOCKING_FAILED);
//        ErrorResponse errorResponse = PCBRResponseFactory.getDefaultStandardErrorResponse(
//                ErrorArgumentKey.UPDATE_RPO_CONFIG_OPERATION,resolvedLocale);
//        updateRpoApiResponse.setDataInWrapper(errorResponse);
//        MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(updateRpoApiResponse);
//        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
//            .body(mappingJacksonValue);
//      }
//
//      pcBackupService.updateRpo(entityId,rpoConfig);
//      String successMessage = "RPO configuration updated";
//      updateRpoApiResponse.setDataInWrapper(PCUtil.getApiSuccess(successMessage));
//      MappingJacksonValue mappingJacksonValue =
//          new MappingJacksonValue(updateRpoApiResponse);
//      return ResponseEntity.ok(mappingJacksonValue);
//    }
//    catch (Exception e) {
//      log.error("Runtime error encountered while executing" +
//          " updateRpo API, stacktrace: ", e);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//              exceptionDetails.getArguments(), e.getMessage());
//      ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.UPDATE_RPO_CONFIG_OPERATION, exceptionDetails, resolvedLocale);
//      updateRpoApiResponse.setDataInWrapper(errorResponse);
//      MappingJacksonValue mappingJacksonValue =
//          new MappingJacksonValue(updateRpoApiResponse);
//      responseEntity = ResponseEntity.status(
//              exceptionDetails.getHttpStatus()).body(mappingJacksonValue);
//    }
//    finally {
//      distributedLock.unlock();
//    }
//    return responseEntity;
//  }
//}
