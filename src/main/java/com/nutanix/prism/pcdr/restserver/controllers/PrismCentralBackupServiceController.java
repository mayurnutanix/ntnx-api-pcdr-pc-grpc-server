//package com.nutanix.prism.pcdr.restserver.controllers;
//
///*
// * Copyright (c) 2024 Nutanix Inc. All rights reserved.
// *
// * Author: shyam.sodankoor@nutanix.com
// *
// * Convert old models to new models
// * and vice versa
// */
//
//import com.google.protobuf.ByteString;
//import com.nutanix.api.utils.task.ErgonTaskUtils;
//import com.nutanix.prism.pcdr.constants.Constants;
//import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
//import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
//import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
//import com.nutanix.prism.pcdr.restserver.converters.PrismCentralBackupConverter;
//import com.nutanix.prism.pcdr.restserver.factory.BackupTargetHelper;
//import com.nutanix.prism.pcdr.restserver.handlers.AsyncMessageHandler;
//import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
//import com.nutanix.prism.pcdr.restserver.util.*;
//import com.nutanix.prism.pcdr.util.DataMaskUtil;
//import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
//import com.nutanix.prism.pcdr.util.ExceptionUtil;
//import com.nutanix.prism.pcdr.util.UserInformationUtil;
//import com.nutanix.util.base.UuidUtils;
//import dp1.pri.prism.v4.config.TaskReference;
//import dp1.pri.prism.v4.error.ErrorResponse;
//import dp1.pri.prism.v4.management.*;
//import dp1.pri.prism.v4.protectpc.BackupTargets;
//import dp1.pri.prism.v4.protectpc.BackupTargetsInfo;
//import dp1.pri.prism.v4.protectpc.PcEndpointCredentials;
//import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.http.HttpHeaders;
//import org.springframework.http.HttpStatus;
//import org.springframework.http.ResponseEntity;
//import org.springframework.http.converter.json.MappingJacksonValue;
//import org.springframework.security.core.Authentication;
//import org.springframework.security.core.context.SecurityContextHolder;
//import org.springframework.util.ObjectUtils;
//import org.springframework.web.bind.annotation.RestController;
//import prism.v4.management.DomainManagerBackupsApiControllerInterface;
//
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.util.*;
//import java.util.concurrent.CompletableFuture;
//
//import static com.nutanix.prism.constants.IdempotencySupportConstants.NTNX_REQUEST_ID;
//import static com.nutanix.prism.pcdr.restserver.constants.Constants.DELETE_BACKUP_TARGET;
//import static com.nutanix.prism.pcdr.restserver.constants.Constants.DELETE_BACKUP_TARGET_OPERATION_TYPE;
//
///**
// * Controller for handling all the v4 Backup related APIs for PC.
// */
//
//@RestController
//@Slf4j
//public class PrismCentralBackupServiceController implements DomainManagerBackupsApiControllerInterface {
//
//    private final PrismCentralBackupConverter prismCentralBackupConverter;
//
//    private final PCBackupService pcBackupService;
//
//    private final InstanceServiceFactory instanceServiceFactory;
//
//    private final ObjectStoreValidator objectStoreValidator;
//
//    private final AsyncMessageHandler asyncMessageHandler;
//
//    private RequestValidator requestValidator;
//
//    private BackupTargetHelper backupTargetHelper;
//
//    private IdempotencyUtil idempotencyUtil;
//
//    private  ErgonServiceHelper ergonServiceHelper;
//
//    private BackupTargetUtil backupTargetUtil;
//
//
//    @Autowired
//    public PrismCentralBackupServiceController(PrismCentralBackupConverter prismCentralBackupConverter, PCBackupService pcBackupService, InstanceServiceFactory instanceServiceFactory, ObjectStoreValidator objectStoreValidator, AsyncMessageHandler asyncMessageHandler, RequestValidator requestValidator, BackupTargetHelper backupTargetHelper, IdempotencyUtil idempotencyUtil, ErgonServiceHelper ergonServiceHelper, BackupTargetUtil backupTargetUtil) {
//        this.prismCentralBackupConverter = prismCentralBackupConverter;
//        this.pcBackupService = pcBackupService;
//        this.instanceServiceFactory = instanceServiceFactory;
//        this.objectStoreValidator = objectStoreValidator;
//        this.asyncMessageHandler = asyncMessageHandler;
//        this.requestValidator = requestValidator;
//        this.backupTargetHelper = backupTargetHelper;
//        this.idempotencyUtil = idempotencyUtil;
//        this.ergonServiceHelper = ergonServiceHelper;
//        this.backupTargetUtil = backupTargetUtil;
//    }
//
//    @Override
//    public ResponseEntity<MappingJacksonValue> createBackupTarget(BackupTarget body, String domainManagerExtId, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//
//        CreateBackupTargetApiResponse createBackupTargetResponse = new CreateBackupTargetApiResponse();
//        TaskReference taskReference = new TaskReference();
//        ByteString taskId;
//        String peClusterUuid = StringUtils.EMPTY;
//        String pcClusterUuid =
//              instanceServiceFactory.getClusterUuidFromZeusConfig();
//        Set<String> peClusterUuidSet = new HashSet<>();
//        List<PcObjectStoreEndpoint> pcObjectStoreEndpointList = new ArrayList<>();
//        final String requestId = request.getHeader(NTNX_REQUEST_ID);
//        String resolvedLocale = requestValidator.resolveLocale(request);
//        String backupTargetStr = DataMaskUtil.maskSecrets(body.toString());
//        try {
//            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
//            String userId = UserInformationUtil.getUserUUID(authentication);
//            String idempotentTaskId  = idempotencyUtil.getIdempotentTaskInfo(requestId, userId);
//            // check if nutanix request id is already referenced and return the existing task.
//            if (!StringUtils.isEmpty(idempotentTaskId)){
//                taskReference = new TaskReference(ErgonTaskUtils.getTaskReferenceExtId(idempotentTaskId));
//                createBackupTargetResponse.setDataInWrapper(taskReference);
//                return new ResponseEntity<>(new MappingJacksonValue(createBackupTargetResponse),
//                        HttpStatus.ACCEPTED);
//            }
//            // Verify that domainManagerExtId == current PC clusterId
//            requestValidator.validatePcClusterUuid(domainManagerExtId, pcClusterUuid);
//            // Validate the inputs for cluster and ObjectStore
//            BackupTargetValidator backupTargetValidator = backupTargetHelper.getBackupTargetValidator(body);
//            backupTargetValidator.validateBackupTarget(body);
//            BackupTargets backupTargets = prismCentralBackupConverter.getBackupTargetsfromBackupTarget(body);
//            if (!ObjectUtils.isEmpty(backupTargets.getClusterUuidList())){
//                peClusterUuid = backupTargets.getClusterUuidList().get(0);
//                peClusterUuidSet = new HashSet<>(backupTargets.getClusterUuidList());
//            }
//            if (!ObjectUtils.isEmpty(backupTargets.getObjectStoreEndpointList())){
//              PcObjectStoreEndpoint pcObjectStoreEndpoint = backupTargets.getObjectStoreEndpointList().get(0);
//              pcBackupService.generateEndpointAddress(pcObjectStoreEndpoint, pcClusterUuid);
//              // In case of NC2, if credentials are not provided,
//              // setting it with an empty object
//              if(pcObjectStoreEndpoint.getEndpointCredentials() == null){
//                pcObjectStoreEndpoint.setEndpointCredentials(new PcEndpointCredentials());
//              }
//              pcObjectStoreEndpointList.add(pcObjectStoreEndpoint);
//            }
//
//            // Check if the backup target is eligible for Backup
//            pcBackupService.isBackupTargetEligibleForBackup(peClusterUuidSet,
//                    pcObjectStoreEndpointList);
//
//            log.info("Create Backup Target Body {}", backupTargetStr);
//            taskId = ergonServiceHelper.createTask(Constants.PCDR_TASK_COMPONENT_TYPE, "Create Backup Target", "kCreateBackupTarget",
//                        authentication, pcClusterUuid, peClusterUuid);
//
//            taskReference.setExtId(ErgonTaskUtils.getTaskReferenceExtId(UuidUtils.getUUID(taskId)));
//            asyncMessageHandler.createBackupTargetAsync(backupTargets, taskId);
//            // update request reference table with task id and request id.
//            idempotencyUtil.updateTaskReferenceTable(UuidUtils.getUUID(taskId), requestId,userId);
//        } catch (Exception e){
//            log.error("Failed to create Backup Target with exception: ", e);
//            ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//            ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//                    exceptionDetails.getArguments(), e.getMessage());
//            ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                  ErrorArgumentKey.CREATE_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
//            createBackupTargetResponse.setDataInWrapper(errorResponse);
//            createBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(true));
//            return new ResponseEntity<>(new MappingJacksonValue(createBackupTargetResponse), exceptionDetails.getHttpStatus());
//        }
//        createBackupTargetResponse.setDataInWrapper(taskReference);
//        createBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(Collections.singletonList(ApiLinkUtil.getApiLinkForTaskReference(taskReference)), false, false));
//        MappingJacksonValue mappingJacksonValue =
//                new MappingJacksonValue(createBackupTargetResponse);
//        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.ACCEPTED);
//    }
//
//
//
//    /**
//     * API for deleting backup target on PC. Expects etag to be present as part of "If-Match" header.
//     *
//     * @param extId          backup target id
//     * @param allQueryParams query params.
//     * @param request        request.
//     * @param response       response.
//     * @return task id if deletion api was called. The task needs to be polled in order to gets its status.
//     */
//    @Override
//    public ResponseEntity<MappingJacksonValue> deleteBackupTargetById(String domainManagerExtId, String extId, Map<String, String> allQueryParams,
//                                                                      HttpServletRequest request,
//                                                                      HttpServletResponse response) {
//        log.info("Request received to remove Backup target : {}", extId);
//        TaskReference taskReference = new TaskReference();
//        DeleteBackupTargetApiResponse deleteBackupTargetResponse = new DeleteBackupTargetApiResponse();
//        ByteString taskId;
//        String peClusterUuid = StringUtils.EMPTY;
//        final String requestId = request.getHeader(NTNX_REQUEST_ID);
//        String resolvedLocale = requestValidator.resolveLocale(request);
//        String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
//        try {
//            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
//            String userId = UserInformationUtil.getUserUUID(authentication);
//            // check if ntnx request id is already referenced and return the existing task.
//            String idempotentTaskId  = idempotencyUtil.getIdempotentTaskInfo(requestId, userId);
//            if (!StringUtils.isEmpty(idempotentTaskId)){
//                taskReference = new TaskReference(ErgonTaskUtils.getTaskReferenceExtId(idempotentTaskId));
//                deleteBackupTargetResponse.setDataInWrapper(taskReference);
//                return new ResponseEntity<>(new MappingJacksonValue(deleteBackupTargetResponse),
//                        HttpStatus.ACCEPTED);
//            }
//            // Verify that domainManagerExtId == current PC clusterId
//            requestValidator.validatePcClusterUuid(domainManagerExtId, pcClusterUuid);
//            BackupTarget backupTarget = backupTargetUtil.getBackupTarget(extId);
//            if(backupTarget.getLocation() instanceof ClusterLocation) {
//                peClusterUuid = backupTarget.getExtId();
//            }
//            String eTag = request.getHeader(HttpHeaders.IF_MATCH);
//            if (!StringUtils.isEmpty(eTag)) {
//                // remove double quotes
//                eTag = eTag.replace("\"", "");
//            }
//            requestValidator.checkIfEtagMatch(eTag, backupTarget);
//            taskId = ergonServiceHelper.createTask(Constants.PCDR_TASK_COMPONENT_TYPE,
//                    DELETE_BACKUP_TARGET,
//                    DELETE_BACKUP_TARGET_OPERATION_TYPE,
//                    authentication, pcClusterUuid, peClusterUuid);
//            taskReference.setExtId(ErgonTaskUtils.getTaskReferenceExtId(UuidUtils.getUUID(taskId)));
//            deleteBackupTargetResponse.setDataInWrapper(taskReference);
//            deleteBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(
//                Collections.singletonList(ApiLinkUtil.getApiLinkForTaskReference(taskReference)), false, false)
//            );
//            asyncMessageHandler.deleteBackupTargetAsync(taskId, eTag, extId);
//            // update request reference table with task id and request id.
//            idempotencyUtil.updateTaskReferenceTable(UuidUtils.getUUID(taskId), requestId, userId);
//            return new ResponseEntity<>(new MappingJacksonValue(deleteBackupTargetResponse), HttpStatus.ACCEPTED);
//        } catch (Exception e) {
//            log.error("Exception while deleting backup target {} ", extId, e);
//            ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//            ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//                    exceptionDetails.getArguments(), e.getMessage());
//            ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                    ErrorArgumentKey.DELETE_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
//            deleteBackupTargetResponse.setDataInWrapper(errorResponse);
//            deleteBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(true));
//            return new ResponseEntity<>(new MappingJacksonValue(deleteBackupTargetResponse),
//                                        exceptionDetails.getHttpStatus());
//        }
//    }
//
//
//
//    /**
//     * API for retreiving the backup target by ID.
//     *
//     * @param extId          id of the backup target.
//     * @param allQueryParams query params.
//     * @param request        request
//     * @param response       response.
//     * @return Backup target on success.
//     */
//    @Override
//    public ResponseEntity<MappingJacksonValue> getBackupTargetById(String domainManagerExtId, String extId, Map<String, String> allQueryParams,
//                                                                   HttpServletRequest request,
//                                                                   HttpServletResponse response) {
//        log.info("Request received to get Backup Target : {}", extId);
//        GetBackupTargetApiResponse backupTargetApiResponse = new GetBackupTargetApiResponse();
//        String resolvedLocale = requestValidator.resolveLocale(request);
//        String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
//        try {
//            // Verify that domainManagerExtId == current PC clusterId
//            requestValidator.validatePcClusterUuid(domainManagerExtId, pcClusterUuid);
//            BackupTarget backupTarget = backupTargetUtil.getBackupTarget(extId);
//            HttpHeaders headers = new HttpHeaders();
//            headers.set(HttpHeaders.ETAG, pcBackupService.generateEtag(backupTarget));
//            backupTargetApiResponse.setDataInWrapper(backupTarget);
//            backupTargetApiResponse.setMetadata(ResponseUtil.createMetadataFor(Collections.singletonList(ApiLinkUtil.getSelfLink()), false, false));
//            return new ResponseEntity<>(new MappingJacksonValue(backupTargetApiResponse), headers, HttpStatus.OK);
//        } catch (Exception e) {
//            log.error("Exception while retrieving backup target {} ", extId, e);
//            ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//            ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//                    exceptionDetails.getArguments(), e.getMessage());
//            ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                    ErrorArgumentKey.GET_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
//            backupTargetApiResponse.setDataInWrapper(errorResponse);
//            backupTargetApiResponse.setMetadata(ResponseUtil.createMetadataFor(true));
//            return new ResponseEntity<>(new MappingJacksonValue(backupTargetApiResponse),
//                    exceptionDetails.getHttpStatus());
//        }
//    }
//
//    @Override
//    public CompletableFuture<ResponseEntity<MappingJacksonValue>> getRestorePointById(String sourceExtId, String restorableDomainManagerExtId, String extId, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//      String locale = requestValidator.resolveLocale(request);
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.API_NOT_IMPLEMENTED_ON_PC,
//                ErrorCode.PCBR_API_NOT_ACCESSIBLE_ON_PC, HttpStatus.NOT_IMPLEMENTED,null);
//        ErrorResponse errorResponse =
//          PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.GET_RESTORATION_INFO_OPERATION,
//              exceptionDetails,
//              locale);
//      return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
//          .body(new MappingJacksonValue(errorResponse)));    }
//
//    @Override
//    public ResponseEntity<MappingJacksonValue> updateBackupTargetById(BackupTarget body, String domainManagerExtId, String extId, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//
//        log.info("Request received to Update Backup Target for {}", extId);
//        TaskReference taskReference = new TaskReference();
//        ByteString taskId;
//        String backupTargetStr = DataMaskUtil.maskSecrets(body.toString());
//        String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
//        BackupTargets backupTargets = prismCentralBackupConverter.getBackupTargetsfromBackupTarget(body);
//
//        UpdateBackupTargetApiResponse updateBackupTargetResponse = new UpdateBackupTargetApiResponse();
//        final String requestId = request.getHeader(NTNX_REQUEST_ID);
//        String resolvedLocale = requestValidator.resolveLocale(request);
//        try {
//            Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
//            String userId = UserInformationUtil.getUserUUID(authentication);
//            String idempotentTaskId  = idempotencyUtil.getIdempotentTaskInfo(requestId, userId);
//            // check if nutanix request id is already referenced and return the existing task.
//            if (!StringUtils.isEmpty(idempotentTaskId)){
//                taskReference = new TaskReference(ErgonTaskUtils.getTaskReferenceExtId(idempotentTaskId));
//                updateBackupTargetResponse.setDataInWrapper(taskReference);
//                return new ResponseEntity<>(new MappingJacksonValue(updateBackupTargetResponse),
//                        HttpStatus.ACCEPTED);
//            }
//            // Verify that domainManagerExtId == current PC clusterId
//            requestValidator.validatePcClusterUuid(domainManagerExtId, pcClusterUuid);
//            if (!(body.getLocation() instanceof ObjectStoreLocation)){
//                //Todo : Check message thrown is correct ?
//                ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(
//                        ErrorMessages.getInvalidInputProvidedMessage(ErrorArgumentKey.OBJECT_STORE),
//                        ErrorCode.PCBR_BACKUP_TARGET_CLUSTER_PROVIDED_OBJECT_STORE_NEEDED,
//                        HttpStatus.BAD_REQUEST, null);
//                ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                        ErrorArgumentKey.UPDATE_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
//                updateBackupTargetResponse.setDataInWrapper(errorResponse);
//                updateBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(true));
//                MappingJacksonValue mappingJacksonValue = new MappingJacksonValue(updateBackupTargetResponse);
//                return new ResponseEntity<>(mappingJacksonValue, HttpStatus.BAD_REQUEST);
//            } else {
//                objectStoreValidator.validateObjectStoreForUpdate((ObjectStoreLocation) body.getLocation(), extId);
//            }
//            BackupTarget backupTarget = backupTargetUtil.getBackupTarget(extId);
//            String eTag = request.getHeader(HttpHeaders.IF_MATCH);
//            if (!StringUtils.isEmpty(eTag)) {
//              // remove double quotes
//              eTag = eTag.replace("\"", "");
//            }
//            requestValidator.checkIfEtagMatch(eTag, backupTarget);
//            log.info("Update Backup Target Body {}", backupTargetStr);
//            taskId = ergonServiceHelper.createTask(Constants.PCDR_TASK_COMPONENT_TYPE, "Update Backup Target", "kUpdateBackupTarget",
//                    authentication, pcClusterUuid, StringUtils.EMPTY);
//            taskReference.setExtId(ErgonTaskUtils.getTaskReferenceExtId(UuidUtils.getUUID(taskId)));
//            asyncMessageHandler.updateBackupTargetAsync(backupTargets, taskId, eTag, extId);
//            // update request reference table with task id and request id.
//            idempotencyUtil.updateTaskReferenceTable(UuidUtils.getUUID(taskId), requestId, userId);
//        } catch (Exception e) {
//            // This is to catch any other exception that we have missed to handle
//            log.error("Failed to Update Backup Target with exception: ", e);
//            ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//            ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//                    exceptionDetails.getArguments(), e.getMessage());
//            ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                    ErrorArgumentKey.UPDATE_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
//            updateBackupTargetResponse.setDataInWrapper(errorResponse);
//            updateBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(true));
//            MappingJacksonValue mappingJacksonValue =
//                    new MappingJacksonValue(updateBackupTargetResponse);
//            return new ResponseEntity<>(mappingJacksonValue, exceptionDetails.getHttpStatus());
//        }
//        updateBackupTargetResponse.setDataInWrapper(taskReference);
//        updateBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(Collections.singletonList(ApiLinkUtil.getApiLinkForTaskReference(taskReference)), false, false));
//        MappingJacksonValue mappingJacksonValue =
//                new MappingJacksonValue(updateBackupTargetResponse);
//        return new ResponseEntity<>(mappingJacksonValue, HttpStatus.ACCEPTED);
//    }
//
//
//
//    /**
//   * Function mapping to the get API defined in backupTarget.yaml
//   *  --- backup-targets ---
//   * @param request - The received request argument.
//   * @param response - Response argument.
//   * @return - Returns a response entity containing BackupTarget List object.
//   */
//  @Override
//  public ResponseEntity<MappingJacksonValue> listBackupTargets(String domainManagerExtId, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//    ListBackupTargetsApiResponse listBackupTargetsApiResponse = new ListBackupTargetsApiResponse();
//    String resolvedLocale = requestValidator.resolveLocale(request);
//    String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
//
//    //Creating a response
//    try {
//      // Verify that domainManagerExtId == current PC clusterId
//      requestValidator.validatePcClusterUuid(domainManagerExtId, pcClusterUuid);
//      // Receives the ListBackupTargetResponse from pcBackupServiceAdapter and then
//      // setting data in response.
//      BackupTargetsInfo backupTargetsInfo = pcBackupService.getReplicas();
//      listBackupTargetsApiResponse.setDataInWrapper(prismCentralBackupConverter.getListBackupTargetFromBackupTargetsInfo(backupTargetsInfo));
//      listBackupTargetsApiResponse.setMetadata(ResponseUtil.createMetadataFor(Collections.singletonList(ApiLinkUtil.getSelfLink()), false, false));
//      return new ResponseEntity<>(new MappingJacksonValue(listBackupTargetsApiResponse), HttpStatus.OK);
//    } catch (Exception e) {
//        log.error("The following error encountered while listBackupTargets: ", e);
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
//        ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
//                exceptionDetails.getArguments(), e.getMessage());
//        ErrorResponse errorResponse = PCBRResponseFactory.createStandardErrorResponse(
//                ErrorArgumentKey.LIST_BACKUP_TARGETS_OPERATION, exceptionDetails, resolvedLocale);
//        listBackupTargetsApiResponse.setDataInWrapper(errorResponse);
//        listBackupTargetsApiResponse.setMetadata(ResponseUtil.createMetadataFor(true));
//        return new ResponseEntity<>(new MappingJacksonValue(listBackupTargetsApiResponse),
//                exceptionDetails.getHttpStatus());
//    }
//  }
//
//    @Override
//    public ResponseEntity<MappingJacksonValue> createRestoreSource(RestoreSource body, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//      String locale = requestValidator.resolveLocale(request);
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.API_NOT_IMPLEMENTED_ON_PC,
//                ErrorCode.PCBR_API_NOT_ACCESSIBLE_ON_PC, HttpStatus.NOT_IMPLEMENTED, null);
//        ErrorResponse errorResponse =
//          PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.CREATE_RESTORE_SOURCE_OPERATION,
//              exceptionDetails,
//              locale);
//      return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED).body(new MappingJacksonValue(errorResponse));
//    }
//
//    @Override
//    public ResponseEntity<MappingJacksonValue> deleteRestoreSourceById(String extId, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//      String locale = requestValidator.resolveLocale(request);
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.API_NOT_IMPLEMENTED_ON_PC,
//                ErrorCode.PCBR_API_NOT_ACCESSIBLE_ON_PC, HttpStatus.NOT_IMPLEMENTED, null);
//        ErrorResponse errorResponse =
//          PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.DELETE_RESTORE_SOURCE_OPERATION,
//              exceptionDetails,
//              locale);
//      return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
//          .body(new MappingJacksonValue(errorResponse));
//    }
//
//    @Override
//    public CompletableFuture<ResponseEntity<MappingJacksonValue>> listRestorableDomainManagers(String sourceExtId, Integer $page, Integer $limit, String $filter, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//      String locale = requestValidator.resolveLocale(request);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.API_NOT_IMPLEMENTED_ON_PC,
//            ErrorCode.PCBR_API_NOT_ACCESSIBLE_ON_PC, HttpStatus.NOT_IMPLEMENTED, null);
//      ErrorResponse errorResponse =
//          PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.LIST_RESTORABLE_DOMAIN_MANAGERS,
//              exceptionDetails,
//              locale);
//      return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
//          .body(new MappingJacksonValue(errorResponse)));
//    }
//
//    @Override
//    public CompletableFuture<ResponseEntity<MappingJacksonValue>> listRestorePoints(String sourceExtId, String domainManagerExtId, Integer $page, Integer $limit, String $filter, String $orderby, String $select, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//      String locale = requestValidator.resolveLocale(request);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.API_NOT_IMPLEMENTED_ON_PC,
//                ErrorCode.PCBR_API_NOT_ACCESSIBLE_ON_PC, HttpStatus.NOT_IMPLEMENTED, null);
//      ErrorResponse errorResponse =
//          PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.LIST_RESTORE_POINTS_OPERATION,
//              exceptionDetails,
//              locale);
//      return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
//          .body(new MappingJacksonValue(errorResponse)));
//    }
//
//    @Override
//    public CompletableFuture<ResponseEntity<MappingJacksonValue>> restore(RestoreSpec body, String sourceExtId, String domainManagerExtId, String extId, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//      String locale = requestValidator.resolveLocale(request);
//      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.API_NOT_IMPLEMENTED_ON_PC,
//                ErrorCode.PCBR_API_NOT_ACCESSIBLE_ON_PC, HttpStatus.NOT_IMPLEMENTED, null);
//      ErrorResponse errorResponse =
//          PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.RESTORE_DOMAIN_MANAGER,
//              exceptionDetails,
//              locale);
//      return CompletableFuture.completedFuture(ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
//          .body(new MappingJacksonValue(errorResponse)));
//    }
//
//    @Override
//    public ResponseEntity<MappingJacksonValue> getRestoreSourceById(String extId, Map<String, String> allQueryParams, HttpServletRequest request, HttpServletResponse response) {
//      String locale = requestValidator.resolveLocale(request);
//        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(ErrorMessages.API_NOT_IMPLEMENTED_ON_PC,
//                ErrorCode.PCBR_API_NOT_ACCESSIBLE_ON_PC, HttpStatus.NOT_IMPLEMENTED, null);
//        ErrorResponse errorResponse =
//          PCBRResponseFactory.createStandardErrorResponse(
//              ErrorArgumentKey.GET_RESTORE_SOURCE_OPERATION,
//              exceptionDetails,
//              locale);
//      return ResponseEntity.status(HttpStatus.NOT_IMPLEMENTED)
//          .body(new MappingJacksonValue(errorResponse));
//  }
//
//}
