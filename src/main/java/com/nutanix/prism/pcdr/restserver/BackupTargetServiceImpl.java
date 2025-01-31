package com.nutanix.prism.pcdr.restserver;

import com.google.protobuf.ByteString;
import com.nutanix.api.utils.task.ErgonTaskUtils;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorArgumentKey;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.converters.PrismCentralBackupConverter;
import com.nutanix.prism.pcdr.restserver.factory.BackupTargetHelper;
import com.nutanix.prism.pcdr.restserver.handlers.AsyncMessageHandler;
import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
import com.nutanix.prism.pcdr.restserver.util.*;
import com.nutanix.prism.pcdr.util.DataMaskUtil;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.pcdr.util.UserInformationUtil;
import com.nutanix.util.base.UuidUtils;
import dp1.pri.prism.v4.config.TaskReference;
import dp1.pri.prism.v4.error.ErrorResponse;
import dp1.pri.prism.v4.management.BackupTarget;
import dp1.pri.prism.v4.management.ClusterLocation;
import dp1.pri.prism.v4.management.CreateBackupTargetApiResponse;
import dp1.pri.prism.v4.management.GetBackupTargetApiResponse;
import dp1.pri.prism.v4.management.ListBackupTargetsApiResponse;
import dp1.pri.prism.v4.management.ObjectStoreLocation;
import dp1.pri.prism.v4.protectpc.BackupTargets;
import dp1.pri.prism.v4.protectpc.BackupTargetsInfo;
import dp1.pri.prism.v4.protectpc.PcEndpointCredentials;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.json.MappingJacksonValue;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.ObjectUtils;
import proto3.dp1.pri.prism.v4.management.mappers.*;
import proto3.prism.v4.management.*;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.*;

import static com.nutanix.prism.constants.IdempotencySupportConstants.NTNX_REQUEST_ID;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.DELETE_BACKUP_TARGET;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.DELETE_BACKUP_TARGET_OPERATION_TYPE;

@Slf4j
@GrpcService
public class BackupTargetServiceImpl extends DomainManagerBackupsServiceGrpc.DomainManagerBackupsServiceImplBase {

  private final InstanceServiceFactory instanceServiceFactory;

  private final RequestValidator requestValidator;

  private final IdempotencyUtil idempotencyUtil;

  private final BackupTargetHelper backupTargetHelper;

  private final PrismCentralBackupConverter prismCentralBackupConverter;

  private final PCBackupService pcBackupService;

  private final ErgonServiceHelper ergonServiceHelper;

  private final AsyncMessageHandler asyncMessageHandler;

  private final BackupTargetMapper backupTargetMapper;

  private final CreateBackupTargetApiResponseMapper createBackupTargetApiResponseMapper;

  private final PCBRResponseFactory responseFactory;

  private final BackupTargetUtil backupTargetUtil;

  private final DeleteBackupTargetApiResponseMapper deleteBackupTargetApiResponseMapper;

  private final GetBackupTargetApiResponseMapper getBackupTargetApiResponseMapper;

  private final ListBackupTargetsApiResponseMapper listBackupTargetsApiResponseMapper;

  private final UpdateBackupTargetApiResponseMapper updateBackupTargetApiResponseMapper;

  private final ObjectStoreValidator objectStoreValidator;


  public BackupTargetServiceImpl(InstanceServiceFactory instanceServiceFactory, RequestValidator requestValidator, IdempotencyUtil idempotencyUtil, BackupTargetHelper backupTargetHelper, PrismCentralBackupConverter prismCentralBackupConverter, PCBackupService pcBackupService, ErgonServiceHelper ergonServiceHelper, AsyncMessageHandler asyncMessageHandler, BackupTargetMapper backupTargetMapper, CreateBackupTargetApiResponseMapper createBackupTargetApiResponseMapper, PCBRResponseFactory responseFactory, BackupTargetUtil backupTargetUtil, DeleteBackupTargetApiResponseMapper deleteBackupTargetApiResponseMapper, GetBackupTargetApiResponseMapper getBackupTargetApiResponseMapper, ListBackupTargetsApiResponseMapper listBackupTargetsApiResponseMapper, UpdateBackupTargetApiResponseMapper updateBackupTargetApiResponseMapper, ObjectStoreValidator objectStoreValidator) {
    this.instanceServiceFactory = instanceServiceFactory;
    this.requestValidator = requestValidator;
    this.idempotencyUtil = idempotencyUtil;
    this.backupTargetHelper = backupTargetHelper;
    this.prismCentralBackupConverter = prismCentralBackupConverter;
    this.pcBackupService = pcBackupService;
    this.ergonServiceHelper = ergonServiceHelper;
    this.asyncMessageHandler = asyncMessageHandler;
    this.backupTargetMapper = backupTargetMapper;
    this.createBackupTargetApiResponseMapper = createBackupTargetApiResponseMapper;
    this.responseFactory = responseFactory;
    this.backupTargetUtil = backupTargetUtil;
    this.deleteBackupTargetApiResponseMapper = deleteBackupTargetApiResponseMapper;
    this.getBackupTargetApiResponseMapper = getBackupTargetApiResponseMapper;
    this.listBackupTargetsApiResponseMapper = listBackupTargetsApiResponseMapper;
    this.updateBackupTargetApiResponseMapper = updateBackupTargetApiResponseMapper;
    this.objectStoreValidator = objectStoreValidator;
  }

  @Override
  public void createBackupTarget(proto3.prism.v4.management.CreateBackupTargetArg request,
                                 io.grpc.stub.StreamObserver<proto3.prism.v4.management.CreateBackupTargetRet> responseObserver) {

    dp1.pri.prism.v4.management.CreateBackupTargetApiResponse createBackupTargetResponse = new CreateBackupTargetApiResponse();
    dp1.pri.prism.v4.config.TaskReference taskReference = new dp1.pri.prism.v4.config.TaskReference();
    CreateBackupTargetRet createBackupTargetRet;
    ByteString taskId;
    String peClusterUuid = StringUtils.EMPTY;
    String pcClusterUuid =
            instanceServiceFactory.getClusterUuidFromZeusConfig();
    BackupTarget backupTarget = backupTargetMapper.protoToModel(request.getBody());
    Set<String> peClusterUuidSet = new HashSet<>();
    List<PcObjectStoreEndpoint> pcObjectStoreEndpointList = new ArrayList<>();
    ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
    HttpServletRequest request1 = attributes.getRequest();
    final String requestId = request1.getHeader(NTNX_REQUEST_ID);
    //String resolvedLocale = requestValidator.resolveLocale(request);
    String resolvedLocale = "en-US";
    String backupTargetStr = DataMaskUtil.maskSecrets(backupTarget.toString());
    try {
      Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      String userId = UserInformationUtil.getUserUUID(authentication);
      String idempotentTaskId = idempotencyUtil.getIdempotentTaskInfo(requestId, userId);
      // check if nutanix request id is already referenced and return the existing task.
      if (!StringUtils.isEmpty(idempotentTaskId)) {
        taskReference = new TaskReference(ErgonTaskUtils.getTaskReferenceExtId(idempotentTaskId));
        createBackupTargetResponse.setDataInWrapper(taskReference);
        createBackupTargetRet = CreateBackupTargetRet.newBuilder().setContent(createBackupTargetApiResponseMapper.modelToProto(createBackupTargetResponse)).build();
        responseObserver.onNext(createBackupTargetRet);
      }
      // Verify that domainManagerExtId == current PC clusterId
      requestValidator.validatePcClusterUuid(request.getDomainManagerExtId(), pcClusterUuid);
      // Validate the inputs for cluster and ObjectStore
      BackupTargetValidator backupTargetValidator = backupTargetHelper.getBackupTargetValidator(backupTarget);
      backupTargetValidator.validateBackupTarget(backupTarget);
      BackupTargets backupTargets = prismCentralBackupConverter.getBackupTargetsfromBackupTarget(backupTarget);
      if (!ObjectUtils.isEmpty(backupTargets.getClusterUuidList())) {
        peClusterUuid = backupTargets.getClusterUuidList().get(0);
        peClusterUuidSet = new HashSet<>(backupTargets.getClusterUuidList());
      }
      if (!ObjectUtils.isEmpty(backupTargets.getObjectStoreEndpointList())) {
        PcObjectStoreEndpoint pcObjectStoreEndpoint = backupTargets.getObjectStoreEndpointList().get(0);
        pcBackupService.generateEndpointAddress(pcObjectStoreEndpoint, pcClusterUuid);
        // In case of NC2, if credentials are not provided,
        // setting it with an empty object
        if (pcObjectStoreEndpoint.getEndpointCredentials() == null) {
          pcObjectStoreEndpoint.setEndpointCredentials(new PcEndpointCredentials());
        }
        pcObjectStoreEndpointList.add(pcObjectStoreEndpoint);
      }

      // Check if the backup target is eligible for Backup
      pcBackupService.isBackupTargetEligibleForBackup(peClusterUuidSet,
              pcObjectStoreEndpointList);

      log.info("Create Backup Target Body {}", backupTargetStr);
      taskId = ergonServiceHelper.createTask(Constants.PCDR_TASK_COMPONENT_TYPE, "Create Backup Target",
              "kCreateBackupTarget",
              authentication, pcClusterUuid, peClusterUuid);

      taskReference.setExtId(ErgonTaskUtils.getTaskReferenceExtId(UuidUtils.getUUID(taskId)));
      asyncMessageHandler.createBackupTargetAsync(backupTargets, taskId);
      // update request reference table with task id and request id.
      idempotencyUtil.updateTaskReferenceTable(UuidUtils.getUUID(taskId), requestId, userId);
    }
    catch (Exception e) {
      log.error("Failed to create Backup Target with exception: ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
              exceptionDetails.getArguments(), e.getMessage());
      ErrorResponse errorResponse = this.responseFactory.createStandardErrorResponse(
              ErrorArgumentKey.CREATE_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
      createBackupTargetResponse.setDataInWrapper(errorResponse);
      createBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(true));

    }
    createBackupTargetResponse.setDataInWrapper(taskReference);
    createBackupTargetResponse.setMetadata(
            ResponseUtil.createMetadataFor(Collections.singletonList(ApiLinkUtil.getApiLinkForTaskReference(taskReference)),
                    false, false));
    createBackupTargetRet = CreateBackupTargetRet.newBuilder().setContent(createBackupTargetApiResponseMapper.modelToProto(createBackupTargetResponse)).build();
    responseObserver.onNext(createBackupTargetRet);
  }

  public void deleteBackupTargetById(DeleteBackupTargetByIdArg request, StreamObserver<DeleteBackupTargetByIdRet> responseObserver) {

    String extId = request.getExtId();
    log.info("Request received to remove Backup target : {}", extId);
    DeleteBackupTargetByIdRet deleteBackupTargetByIdRet;
    TaskReference taskReference = new TaskReference();
    dp1.pri.prism.v4.management.DeleteBackupTargetApiResponse deleteBackupTargetResponse = new dp1.pri.prism.v4.management.DeleteBackupTargetApiResponse();
    ByteString taskId;
    String peClusterUuid = StringUtils.EMPTY;
    ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
    HttpServletRequest request1 = attributes.getRequest();
    final String requestId = request1.getHeader(NTNX_REQUEST_ID);
    //String resolvedLocale = requestValidator.resolveLocale(request);
    String resolvedLocale = "en-US";
    String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    try {
      Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      String userId = UserInformationUtil.getUserUUID(authentication);
      // check if ntnx request id is already referenced and return the existing task.
      String idempotentTaskId = idempotencyUtil.getIdempotentTaskInfo(requestId, userId);
      if (!StringUtils.isEmpty(idempotentTaskId)) {
        taskReference = new TaskReference(ErgonTaskUtils.getTaskReferenceExtId(idempotentTaskId));
        deleteBackupTargetResponse.setDataInWrapper(taskReference);
        deleteBackupTargetByIdRet = DeleteBackupTargetByIdRet.newBuilder().setContent(
                deleteBackupTargetApiResponseMapper.modelToProto(deleteBackupTargetResponse)).build();
        responseObserver.onNext(deleteBackupTargetByIdRet);
      }
      // Verify that domainManagerExtId == current PC clusterId
      requestValidator.validatePcClusterUuid(request.getDomainManagerExtId(), pcClusterUuid);
      BackupTarget backupTarget = backupTargetUtil.getBackupTarget(extId);
      if (backupTarget.getLocation() instanceof ClusterLocation) {
        peClusterUuid = backupTarget.getExtId();
      }
      String eTag = request1.getHeader(HttpHeaders.IF_MATCH);
      if (!StringUtils.isEmpty(eTag)) {
        // remove double quotes
        eTag = eTag.replace("\"", "");
      }
      requestValidator.checkIfEtagMatch(eTag, backupTarget);
      taskId = ergonServiceHelper.createTask(Constants.PCDR_TASK_COMPONENT_TYPE,
              DELETE_BACKUP_TARGET,
              DELETE_BACKUP_TARGET_OPERATION_TYPE,
              authentication, pcClusterUuid, peClusterUuid);
      taskReference.setExtId(ErgonTaskUtils.getTaskReferenceExtId(UuidUtils.getUUID(taskId)));
      deleteBackupTargetResponse.setDataInWrapper(taskReference);
      deleteBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(
              Collections.singletonList(ApiLinkUtil.getApiLinkForTaskReference(taskReference)), false, false)
      );
      asyncMessageHandler.deleteBackupTargetAsync(taskId, eTag, extId);
      // update request reference table with task id and request id.
      idempotencyUtil.updateTaskReferenceTable(UuidUtils.getUUID(taskId), requestId, userId);
      deleteBackupTargetByIdRet = DeleteBackupTargetByIdRet.newBuilder().setContent(
              deleteBackupTargetApiResponseMapper.modelToProto(deleteBackupTargetResponse)).build();
      responseObserver.onNext(deleteBackupTargetByIdRet);
    }
    catch (Exception e) {
      log.error("Exception while deleting backup target {} ", extId, e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
              exceptionDetails.getArguments(), e.getMessage());
      ErrorResponse errorResponse = this.responseFactory.createStandardErrorResponse(
              ErrorArgumentKey.DELETE_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
      deleteBackupTargetResponse.setDataInWrapper(errorResponse);
      deleteBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(true));
      deleteBackupTargetByIdRet = DeleteBackupTargetByIdRet.newBuilder().setContent(
              deleteBackupTargetApiResponseMapper.modelToProto(deleteBackupTargetResponse)).build();
      responseObserver.onNext(deleteBackupTargetByIdRet);
    }
  }

  public void getBackupTargetById(GetBackupTargetByIdArg request, StreamObserver<GetBackupTargetByIdRet> responseObserver) {

    GetBackupTargetByIdRet getBackupTargetByIdRet;
    String extId = request.getExtId();
    log.info("Request received to get Backup Target : {}", extId);
    dp1.pri.prism.v4.management.GetBackupTargetApiResponse backupTargetApiResponse = new GetBackupTargetApiResponse();
    //String resolvedLocale = requestValidator.resolveLocale(request);
    String resolvedLocale = "en-US";
    String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    try {
      // Verify that domainManagerExtId == current PC clusterId
      requestValidator.validatePcClusterUuid(request.getDomainManagerExtId(), pcClusterUuid);
      BackupTarget backupTarget = backupTargetUtil.getBackupTarget(extId);
      HttpHeaders headers = new HttpHeaders();
      headers.set(HttpHeaders.ETAG, pcBackupService.generateEtag(backupTarget));
      backupTargetApiResponse.setDataInWrapper(backupTarget);
      backupTargetApiResponse.setMetadata(
              ResponseUtil.createMetadataFor(Collections.singletonList(ApiLinkUtil.getSelfLink()), false, false));
      getBackupTargetByIdRet = GetBackupTargetByIdRet.newBuilder().setContent(
              getBackupTargetApiResponseMapper.modelToProto(backupTargetApiResponse)).build();
      responseObserver.onNext(getBackupTargetByIdRet);
    }
    catch (Exception e) {
      log.error("Exception while retrieving backup target {} ", extId, e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
              exceptionDetails.getArguments(), e.getMessage());
      ErrorResponse errorResponse = this.responseFactory.createStandardErrorResponse(
              ErrorArgumentKey.GET_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
      backupTargetApiResponse.setDataInWrapper(errorResponse);
      backupTargetApiResponse.setMetadata(ResponseUtil.createMetadataFor(true));
      getBackupTargetByIdRet = GetBackupTargetByIdRet.newBuilder().setContent(
              getBackupTargetApiResponseMapper.modelToProto(backupTargetApiResponse)).build();
      responseObserver.onNext(getBackupTargetByIdRet);
    }
  }

  public void listBackupTargets(ListBackupTargetsArg request, StreamObserver<ListBackupTargetsRet> responseObserver) {

    ListBackupTargetsRet listBackupTargetsRet;
    dp1.pri.prism.v4.management.ListBackupTargetsApiResponse listBackupTargetsApiResponse = new ListBackupTargetsApiResponse();
    //String resolvedLocale = requestValidator.resolveLocale(request);
    String resolvedLocale = "en-US";
    String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    //Creating a response
    try {
      // Verify that domainManagerExtId == current PC clusterId
      requestValidator.validatePcClusterUuid(request.getDomainManagerExtId(), pcClusterUuid);
      // Receives the ListBackupTargetResponse from pcBackupServiceAdapter and then
      // setting data in response.
      BackupTargetsInfo backupTargetsInfo = pcBackupService.getReplicas();
      listBackupTargetsApiResponse.setDataInWrapper(
              prismCentralBackupConverter.getListBackupTargetFromBackupTargetsInfo(backupTargetsInfo));
      listBackupTargetsApiResponse.setMetadata(
              ResponseUtil.createMetadataFor(Collections.singletonList(ApiLinkUtil.getSelfLink()), false, false));
      listBackupTargetsRet = ListBackupTargetsRet.newBuilder().setContent(
              listBackupTargetsApiResponseMapper.modelToProto(listBackupTargetsApiResponse)).build();
      responseObserver.onNext(listBackupTargetsRet);
    }
    catch (Exception e) {
      log.error("The following error encountered while listBackupTargets: ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
              exceptionDetails.getArguments(), e.getMessage());
      ErrorResponse errorResponse = this.responseFactory.createStandardErrorResponse(
              ErrorArgumentKey.LIST_BACKUP_TARGETS_OPERATION, exceptionDetails, resolvedLocale);
      listBackupTargetsApiResponse.setDataInWrapper(errorResponse);
      listBackupTargetsApiResponse.setMetadata(ResponseUtil.createMetadataFor(true));
      listBackupTargetsRet = ListBackupTargetsRet.newBuilder().setContent(
              listBackupTargetsApiResponseMapper.modelToProto(listBackupTargetsApiResponse)).build();
      responseObserver.onNext(listBackupTargetsRet);
    }
  }

  public void updateBackupTargetById(UpdateBackupTargetByIdArg request, StreamObserver<UpdateBackupTargetByIdRet> responseObserver) {

    UpdateBackupTargetByIdRet updateBackupTargetByIdRet;
    BackupTarget backupTarget = backupTargetMapper.protoToModel(request.getBody());
    String extId = request.getExtId();
    log.info("Request received to Update Backup Target for {}", extId);
    TaskReference taskReference = new TaskReference();
    ByteString taskId;
    String backupTargetStr = DataMaskUtil.maskSecrets(backupTarget.toString());
    String pcClusterUuid = instanceServiceFactory.getClusterUuidFromZeusConfig();
    BackupTargets backupTargets = prismCentralBackupConverter.getBackupTargetsfromBackupTarget(backupTarget);

    dp1.pri.prism.v4.management.UpdateBackupTargetApiResponse updateBackupTargetResponse = new dp1.pri.prism.v4.management.UpdateBackupTargetApiResponse();
    ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
    HttpServletRequest request1 = attributes.getRequest();
    final String requestId = request1.getHeader(NTNX_REQUEST_ID);
    //String resolvedLocale = requestValidator.resolveLocale(request);
    String resolvedLocale = "en-US";
    try {
      Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
      String userId = UserInformationUtil.getUserUUID(authentication);
      String idempotentTaskId = idempotencyUtil.getIdempotentTaskInfo(requestId, userId);
      // check if nutanix request id is already referenced and return the existing task.
      if (!StringUtils.isEmpty(idempotentTaskId)) {
        taskReference = new TaskReference(ErgonTaskUtils.getTaskReferenceExtId(idempotentTaskId));
        updateBackupTargetResponse.setDataInWrapper(taskReference);
        updateBackupTargetByIdRet = UpdateBackupTargetByIdRet.newBuilder().setContent(
                updateBackupTargetApiResponseMapper.modelToProto(updateBackupTargetResponse)).build();
        responseObserver.onNext(updateBackupTargetByIdRet);
      }
      // Verify that domainManagerExtId == current PC clusterId
      requestValidator.validatePcClusterUuid(request.getDomainManagerExtId(), pcClusterUuid);
      if (!(backupTarget.getLocation() instanceof dp1.pri.prism.v4.management.ObjectStoreLocation)) {
        //Todo : Check message thrown is correct ?
        ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(
                ErrorMessages.getInvalidInputProvidedMessage(ErrorArgumentKey.OBJECT_STORE),
                ErrorCode.PCBR_BACKUP_TARGET_CLUSTER_PROVIDED_OBJECT_STORE_NEEDED,
                HttpStatus.BAD_REQUEST, null);
        ErrorResponse errorResponse = this.responseFactory.createStandardErrorResponse(
                ErrorArgumentKey.UPDATE_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
        updateBackupTargetResponse.setDataInWrapper(errorResponse);
        updateBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(true));
        updateBackupTargetByIdRet = UpdateBackupTargetByIdRet.newBuilder().setContent(
                updateBackupTargetApiResponseMapper.modelToProto(updateBackupTargetResponse)).build();
        responseObserver.onNext(updateBackupTargetByIdRet);
      }
      else {
        objectStoreValidator.validateObjectStoreForUpdate((ObjectStoreLocation) backupTarget.getLocation(), extId);
      }
      BackupTarget backupTarget1 = backupTargetUtil.getBackupTarget(extId);
      String eTag = request1.getHeader(HttpHeaders.IF_MATCH);
      if (!StringUtils.isEmpty(eTag)) {
        // remove double quotes
        eTag = eTag.replace("\"", "");
      }
      requestValidator.checkIfEtagMatch(eTag, backupTarget1);
      log.info("Update Backup Target Body {}", backupTargetStr);
      taskId = ergonServiceHelper.createTask(Constants.PCDR_TASK_COMPONENT_TYPE, "Update Backup Target",
              "kUpdateBackupTarget",
              authentication, pcClusterUuid, StringUtils.EMPTY);
      taskReference.setExtId(ErgonTaskUtils.getTaskReferenceExtId(UuidUtils.getUUID(taskId)));
      asyncMessageHandler.updateBackupTargetAsync(backupTargets, taskId, eTag, extId);
      // update request reference table with task id and request id.
      idempotencyUtil.updateTaskReferenceTable(UuidUtils.getUUID(taskId), requestId, userId);
    }
    catch (Exception e) {
      // This is to catch any other exception that we have missed to handle
      log.error("Failed to Update Backup Target with exception: ", e);
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      ErrorCodeArgumentMapper.loggingAndValidateErrorCodeArguments(exceptionDetails.getErrorCode(),
              exceptionDetails.getArguments(), e.getMessage());
      ErrorResponse errorResponse = this.responseFactory.createStandardErrorResponse(
              ErrorArgumentKey.UPDATE_BACKUP_TARGET_OPERATION, exceptionDetails, resolvedLocale);
      updateBackupTargetResponse.setDataInWrapper(errorResponse);
      updateBackupTargetResponse.setMetadata(ResponseUtil.createMetadataFor(true));
      updateBackupTargetByIdRet = UpdateBackupTargetByIdRet.newBuilder().setContent(
              updateBackupTargetApiResponseMapper.modelToProto(updateBackupTargetResponse)).build();
      responseObserver.onNext(updateBackupTargetByIdRet);
    }
    updateBackupTargetResponse.setDataInWrapper(taskReference);
    updateBackupTargetResponse.setMetadata(
            ResponseUtil.createMetadataFor(Collections.singletonList(ApiLinkUtil.getApiLinkForTaskReference(taskReference)),
                    false, false));
    updateBackupTargetByIdRet = UpdateBackupTargetByIdRet.newBuilder().setContent(
            updateBackupTargetApiResponseMapper.modelToProto(updateBackupTargetResponse)).build();
    responseObserver.onNext(updateBackupTargetByIdRet);
  }
}
