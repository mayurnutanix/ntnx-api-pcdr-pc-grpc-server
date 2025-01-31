package com.nutanix.prism.pcdr.restserver.services.impl;

import com.google.protobuf.ByteString;
import com.nutanix.prism.cluster.protobuf.ClusterExternalStateProto;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.exception.MantleException;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.util.CertFileUtil;
import com.nutanix.prism.pcdr.restserver.util.S3ServiceUtil;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.MantleUtils;
import com.nutanix.prism.pcdr.util.TaskHelperUtil;
import com.nutanix.prism.service.ErgonService;
import com.nutanix.util.base.UuidUtils;
import dp1.pri.prism.v4.protectpc.PcEndpointCredentials;
import dp1.pri.prism.v4.protectpc.PcEndpointFlavour;
import dp1.pri.prism.v4.protectpc.PcObjectStoreEndpoint;
import dp1.pri.prism.v4.protectpc.ReplicaInfo;
import lombok.Generated;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonInterface;
import nutanix.ergon.ErgonInterface.TaskCreateArg;
import nutanix.ergon.ErgonInterface.TaskListRet;
import nutanix.ergon.ErgonTypes.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.SerializationUtils;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static com.nutanix.prism.pcdr.constants.Constants.PCDR_TASK_COMPONENT_TYPE;

@Slf4j
@Service
public class PCTaskServiceHelper {

  @Autowired
  @Qualifier("pcdr.ergon")
  private ErgonService ergonService;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private MantleUtils mantleUtils;
  @Autowired
  private TaskHelperUtil taskHelperUtil;
  @Autowired
  private CertFileUtil certFileUtil;
  @Autowired
  private PCVMDataService pcvmDataService;

  /**
   * Creates the root task for PC restore using replica PE Ips.
   * @param replicaInfo - Replica PE Info.
   * @param authentication - authentication details from API Controller
   * @return - Returns the root task object.
   */
  public ByteString createPCRestoreRootTask(ReplicaInfo replicaInfo, Authentication authentication)
      throws ErgonException, PCResilienceException {
    // Store endpoint credentials in mantle service
    String credentialkeyId = writeObjectStoreCredentialsInMantle
        (replicaInfo.getObjectStoreEndpoint());
    ObjectStoreEndPointDto objectStoreEndPointDto = pcvmDataService.
                getObjectStoreEndpointDtoFromPcObjectStoreEndpoint(replicaInfo.getObjectStoreEndpoint());
    // store objects ssl certs on all pcvm
    storeNutanixObjectsCerts(objectStoreEndPointDto);
    // set endpoint credentials to null once stored in mantle
    // so that it is not stored in internal opaque
    if( !ObjectUtils.isEmpty(replicaInfo.getObjectStoreEndpoint()) && !ObjectUtils.isEmpty
                                         (replicaInfo.getObjectStoreEndpoint().getEndpointCredentials())){
       PcEndpointCredentials endpointCredentials = replicaInfo.getObjectStoreEndpoint().getEndpointCredentials();
       endpointCredentials.setAccessKey(null);
       endpointCredentials.setSecretAccessKey(null);
    }
    RestoreInternalOpaque restoreInternalOpaque = new RestoreInternalOpaque();
    restoreInternalOpaque.setPeIps(replicaInfo.getPeClusterIpList());
    restoreInternalOpaque.setPeUuid(replicaInfo.getPeClusterId());
    restoreInternalOpaque.setObjectStoreEndpoint(
            replicaInfo.getObjectStoreEndpoint());
    restoreInternalOpaque.setCredentialKeyId(credentialkeyId);
    restoreInternalOpaque.setBackupUuid(replicaInfo.getBackupUuid());
    if (!ObjectUtils.isEmpty(objectStoreEndPointDto)) {
      restoreInternalOpaque.setCertificatePath(objectStoreEndPointDto.getCertificatePath());
      restoreInternalOpaque.setPathStyle(objectStoreEndPointDto.isPathStyle());
    }
    ByteString restoreInternalOpaqueByteString =
      ByteString.copyFrom(Objects.requireNonNull
              (SerializationUtils.serialize(restoreInternalOpaque)));

    TaskCreateArg.Builder taskCreateArgBuilder =
      getTaskCreateArgBuilderWithOperationType(
          TaskConstants.TaskOperationType.kPCRestore, replicaInfo.getPeClusterId());
    // Enables entity_list backpropagation from child to parent if
    // disable_auto_entity_list_update is set False.
    taskCreateArgBuilder.setDisableAutoEntityListUpdate(false);

    // Adding user details in PCRestoreRootTask for UI to display which will be
    // auto propagated from Parent to all Child subtasks
    taskHelperUtil.addUserDetailsInTask(taskCreateArgBuilder, authentication);

    taskCreateArgBuilder.setInternalOpaque(restoreInternalOpaqueByteString);
    log.info("TaskArg : " + taskCreateArgBuilder.build());
    return ergonServiceHelper.createTask(taskCreateArgBuilder.build()).getUuid();
  }

  /**
   * Return keyId from Mantle service for secret credentials provided
   * @param objectStoreEndpoint Object store endpoint
   * @throws PCResilienceException can throw PC backup exception.
   */
  protected String writeObjectStoreCredentialsInMantle(PcObjectStoreEndpoint objectStoreEndpoint) throws PCResilienceException {
    if (!ObjectUtils.isEmpty(objectStoreEndpoint) && !ObjectUtils.isEmpty(objectStoreEndpoint.getEndpointCredentials())
        && !StringUtils.isEmpty(objectStoreEndpoint.getEndpointCredentials().getAccessKey()) &&
        !StringUtils.isEmpty(objectStoreEndpoint.getEndpointCredentials().getSecretAccessKey())) {
      String credentialkey;
      try {
        ClusterExternalStateProto.ObjectStoreCredentials objectStoreCredentialsProto =
            S3ServiceUtil.getCredentialsProtoFromEndpointCredentials(objectStoreEndpoint.getEndpointCredentials());
        // fetch the mantle key from mantle service using object credentials proto
        credentialkey = mantleUtils.writeSecret(objectStoreCredentialsProto);
        log.debug("Credential Key Id for recovery object store endpoint address is {}"
            , credentialkey);
      }
      catch (MantleException | PCResilienceException e) {
        log.error("Failed to store secrets in Mantle for recovery object store" +
                  " endpoint address  with exception ", e);
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
      return credentialkey;
    }
    return null;
  }

  private void storeNutanixObjectsCerts(ObjectStoreEndPointDto objectStoreEndPointDto) throws PCResilienceException {
    if (!ObjectUtils.isEmpty(objectStoreEndPointDto) && objectStoreEndPointDto.getEndpointFlavour().equalsIgnoreCase(
            PcEndpointFlavour.KOBJECTS.toString()) && !ObjectUtils.isEmpty(objectStoreEndPointDto.getCertificateContent())) {
       // store objects certs on all pcvm and fail even if anyone fails
        certFileUtil.createCertFileOnAllPCVM(Collections.singletonList(objectStoreEndPointDto), true);
    }
  }
  /**
   * Creates the subtask given parent task uuid.
   * @param taskOperationType - Enum for the operation type.
   * @param rootTaskUuid - Uuid of the parent task.
   * @param subTaskSeqId - sequence Id of the subtask.
   * @param pcClusterUuid - pcClusterUuid
   */
  public void createSubtask(TaskConstants.TaskOperationType taskOperationType,
                            ByteString rootTaskUuid,
                            int subTaskSeqId,
                            String pcClusterUuid,
                            String peClusterUuid) throws ErgonException {

    TaskCreateArg.Builder taskCreateArgBuilder =
      getTaskCreateArgBuilderWithOperationType(taskOperationType, peClusterUuid);
    taskCreateArgBuilder.setParentTaskUuid(rootTaskUuid);
    taskCreateArgBuilder.setSubtaskSequenceId(subTaskSeqId);

    // Adding entity details in PCRestore subtasks for UI to display which will be
    // back propagated to PCRestore Root task
    taskHelperUtil.addEntityDetailsInTask(taskCreateArgBuilder, pcClusterUuid);

    ergonServiceHelper.createTask(taskCreateArgBuilder.build());
  }

  /**
   * Return TaskCreateArg.Builder when we send a taskOperationType.
   * @param taskOperationType - TaskOperationType enum current value.
   * @return - returns the TaskCreateArg.Builder
   */
  public TaskCreateArg.Builder getTaskCreateArgBuilderWithOperationType(
    TaskConstants.TaskOperationType taskOperationType, String peClusterUuid) {
    TaskCreateArg.Builder taskCreateArgBuilder = TaskCreateArg.newBuilder();
    taskCreateArgBuilder.setComponent(PCDR_TASK_COMPONENT_TYPE);
    taskCreateArgBuilder.setDisplayName(
      taskOperationType.getTaskName());
    taskCreateArgBuilder.setTotalSteps(
      taskOperationType.getTotalTaskStep());
    taskCreateArgBuilder.setOperationType(
      taskOperationType.name());
    // Adding cluster in Task UI for PCRestore through PE
    if (!StringUtils.isEmpty(peClusterUuid)) {
      taskCreateArgBuilder.setDisableAutoClusterListUpdate(true);
      taskCreateArgBuilder.addClusterList(UuidUtils.getByteStringFromUUID(peClusterUuid));
    }
    return taskCreateArgBuilder;
  }

  /**
   * This method fetches the root task for PC restore in Ergon. Checks
   * whether if there is any restore task in progress and if not returns.
   * Incase multiple restore task are also there it raises an exception.
   * @return - Root PC task in ergon.
   * @throws PCResilienceException - Throws exception when more than one
   * restore tasks are running.
   */
  public Task fetchPCRestoreRootTask() throws ErgonException, PCResilienceException {
    TaskListRet taskListRet = ergonServiceHelper
      .fetchTaskListRet(
        TaskConstants.TaskOperationType.kPCRestore.name(), false, PCDR_TASK_COMPONENT_TYPE);
    if (taskListRet.getTaskUuidListCount() == 0) {
      log.info("No PC restore task found in Ergon.");
      return null;
    } else if (taskListRet.getTaskUuidListCount() > 1) {
      throw new PCResilienceException(ErrorMessages.MULTIPLE_RECOVERY_TASK_IN_PROGRESS_ERROR,
              ErrorCode.PCBR_MULTIPLE_RECOVERY_TASK_IN_PROGRESS, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    // Root task uuid will always be at 0th index.
    ByteString rootTaskUuid = taskListRet.getTaskUuidList(0);
    // There will only be one task in taskGetRet fetched based on one uuid.
    // Ignoring the case where task could have been deleted manually. In that
    // case PCDataRecovery Exception will be thrown.
    return ergonServiceHelper.fetchTask(rootTaskUuid, true);
  }

  /**
   * Update queued task to running task. Based on includeSubtaskUuids it will
   * return a task containing subtask uuids or not.
   * @param task - Ergon Task
   * @param includeSubtaskUuids - boolean to tell whether to include
   *                            subtaskuuid.
   * @return - returns the updated task.
   * @throws PCResilienceException - Can throw PCResilienceException
   * while updating task.
   */
  public Task updateQueuedTaskStatusToRunning(Task task,
    boolean includeSubtaskUuids) throws ErgonException, PCResilienceException {
    if (task.getStatus() == Task.Status.kQueued) {
      ergonServiceHelper.updateTaskStatus(task, Task.Status.kRunning);
      task = ergonServiceHelper.fetchTask(task.getUuid(), includeSubtaskUuids);
    }
    return task;
  }

  /**
   * Update the queued task status to aborted task.
   * @param taskUuid - task uuid of the task to update.
   * @throws PCResilienceException - Can throw PCResilienceException
   * while updating task.
   */
  public void updateQueuedTaskStatusToAborted(ByteString taskUuid)
      throws ErgonException, PCResilienceException {
    Task task = ergonServiceHelper.fetchTask(taskUuid, false);
    if (task.getStatus() == Task.Status.kQueued) {
      ergonServiceHelper.updateTaskStatus(task, Task.Status.kAborted);
    }
  }

  /**
   * Updates ergon task status and progress percent.
   *
   * @param taskId task id
   * @param status status of task to be updated
   * @param percentComplete progress percent.
   */
  public void updateTaskStatus(ByteString taskId, Task.Status status, int percentComplete)
          throws ErgonException {
    if (taskId != null) {
      ErgonInterface.TaskUpdateArg.Builder taskUpdateArgBuilder =
              ErgonInterface.TaskUpdateArg.newBuilder().setUuid(taskId);
      taskUpdateArgBuilder.setStatus(status);
      taskUpdateArgBuilder.setPercentageComplete(percentComplete);
      taskUpdateArgBuilder.setLogicalTimestamp(taskUpdateArgBuilder.getLogicalTimestamp() + 1);
      ergonServiceHelper.updateTask(taskUpdateArgBuilder.build());
    }
  }

  /**
   * Run process builder cluster start command on bash.
   * @return - returns the process
   * @throws IOException - can raise IOException
   */
  @Generated
  public Process runBashClusterStart() throws IOException {
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.redirectOutput(new File("/dev/null")).redirectErrorStream(true);
    processBuilder.command("bash", "-c", "cluster start");
    log.info("Invoking 'cluster start' command to restart services on Prism " +
             "Central");
    return processBuilder.start();
  }

  /**
   * Run process builder genesis stop and cluster start command.
   * @return - returns the process
   * @throws IOException - can raise IOException
   */
  @Generated
  public Process stopServicesUsingGenesis(List<String> services) throws IOException {
    String servicesString = String.join(" ", services);
    String command = String.format("genesis stop %s", servicesString);
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.redirectOutput(new File("/dev/null")).redirectErrorStream(true);
    processBuilder.command("bash", "-c", command);
    log.info("Invoking 'genesis stop' command to stop following services on " +
             "Prism Central: {}", servicesString);
    return processBuilder.start();
  }

  public void updateTask(ByteString taskId, int percentCompleted) throws ErgonException {
    if (!ObjectUtils.isEmpty(taskId)){
      ergonServiceHelper.updateTask(taskId, percentCompleted);
    }
  }

  public void updateTask(ByteString taskId, int percentCompleted, Task.Status status) throws ErgonException {
    if (!ObjectUtils.isEmpty(taskId)){
      ergonServiceHelper.updateTask(taskId, percentCompleted, status);
    }
  }
}

