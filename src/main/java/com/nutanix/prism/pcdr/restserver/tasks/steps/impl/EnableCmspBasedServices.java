package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.nutanix.api.utils.json.JsonUtils;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.dto.ExceptionDetailsDTO;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.dto.CmspBaseServices;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.ExceptionUtil;
import com.nutanix.prism.service.ErgonService;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeoutException;

import static com.nutanix.prism.pcdr.constants.Constants.CMSP_CONTROLLER_CLUSTER_TYPE;

@Service
@Slf4j
public class EnableCmspBasedServices implements PcdrStepsHandler {

  @Autowired
  private PCDRYamlAdapterImpl pcdrYamlAdapter;
  @Value("${prism.pcdr.msp.port:2082}")
  private int mspPort;
  @Value("${prism.pcdr.msp.host:localhost}")
  private String mspHost;
  @Value("${prism.pcdr.msp.protocol:http}")
  private String mspProtocol;
  @Value("${prism.pcdr.msp.enable.services.task.timeoutSec:600}")
  private long enableServicesTaskPollTimeoutSec;
  @Autowired
  @Qualifier("pcdr.ergon")
  private ErgonService ergonService;
  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  private RestTemplate restTemplate;

  public EnableCmspBasedServices() {
    this.restTemplate = new RestTemplate();
  }

  public void init(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
      throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    // STEP: Enabling and starting all the cmsp based services like IAMv2 etc.
    log.info("STEP {}: Enabling CMSP based services.", currentStep);
    try {
      if (pcdrYamlAdapter.getServicesToDisableOnStartupCMSP() == null ||
          pcdrYamlAdapter.getServicesToDisableOnStartupCMSP().isEmpty()) {
        log.info("There are no disabled services on CMSP, skipping this step.");
      } else {
        String taskUuid = "";
        // Assuming that the internalOpaque will be empty for this subtask.
        // Make sure that internalOpaque is not getting initialised before this
        // so that length of the bytearray is empty.
        if (currentSubtask.getInternalOpaque().toByteArray().length != 0) {
          taskUuid = new String(
              currentSubtask.getInternalOpaque().toByteArray());
        }

        if (taskUuid.isEmpty()) {
          taskUuid = enableCMSPBasedServices(getMSPClusterId());
          currentSubtask = ergonServiceHelper.updateTask(
              currentSubtask,
              ByteString.copyFrom(taskUuid.getBytes()));
        }
        if (!pollCMSPServicesEnableTask(taskUuid)) {
          // If the above function returns false that means the task is still
          // in either running state or in queued state.
          return null;
        }
      }
    } catch (Exception e) {
      ExceptionDetailsDTO exceptionDetails = ExceptionUtil.getExceptionDetails(e);
      log.error("Error encountered while enabling CMSP based services.", e);
      throw new PCResilienceException(exceptionDetails.getMessage(), exceptionDetails.getErrorCode(),
              exceptionDetails.getHttpStatus(), exceptionDetails.getArguments());
    }
    log.info("STEP {}: Successfully enabled CMSP based services.", currentStep);
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep, ByteString.EMPTY,
        TaskConstants.ENABLE_CMSP_BASED_SERVICES);
    return currentSubtask;
  }

  /**
   * Polls for the CMSP task until it is completed.
   * @param taskUuid - task uuid of the enable cmsp task
   */
  boolean pollCMSPServicesEnableTask(String taskUuid)
      throws JsonProcessingException, TimeoutException, PCResilienceException {
    log.info("Polling on the cmsp enable services task {}", taskUuid);
    String uri = String.format("%s://%s:%d/msp/tasks/{taskUuid}", mspProtocol,
                               mspHost, mspPort);
    Map<String, String> params = new HashMap<>();
    params.put("taskUuid", taskUuid);
    String mspTask = restTemplate.getForObject(uri, String.class, params);
    ObjectMapper objectMapper = JsonUtils.getObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(mspTask);
    String currentState = jsonNode.get("state").asText();
    List<String> failedTaskList = Arrays.asList(
        TaskConstants.CMSPTaskState.FAILED.name(),
        TaskConstants.CMSPTaskState.ABORTED.name(),
        TaskConstants.CMSPTaskState.CANCELED.name(),
        TaskConstants.CMSPTaskState.ROLLING_BACK.name());
    if (currentState.equals(TaskConstants.CMSPTaskState.SUCCEEDED.name())) {
      return true;
    } else if (failedTaskList.contains(currentState)) {
      log.error("Current state of the task is {}, cannot proceed further with" +
                " this task.", currentState);
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
    DateTimeFormatter formatter = DateTimeFormatter
        .ofPattern("yyyy-MM-dd'T'HH:mm:ss'+'SSSS")
        .withLocale(Locale.ROOT).withZone(ZoneOffset.UTC);
    LocalDateTime createdTime = LocalDateTime.parse(jsonNode.get(
        "created_timestamp").asText(), formatter);
    Duration duration = Duration.between(createdTime, LocalDateTime.now());
    if (duration.getSeconds() > enableServicesTaskPollTimeoutSec) {
      throw new TimeoutException(
          String.format("Unable to complete enable CMSP services " +
                        "task in specified time %d ms",
                        enableServicesTaskPollTimeoutSec)
      );
    }
    return false;
  }

  /**
   * Makes a GET API call to msp/v2/clusters to receive the msp cluster uuid.
   * @return - returns MSP cluster uuid
   * @throws JsonProcessingException - can throw JsonProcessing exception
   * @throws PCResilienceException - can throw PCResilience exception if cluster_id
   * is not found.
   */
  String getMSPClusterId()
      throws JsonProcessingException, PCResilienceException {
    String uri = String.format("%s://%s:%d/msp/v2/clusters", mspProtocol,
                               mspHost, mspPort);
    String mspClusterList = restTemplate.getForObject(uri, String.class);
    log.debug("MSP cluster list : {}", mspClusterList);
    ObjectMapper objectMapper = JsonUtils.getObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(mspClusterList);
    JsonNode clusterListNode = jsonNode.get("cluster_list");
    if (clusterListNode != null && clusterListNode.isArray()) {
      for (JsonNode clusterNode: clusterListNode) {
        if (clusterNode.get("cluster_type").asText().equals(
            CMSP_CONTROLLER_CLUSTER_TYPE)) {
          return clusterNode.get("cluster_uuid").asText();
        }
      }
    }
    log.error("Unable to find CMSP controller cluster uuid.");
    throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
  }

  /**
   * Enables all CMSP based disabled services
   * @param cmspClusterId - cmsp cluster uuid
   * @return - returns a task_uuid for the same
   * @throws JsonProcessingException - can throw JsonProcessing exception
   * @throws PCResilienceException - If the task_uuid is not found throws
   * PCResilienceException.
   */
  String enableCMSPBasedServices(String cmspClusterId)
      throws JsonProcessingException, PCResilienceException {
    String uri = String.format("%s://%s:%d/msp/v2/clusters/{cluster_id}/" +
                               "update-base-service", mspProtocol, mspHost,
                               mspPort);
    Map<String, String> params = new HashMap<>();
    params.put("cluster_id", cmspClusterId);
    CmspBaseServices cmspBaseServices = new CmspBaseServices();
    cmspBaseServices.setEnabledServices(
        pcdrYamlAdapter.getServicesToDisableOnStartupCMSP());
    cmspBaseServices.setEnable(true);
    HttpEntity<String> response = restTemplate.exchange(
        uri, HttpMethod.PUT, new HttpEntity<>(cmspBaseServices),
        String.class, params);
    log.debug("MSP enabled service output: {}", response.getBody());
    ObjectMapper objectMapper = JsonUtils.getObjectMapper();
    JsonNode jsonNode = objectMapper.readTree(response.getBody());
    if (jsonNode.get("task_uuid") != null) {
      return jsonNode.get("task_uuid").asText();
    } else {
      log.error(String.format("Unable to receive task uuid for enabling the following %s services.",
              pcdrYamlAdapter.getServicesToDisableOnStartupCMSP()));
      throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
    }
  }
}
