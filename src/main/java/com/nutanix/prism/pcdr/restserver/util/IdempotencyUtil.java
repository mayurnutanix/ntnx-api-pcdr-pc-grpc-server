package com.nutanix.prism.pcdr.restserver.util;


/**
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: mayur.ramavat@nutanix.com
 *
 * Util to check idempotency
 */

import com.nutanix.api.utils.json.JsonUtils;
import com.nutanix.api.utils.task.ErgonTaskUtils;
import com.nutanix.prism.exception.idempotency.IdempotencySupportException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.util.idempotency.IdempotencySupportService;
import dp1.pri.prism.v4.config.TaskReference;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class IdempotencyUtil {

  private static final int DEFAULT_NUM_RETRIES = 3;
  private static final int DEFAULT_RETRY_DELAY_MILLISECONDS = 500;

  private final IdempotencySupportService idempotencySupportService;

  @Autowired
  public IdempotencyUtil(final IdempotencySupportService idempotencySupportService) {
    this.idempotencySupportService = idempotencySupportService;
  }

   /**
     * Fetch existing task info from api utils for the request id.
     * @param requestId
     * @param userId
     * @return taskId.
    * @throws IdempotencySupportException,PCResilienceException
   */
  @Retryable(value = IdempotencySupportException.class, maxAttempts = DEFAULT_NUM_RETRIES,
          backoff = @Backoff(delay = DEFAULT_RETRY_DELAY_MILLISECONDS))
  public String getIdempotentTaskInfo(final String requestId, final String userId)
          throws IdempotencySupportException, PCResilienceException {

    if (requestId == null || requestId.trim().isEmpty()) {
     throw new PCResilienceException(ErrorMessages.MISSING_REQUEST_ID,
              ErrorCode.PCBR_HEADER_NTNX_REQUEST_ID_NOT_FOUND, HttpStatus.BAD_REQUEST);
    }

    if (userId == null || userId.trim().isEmpty()) {
      throw new PCResilienceException(ErrorMessages.MISSING_USER_ID,
              ErrorCode.PCBR_HEADER_USER_ID_NOT_FOUND, HttpStatus.BAD_REQUEST);
    }
    // Look up idempotency map
    final String taskReferenceJson = idempotencySupportService.fetchTaskOrReserveRequestId(requestId, userId, null);
    String taskUuid = null;
    if (taskReferenceJson != null && !taskReferenceJson.isEmpty()) {
      TaskReference taskReference = getTaskReferenceFromJson(taskReferenceJson);
      if (taskReference != null) {
        taskUuid = ErgonTaskUtils.getTaskUuid(taskReference.getExtId());
      } else {
        log.error("Deserialization of task JSON queried from idempotency service failed.");
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
    }
    return taskUuid;
  }

  /**
     * Update request id and task id in request_reference entity.
     * @param taskUuid
     * @param requestId
     * @param userId
     * @return taskId.
    * @throws IdempotencySupportException,PCResilienceException
   */
  @Retryable(value = IdempotencySupportException.class, maxAttempts = DEFAULT_NUM_RETRIES,
          backoff = @Backoff(delay = DEFAULT_RETRY_DELAY_MILLISECONDS))
  public void updateTaskReferenceTable(final String taskUuid, final String requestId, final String userId) {
    try{
      TaskReference taskReference = new TaskReference();
      taskReference.setExtId(ErgonTaskUtils.getTaskReferenceExtId(taskUuid));
      idempotencySupportService.updateTask(requestId, userId, taskUuid, taskReference);
    } catch (IdempotencySupportException e) {
      // Not throwing the exception if failed to update the table as backup
      // is already registered.
      log.error("Failed to update the request reference table for idempotency check.");
    }
  }

  static TaskReference getTaskReferenceFromJson(final String taskJson) {
    return JsonUtils.getType(taskJson, TaskReference.class);
  }

}
