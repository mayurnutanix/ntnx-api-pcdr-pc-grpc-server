package com.nutanix.prism.pcdr.restserver.tasks.api;

import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import nutanix.ergon.ErgonTypes;

public interface PcdrSubtaskHandler {
  ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                          ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException;
  TaskConstants.TaskOperationType getTaskOperationType();
}