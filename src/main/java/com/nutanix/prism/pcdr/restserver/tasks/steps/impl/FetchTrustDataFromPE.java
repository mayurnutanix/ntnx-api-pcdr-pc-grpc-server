package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.google.protobuf.ByteString;
import com.nutanix.dp1.pri.prism.v4.recoverpc.PCRestoreData;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.clients.RecoverPcProxyClient;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.SerializationUtils;

import java.util.Objects;

@Service
@Slf4j
public class FetchTrustDataFromPE implements PcdrStepsHandler {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;
  @Autowired
  private InstanceServiceFactory instanceServiceFactory;
  @Autowired
  private RecoverPcProxyClient recoverPcProxyClient;
  @Autowired
  private PCDRYamlAdapterImpl pcdrYamlAdapter;

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;
    // STEP: First step of restore basic trust is making a call to
    // replica PE using the IP's that are stored in internal opaque.
    // Now the trust data fetched is stored in the internal opaque of
    // restoreBasicTrustSubtask.
    log.info("Step {}: Fetching trust data from replica PE.", currentStep);
    RestoreInternalOpaque restoreInternalOpaque =
      (RestoreInternalOpaque) SerializationUtils.deserialize(
        rootTask.getInternalOpaque().toByteArray());
    PCRestoreData pcRestoreData = PCUtil.fetchPCRestoreDataFromReplicaPE(
      Objects.requireNonNull(restoreInternalOpaque, "Restore Internal " +
        "Opaque was found null. It should be present for the task to " +
        "continue."),
      instanceServiceFactory.getClusterUuidFromZeusConfigWithRetry(),
      pcdrYamlAdapter,
      recoverPcProxyClient
    );
    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        ByteString.copyFrom(Objects.requireNonNull(
        SerializationUtils.serialize(pcRestoreData), "The serilization of " +
          "trust data failed.")), TaskConstants.FETCH_TRUST_DATA_PERCENTAGE);
    log.info("Step {} of basic restore trust subtask successfully " +
      "completed.", currentStep);
    return currentSubtask;
  }
}