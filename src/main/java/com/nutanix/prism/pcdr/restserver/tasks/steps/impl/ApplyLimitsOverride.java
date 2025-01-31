package com.nutanix.prism.pcdr.restserver.tasks.steps.impl;

import com.nutanix.infrastructure.cluster.genesis.GenesisInterfaceProto;
import com.nutanix.prism.exception.ErgonException;
import com.nutanix.prism.exception.GenericException;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.constants.TaskConstants;
import com.nutanix.prism.pcdr.restserver.tasks.api.PcdrStepsHandler;
import com.nutanix.prism.pcdr.util.ErgonServiceHelper;
import com.nutanix.prism.pcdr.util.GenesisServiceHelper;
import lombok.extern.slf4j.Slf4j;
import nutanix.ergon.ErgonTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;

import static com.nutanix.prism.proxy.GenesisRemoteProxyImpl.RemoteRpcName.ADD_PRODUCT_RESOURCE_LIMITS;
import static com.nutanix.prism.proxy.GenesisRemoteProxyImpl.RemoteRpcName.GET_ENABLED_PORTFOLIO_PRODUCTS;

@Slf4j
@Service
public class ApplyLimitsOverride implements PcdrStepsHandler {

  @Autowired
  private ErgonServiceHelper ergonServiceHelper;

  @Autowired
  private GenesisServiceHelper genesisServiceHelper;

  private static final String PC = "pc";

  @Override
  public ErgonTypes.Task execute(ErgonTypes.Task rootTask,
                                 ErgonTypes.Task currentSubtask)
    throws ErgonException, PCResilienceException {
    int currentStep = (int) currentSubtask.getStepsCompleted() + 1;

    log.info("STEP {}: Applying limits override.", currentStep);

    try {
      final GenesisInterfaceProto.GetEnabledPortfolioProductsRet
          getEnabledPortfolioProductsRet;

      getEnabledPortfolioProductsRet = genesisServiceHelper.doExecute(
          GET_ENABLED_PORTFOLIO_PRODUCTS,
          GenesisInterfaceProto.NullArg.newBuilder().build());

      List<String> enabledPortfolioProducts;
      if (getEnabledPortfolioProductsRet == null) {
        log.warn("Failed to get list of enabled portfolio" +
                  " products.");
        enabledPortfolioProducts = Collections.EMPTY_LIST;
      } else {
        enabledPortfolioProducts =
            getEnabledPortfolioProductsRet.getProductNamesList();
      }

      final GenesisInterfaceProto.AddProductResourceLimitsArg.Builder
          addProductResourceLimitsArgBuilder = GenesisInterfaceProto
          .AddProductResourceLimitsArg.newBuilder();

      addProductResourceLimitsArgBuilder.addAllProductNames(
          enabledPortfolioProducts);
      addProductResourceLimitsArgBuilder.addProductNames(PC);
      addProductResourceLimitsArgBuilder.setApplyLimits(false);

      final GenesisInterfaceProto.AddProductResourceLimitsRet
          addProductResourceLimitsRet;

      addProductResourceLimitsRet = genesisServiceHelper.doExecute(
          ADD_PRODUCT_RESOURCE_LIMITS,
          addProductResourceLimitsArgBuilder.build());

      if (null == addProductResourceLimitsRet || !addProductResourceLimitsRet
          .getStatus()) {
        String errorMessage = addProductResourceLimitsRet != null?
                              addProductResourceLimitsRet.getMsg() :
                              "Failed to apply limits";
        log.warn(errorMessage);
      }
    }
    catch (GenericException ex) {
      log.warn("Failed to apply limits", ex);
    }

    currentSubtask = ergonServiceHelper.updateTask(
        currentSubtask, currentStep,
        TaskConstants.APPLY_LIMITS_OVERRIDE);

    log.info("STEP {}: Limits override step completed", currentStep);
    return currentSubtask;
  }
}