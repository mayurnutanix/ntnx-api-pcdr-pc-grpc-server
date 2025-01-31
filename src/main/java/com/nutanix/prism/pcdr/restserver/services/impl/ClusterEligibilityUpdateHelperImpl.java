package com.nutanix.prism.pcdr.restserver.services.impl;

/*
 * Copyright (c) 2024 Nutanix Inc. All rights reserved.
 *
 * Author: shivom.taank@nutanix.com
 *
 */

import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.restserver.dto.ClusterNodeInfo;
import com.nutanix.prism.pcdr.restserver.services.api.ClusterEligibilityUpdateHelper;
import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
import com.nutanix.prism.pcdr.restserver.services.api.PCVMDataService;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.restserver.zk.PCUpgradeWatcher;
import com.nutanix.prism.pcdr.util.IDFUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static com.nutanix.prism.pcdr.constants.Constants.BACKUP_ELIGIBILITY_SCORE_ATTRIBUTE;

@Slf4j
@Component
public class ClusterEligibilityUpdateHelperImpl implements ClusterEligibilityUpdateHelper {

    @Autowired
    private PCBackupService pcBackupService;
    @Autowired
    private EntityDBProxyImpl entityDBProxy;
    @Autowired
    private PCUpgradeWatcher pcUpgradeWatcher;
    @Autowired
    private PCVMDataService pcvmDataService;

    private LocalDateTime lastRunTime;

    // default value if 4 hours
    @Value("${prism.pcdr.backup.schedule.updateEligibilityHighFrequency:14400000}")
    private long updateClusterEligibilityFrequencyMisec;

    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
    @Override
    public void updateClustersEligibility() {

        try {
            if (eligibilityUpdationPreCheckFailure())
                return;

            // 1. get the cluster node info
            List<ClusterNodeInfo> eligibleClusters = getClusterNodeInfos();

            if(!eligibleClusters.isEmpty()) {
                // 2. after retrieving all clusters then update batchUpdateEntitiesArg
                //    with the cluster uuid and attributes to update
                InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg = getClusterBatchUpdateEntityArg(eligibleClusters);

                // 3. update the db
                updateClusterEligibilityInDB(batchUpdateEntitiesArg);
                log.info("Successfully updated cluster eligibility in DB");
            } else {
                log.info("No eligible clusters found to be updated");
            }
        } catch (Exception e) {
            log.error("Aborting updating eligibility of clusters. The following error encountered: ", e);
        }
    }

    @Override
    public boolean isTimeToUpdateClusterEligibility() {
        LocalDateTime current = LocalDateTime.now();
        if(lastRunTime == null)
            return true;
        Duration duration = Duration.between(lastRunTime, current);
        if (duration.toMillis() < updateClusterEligibilityFrequencyMisec) {
            log.debug("Scheduled job for updating cluster already ran at time: {}", dtf.format(lastRunTime));
            return false;
        }
        return true;
    }

    private boolean eligibilityUpdationPreCheckFailure()
            throws PCResilienceException {
        // if the hosting pe is hyper-v then all registered clusters will not be eligible clusters
        // hence don't update the eligibility ever so they will always remain uneligible
        String hostingPEUuid = pcvmDataService.getHostingPEUuid();
        if (hostingPEUuid != null && pcvmDataService.isHostingPEHyperV(hostingPEUuid)) {
            log.info("Hosting PE is HyperV, not updating eligibility of any clusters");
            return true;
        }
        if (pcUpgradeWatcher.isUpgradeInProgress()) {
            log.info("Upgrade is in progress, not proceeding with updating clusters");
            return true;
        }
        return false;
    }

    private List<ClusterNodeInfo> getClusterNodeInfos() throws PCResilienceException {
        InsightsInterfaceProto.GetEntitiesWithMetricsRet result = pcBackupService.fetchPEClusterListFromIDF();
        List<ClusterNodeInfo> eligibleClusters = new ArrayList<>();
        for (InsightsInterfaceProto.QueryGroupResult group : result.getGroupResultsListList()) {
            long numNodes = group.getGroupByColumnValue().getInt64Value();
            // The rows are grouped by num_nodes
            log.debug("Traversing the group with column (num_nodes) value - {}", numNodes);
            for (InsightsInterfaceProto.EntityWithMetricAndLookup entity : group.getLookupQueryResultsList()) {

                final String clusterUuid = Objects.requireNonNull(
                                IDFUtil
                                        .getAttributeValueInEntityMetric(Constants.CLUSTER_UUID,
                                                entity.getEntityWithMetrics()),
                                "cluster_uuid field in Cluster should not be null.")
                        .getStrValue();

                long computeOnlyNodeCount = PCUtil.getComputeOnlyNodeCount(entity);
                eligibleClusters.add(new ClusterNodeInfo(clusterUuid, numNodes - computeOnlyNodeCount));
            }
        }
        return eligibleClusters;
    }


    private InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder getClusterBatchUpdateEntityArg(List<ClusterNodeInfo> eligibleClusters) {
        InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg =
                InsightsInterfaceProto.BatchUpdateEntitiesArg.newBuilder();
        eligibleClusters.forEach(cluster -> addUpdateEntitiesArgForClustersEligibility(
                batchUpdateEntitiesArg, cluster.getExtId(), cluster.getNumCvmNodes()));
        return batchUpdateEntitiesArg;
    }

    private void updateClusterEligibilityInDB(InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg) {
        List<String> errorList;
        try {
            errorList = IDFUtil.getErrorListAfterBatchUpdateRPC(
                    batchUpdateEntitiesArg, entityDBProxy);
            if (CollectionUtils.isNotEmpty(errorList))
                log.error("Error while updating clusters eligibility score: {}", errorList);
            else {
                lastRunTime = LocalDateTime.now();
            }
        } catch (InsightsInterfaceException e) {
            log.error("Exception occurred while updating clusters eligibility score: ", e);
        }
    }

    private void addUpdateEntitiesArgForClustersEligibility(
            InsightsInterfaceProto.BatchUpdateEntitiesArg.Builder batchUpdateEntitiesArg,
            String clusterUuid, Long eligibilityScore) {

        Map<String, Object> attributesValueMap = new HashMap<>();
        attributesValueMap.put(BACKUP_ELIGIBILITY_SCORE_ATTRIBUTE, eligibilityScore);
        // Entity ID is the PC Cluster Uuid.
        InsightsInterfaceProto.UpdateEntityArg.Builder updateEntityArgBuilder =
                IDFUtil.constructUpdateEntityArgBuilder(
                        Constants.CLUSTER,
                        clusterUuid,
                        attributesValueMap);
        updateEntityArgBuilder.setFullUpdate(false);
        batchUpdateEntitiesArg.addEntityList(updateEntityArgBuilder.build());
    }
}
