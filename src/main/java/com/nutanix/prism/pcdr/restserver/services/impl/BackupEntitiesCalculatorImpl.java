package com.nutanix.prism.pcdr.restserver.services.impl;

import com.google.common.collect.Lists;
import com.nutanix.insights.exception.InsightsInterfaceException;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.insights.ifc.InsightsInterfaceProto.*;
import com.nutanix.insights.ifc.InsightsInterfaceProto.BatchGetEntitiesWithMetricsRet.QueryRet;
import com.nutanix.prism.pcdr.exceptions.EntityCalculationException;
import com.nutanix.prism.pcdr.proxy.EntityDBProxyImpl;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl.MaxBackupEntityCountVsAOSVersion;
import com.nutanix.prism.pcdr.restserver.dto.BackupEntity;
import com.nutanix.prism.pcdr.restserver.services.api.BackupEntitiesCalculator;
import com.nutanix.prism.pcdr.restserver.util.PCUtil;
import com.nutanix.prism.pcdr.util.IDFUtil;
import dp1.pri.prism.v4.protectpc.EligibleCluster;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.*;

@Slf4j
@Service
public class BackupEntitiesCalculatorImpl implements BackupEntitiesCalculator {

  private final EntityDBProxyImpl entityDBProxy;
  private final PCDRYamlAdapterImpl pcdrYamlAdapter;
  private final ExecutorService adonisServiceThreadPool;

  @Getter @Setter
  private List<BackupEntity> backupEntityList;

  // This is for master branch
  @Value("${prism.pcdr.maxMasterBackupEntitiesAllowed:120000}")
  private long maxMasterBackupEntitiesAllowed;
  @Value("${prism.pcdr.batch.get.limit:50}")
  private int batchGetEntitiesLimit;

  @Autowired
  public BackupEntitiesCalculatorImpl(
      EntityDBProxyImpl entityDBProxy,
      PCDRYamlAdapterImpl pcdrYamlAdapter,
      ExecutorService adonisServiceThreadPool) {
    setBackupEntityList(new ArrayList<>());
    this.entityDBProxy = entityDBProxy;
    this.pcdrYamlAdapter = pcdrYamlAdapter;
    this.adonisServiceThreadPool = adonisServiceThreadPool;
  }

  /**
   * Update the backup entity list.
   * @throws InsightsInterfaceException - can throw an insights interface
   * exception.
   */
  public void updateBackupEntityList() throws EntityCalculationException {
    GetEntityTypesArg getEntityTypesArg =
        GetEntityTypesArg.newBuilder().build();
    GetEntityTypesRet ret;
    try {
       ret = entityDBProxy.getEntityType(getEntityTypesArg);
    } catch (InsightsInterfaceException e) {
      log.error("Error encountered while fetching tables marked for backup.",
                e);
      throw new EntityCalculationException("Unable to update backup entity " +
                                           "list. Erroring out.");
    }
    List<BackupEntity> newBackupEntityList = new ArrayList<>();

    for (EntityType entityType: ret.getEntityTypeListList()) {
      if (entityType.getTypeInfo().getBackupReplicationControl()
                    .getIsBackupRequired()) {
        boolean isEnableReplicationFromNDFS = false;
        for (TypeInfo.ReplicationControl replicationControl:
            entityType.getTypeInfo().getReplicationControlListList()) {
          if (TypeInfo.ClusterFunctions.kNDFS.equals(
              replicationControl.getEnableReplicationFrom())) {
            isEnableReplicationFromNDFS = true;
            break;
          }
        }
        newBackupEntityList.add(new BackupEntity(
            entityType.getEntityTypeName(),
            isEnableReplicationFromNDFS));
      }
    }
    // Sorting true first and false second. final result will be
    // [true, true, false, false]
    newBackupEntityList.sort(Comparator.comparing(
        BackupEntity::isEnableReplicationFromNDFS,
        Comparator.reverseOrder()));
    log.debug("Backup entity list is being updated to {}", newBackupEntityList);
    setBackupEntityList(newBackupEntityList);
  }

  /**
   * Gives the total entity count in IDF
   * @return - returns a long with total entities.
   * @throws EntityCalculationException - can throw pc backup exception
   */
  public long getTotalBackupEntityCount() throws EntityCalculationException {
    List<List<BackupEntity>> partitionedList =
        Lists.partition(backupEntityList, batchGetEntitiesLimit);
    long totalEntityCount = 0;
    AtomicBoolean isExceptionEncountered = new AtomicBoolean(false);
    List<CompletableFuture<Long>> allFutures =
        new ArrayList<>();
    for (List<BackupEntity> batchList : partitionedList) {
      allFutures.add(
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return calculateEntityCount(batchList);
                }
                catch (InsightsInterfaceException e) {
                  // In-case of exception return count as 0. We will be
                  // allowing the backup to continue even in the above
                  // scenario.
                  isExceptionEncountered.set(true);
                  log.error(
                      "Some error occurred while fetching the count of the entities" +
                      " -", e);
                  return 0L;
                }
              }, adonisServiceThreadPool));
    }
    CompletableFuture.allOf(allFutures.toArray(
        new CompletableFuture[0])).join();
    // Return -1 to tell there is some error which has happened and we can
    // not unpause the backup based on this output.
    if (isExceptionEncountered.get()) {
      throw new EntityCalculationException("Unable to calculate the count of total " +
                                           "entities for backup due " +
                                           "insights exception.");
    }
    for (CompletableFuture<Long> completableFuture : allFutures) {
      try {
        if (completableFuture.get() != null) {
          totalEntityCount += completableFuture.get();
        }
      } catch (Exception e) {
        throw new EntityCalculationException("Unable to calculate count of total " +
                                             "entities for backup.");
      }
    }
    log.info("Entities to backup count is: {}", totalEntityCount);
    return totalEntityCount;
  }

  /**
   * Calculate entity count for the given list of backedUpEntity
   * @param backupEntityList - list of entities for which the count is
   *                           required to be fetched
   * @return - returns a long representing the count of entities
   * @throws InsightsInterfaceException - throws InsightsInterfaceException
   */
  private long calculateEntityCount(List<BackupEntity> backupEntityList)
      throws InsightsInterfaceException {
    BatchGetEntitiesWithMetricsArg.Builder countBatchGet =
        BatchGetEntitiesWithMetricsArg.newBuilder();
    for (BackupEntity backupEntity : backupEntityList) {
      if (backupEntity.isEnableReplicationFromNDFS()) {
        countBatchGet.addQueryList(createGetEntitiesCountQueryForNDFS(
            backupEntity.getEntityName()));
      } else {
        countBatchGet.addQueryList(createGetEntitiesCountQuery(
            backupEntity.getEntityName()));
      }
    }
    long entitiesCount = 0;
    BatchGetEntitiesWithMetricsRet batchRet =
        entityDBProxy.batchGetEntitiesWithMetrics(countBatchGet.build());
    for (QueryRet ret : batchRet.getQueryRetListList()) {
      long currentEntityTableCount = 0;
      for (QueryGroupResult queryGroupResult :
          ret.getQueryRet().getGroupResultsListList()) {
        currentEntityTableCount += queryGroupResult.getTotalEntityCount();
      }
      entitiesCount += currentEntityTableCount;
    }
    return entitiesCount;
  }

  /**
   * Create a get entities count query for non ndfs type entities
   * @param entityName - get entity name
   * @return - returns GetEntitiesWithMetricsArg
   */
  private GetEntitiesWithMetricsArg createGetEntitiesCountQuery(
      String entityName) {
   Query query =
        Query.newBuilder().addEntityList(
            EntityGuid.newBuilder()
                      .setEntityTypeName(entityName)
                      .build()).setFlags(COUNT_FLAG_IDF)
             .setQueryName("prism_pcdr:entity_count_" + entityName)
             .build();
    return GetEntitiesWithMetricsArg.newBuilder().setQuery(query).build();
  }

  /**
   * Create a get entities count query for ndfs type
   * @param entityName - get entity name
   * @return - returns GetEntitiesWithMetricsArg
   */
  private GetEntitiesWithMetricsArg createGetEntitiesCountQueryForNDFS(
      String entityName) {
    BooleanExpression emptyMasterClusterUuid =
        IDFUtil.constructBooleanExpression(
            IDFUtil.getColumnExpression(MASTER_CLUSTER_UUID_ATTRIBUTE),
            Expression.newBuilder().setLeaf(
                LeafExpression.newBuilder().setValue(
                    InsightsInterfaceProto.DataValue.newBuilder().build())
                              .build()).build(),
            ComparisonExpression.Operator.kEQ);
    Query query =
        Query.newBuilder().addEntityList(
            EntityGuid.newBuilder()
                      .setEntityTypeName(entityName)
                      .build()).setFlags(COUNT_FLAG_IDF)
             .setQueryName("prism_pcdr:entity_count_with_where_clause_" + entityName)
             .setWhereClause(emptyMasterClusterUuid).build();
    return GetEntitiesWithMetricsArg.newBuilder().setQuery(query).build();
  }

  /**
   * The method calculates the max backup entities supported with the help of
   * pre-seeded max allowed entities with a minimum version.
   * @param getEntitiesWithMetricsRet - Data returned by IDF.
   * @return - returns the number of entities supported
   * @throws InsightsInterfaceException - can throw insights_interface
   * exception.
   */
  public long getMaxBackupEntitiesSupported(GetEntitiesWithMetricsRet
                                              getEntitiesWithMetricsRet) {

    long maxBackupEntitiesSupported = Long.MAX_VALUE;

    List<EligibleCluster> maxBackupEntitiesSupportedPEs =
        getPeMaxBackupEntitiesSupportedList(getEntitiesWithMetricsRet);

    for(EligibleCluster eligibleCluster : maxBackupEntitiesSupportedPEs) {
      maxBackupEntitiesSupported = Math.min(maxBackupEntitiesSupported,
                                            eligibleCluster.backedUpEntitiesCountSupported);
    }

    log.info("Maximum  backup entities supported are:" +
      maxBackupEntitiesSupported);
    return maxBackupEntitiesSupported;
  }

  /**
   * The method calculates the max backup entities supported with the help of
   * pre-seeded max allowed entities with a minimum version.
   * @param getEntitiesWithMetricsRet - Cluster table Data returned by IDF.
   * @return - returns list of cluster swith attributes name, uuid,
   *           backupEntitySupported
   * @throws InsightsInterfaceException - can throw insights_interface
   * exception.
   */
  public List<EligibleCluster> getPeMaxBackupEntitiesSupportedList(
    GetEntitiesWithMetricsRet getEntitiesWithMetricsRet){
    List<EligibleCluster> eligibleClusters = new ArrayList<>();
    for (QueryGroupResult group : getEntitiesWithMetricsRet.getGroupResultsListList()) {
      for (EntityWithMetricAndLookup entity : group.getLookupQueryResultsList()) {
        String peVersion =
          IDFUtil.getAttributeValueInEntityMetric(
            CLUSTER_VERSION,
            entity.getEntityWithMetrics()).getStrValue();
        long numNodes =
          IDFUtil.getAttributeValueInEntityMetric(
            NUM_NODES,
            entity.getEntityWithMetrics()).getInt64Value();
        String clusterName =
          IDFUtil.getAttributeValueInEntityMetric(
            CLUSTER_NAME,
            entity.getEntityWithMetrics()).getStrValue();
        long computeOnlyNodeCount = PCUtil.getComputeOnlyNodeCount(entity);
        long backupEntitiesSupported = getMaxBackupEntitiesSupportedForAOS(
          peVersion, numNodes - computeOnlyNodeCount);
        EligibleCluster eligibleCluster = new EligibleCluster();
        eligibleCluster.setClusterName(clusterName);
        eligibleCluster.setBackedUpEntitiesCountSupported(backupEntitiesSupported);
        eligibleCluster.setClusterUuid(IDFUtil.getAttributeValueInEntityMetric(
            CLUSTER_UUID,
            entity.getEntityWithMetrics()).getStrValue());
        eligibleClusters.add(eligibleCluster);
      }
    }
    log.debug("Cluster list with attributes like name, uuid & " +
              "backupEntitiesSupported: {}", eligibleClusters);
    return eligibleClusters;
  }

  /**
   * It tells the count of backup entity supported for a given AOS version
   * and it's number of nodes.
   * Let's say you pass 6.1 as the version of AOS and the number of nodes are 16
   * The below values are not actual values just for example:
   * Let's say master supports 180000
   * The MaxBackupEntityCountVsAOSVersions (sorted list)
   * [{6.0, 40000}, {6.0.1, 50000}, {6.1, 120000}, {6.2, 150000}]
   * Then the code will work in following way ->
   * Initially maxSupportedCount will be 180000, then it will check
   * aosVersion is equal to master if that's the case supported count is
   * 180000 and no need for further computation else let's go inside the loop.
   * first {6.0, 40000} -> compare 6.0 and 6.1, assign supportedCount = 40000
   * second {6.0.1, 50000} -> compare 6.0.1 and 6.1, assign supportedCount =
   * 50000
   * third {6.1, 120000} -> compare 6.1 and 6.1, assign supported count = 120000
   * fourth {6.2, 150000} -> compare 6.2 and 6.1, returns false, breaks
   * return 120000 * 16
   * @param aosVersion - the AOS version of the current cluster
   * @param numNodes - number of nodes of AOS
   * @return - returns max backup entities supported.
   */
  public long getMaxBackupEntitiesSupportedForAOS(String aosVersion,
                                                  long numNodes) {
    // We require the sorted list of adapter according to versions.
    List<MaxBackupEntityCountVsAOSVersion> maxBackupEntityCountVsAOSVersions =
        pcdrYamlAdapter.getMaxBackupEntityCountVsAOSVersions();
    long supportedCount = maxMasterBackupEntitiesAllowed;
    // If peVersion is master then the supported count will be
    // maxMasterBackupEntitiesAllowed
    if (!aosVersion.equals("master")) {
      for (MaxBackupEntityCountVsAOSVersion maxBackupEntityCountVsAOSVersion:
          maxBackupEntityCountVsAOSVersions) {
        // If peVersion >= minimum count version then the method returns true
        if (PCUtil.comparePEVersions(aosVersion,
                                     maxBackupEntityCountVsAOSVersion.getMinVersion())) {
          supportedCount = maxBackupEntityCountVsAOSVersion.getSupportedCount();
        } else {
          break;
        }
      }
    }
    long totalBackupSupportedEntityCount = supportedCount * numNodes;
    log.debug("Total entities supported for backup by the AOS version {} with" +
              "number of nodes {} is: {}", aosVersion, numNodes,
              totalBackupSupportedEntityCount);
    return totalBackupSupportedEntityCount;
  }

  /**
   * The method gets all the PEs on which backup Limit is exceeded.
   * @param replicaPEUuids - Replica PE UUIDS.
   * @return - returns the list of PEs on which backup limit exceeded.
   * @throws InsightsInterfaceException - can throw insights_interface
   * exception.
   */
  public List<EligibleCluster> getBackupLimitExceededPes(
      List<String> replicaPEUuids) throws EntityCalculationException {
    List<EligibleCluster> backupLimitExceededPes = new ArrayList<>();
    if (getBackupEntityList().isEmpty()) {
      return backupLimitExceededPes;
    }
    GetEntitiesWithMetricsRet ret;
    try {
      ret = PCUtil.getClusterInfoFromIDf(replicaPEUuids, entityDBProxy);
    }
    catch (InsightsInterfaceException e) {
      String errorMessage = "Unable to get maximum entities supported on " +
                            "replica clusters.";
      log.warn(errorMessage, e);
      throw new EntityCalculationException(errorMessage);
    }
    long  totalBackupEntityCount = getTotalBackupEntityCount();

    List<EligibleCluster> maxBackupEntitiesSupportedPEs =
        getPeMaxBackupEntitiesSupportedList(ret);

    return maxBackupEntitiesSupportedPEs
        .stream()
        .filter(pe -> totalBackupEntityCount > pe.backedUpEntitiesCountSupported)
        .collect(Collectors.toList());
  }
}
