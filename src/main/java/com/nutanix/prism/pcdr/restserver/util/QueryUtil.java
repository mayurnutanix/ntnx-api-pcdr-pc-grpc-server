package com.nutanix.prism.pcdr.restserver.util;

import com.nutanix.dp1.pri.prism.v4.recoverpc.PCRestoreDataQuery;
import com.nutanix.insights.ifc.InsightsInterfaceProto;
import com.nutanix.insights.ifc.InsightsInterfaceProto.*;
import com.nutanix.insights.ifc.InsightsInterfaceProto.QueryOrderBy.SortKey;
import com.nutanix.insights.ifc.InsightsInterfaceProto.QueryOrderBy.SortOrder;
import com.nutanix.prism.pcdr.constants.Constants;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl;
import com.nutanix.prism.pcdr.restserver.dto.RestoreInternalOpaque;
import com.nutanix.prism.pcdr.util.IDFUtil;
import com.nutanix.prism.service.EntityDbServiceHelper;
import com.nutanix.zeus.protobuf.Configuration.ConfigurationProto.ManagementServer;
import lombok.extern.slf4j.Slf4j;
import nutanix.abac.AbacTypes;

import java.util.*;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.*;
import static com.nutanix.prism.pcdr.constants.Constants.CLUSTER_NAME;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.*;

@Slf4j
public class QueryUtil {
  /**
   * Private constructor to prevent initialization of utility class
   */
  private QueryUtil(){}

  /**
   * Construct the query for PCBackupConfig IDF table
   * @return IDF Query
   */
  public static Query constructPCBackupConfigQuery() {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    // Set the name of the query.
    query.setQueryName("prism_pcdr:pc_backup_config");
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
      .setEntityTypeName(Constants.PC_BACKUP_CONFIG));

    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Create a list containing rawColumns to query from IDF.
    List<String> rawColumns = new ArrayList<>();
    rawColumns.add(Constants.ZPROTOBUF);
    // Set raw Columns in query
    IDFUtil.addRawColumnsInGroupByBuilder(rawColumns, groupBy);
    // Set groupby in query
    query.setGroupBy(groupBy);
    return query.build();
  }

  /**
   * Construct the kExist query for PCBackupConfig IDF table for given attribute.
   * @return IDF Query
   */
  public static Query constructPCBackupConfigWithExistQuery(String attrName) {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    // Set the name of the query.
    query.setQueryName("prism_pcdr:pc_backup_config");
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
      .setEntityTypeName(Constants.PC_BACKUP_CONFIG));

    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Create a list containing rawColumns to query from IDF.
    List<String> rawColumns = new ArrayList<>();
    rawColumns.add(attrName);
    rawColumns.add(Constants.ZPROTOBUF);
    // Set raw Columns in query
    IDFUtil.addRawColumnsInGroupByBuilder(rawColumns, groupBy);
    // Set groupby in query
    query.setGroupBy(groupBy);

    final InsightsInterfaceProto.LeafExpression.Builder leafExpression =
            InsightsInterfaceProto.LeafExpression.newBuilder();
    leafExpression.setColumn(attrName);
    final InsightsInterfaceProto.Expression.Builder expression =
            InsightsInterfaceProto.Expression.newBuilder();
    expression.setLeaf(leafExpression);
    final ComparisonExpression.Builder comparisonExpression =
            ComparisonExpression.newBuilder();
    comparisonExpression.setLhs(expression);
    comparisonExpression.setOperator(ComparisonExpression.Operator.kExists);
    final BooleanExpression.Builder whereClause =
            BooleanExpression.newBuilder();
    whereClause.setComparisonExpr(comparisonExpression);
    query.setWhereClause(whereClause);
    return query.build();
  }

  /**
   * Construct the query for fetching PCBackupConfig  By entity ID in IDF table
   * @return IDF Query
   */
  public static Query constructGetPCBackupConfigQueryById(String entityID) {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    // Set the name of the query.
    query.setQueryName("prism_pcdr:pc_backup_config");
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
      .setEntityTypeName(Constants.PC_BACKUP_CONFIG)
      .setEntityId(entityID));

    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Create a list containing rawColumns to query from IDF.
    List<String> rawColumns = new ArrayList<>();
    rawColumns.add(Constants.ZPROTOBUF);
    // Set raw Columns in query
    IDFUtil.addRawColumnsInGroupByBuilder(rawColumns, groupBy);
    // Set groupby in query
    query.setGroupBy(groupBy);
    return query.build();
  }

  /**
   * Constructs IDF query to return all the pc_zk_data entities.
   * @return - IDF Query.
   */
  public static Query constructPCZkDataQuery() {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    // Set the name of the query.
    query.setQueryName("prism_pcdr:pc_backup_config");
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
                                  .setEntityTypeName(Constants.PC_ZK_DATA));

    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Set groupby in query
    query.setGroupBy(groupBy);
    return query.build();
  }

  /**
   * Creates query - the PE entities from cluster entity_type and sorts it in
   * a sortorder given for ssd free space.
   * @param sortOrder - order in which the free space needs to sorted
   *                    (kDescending)
   * @param sortKey - Key to be provided for free space sorting (kLatest)
   * @return query for filtering PE clusters.
   **/
  public static Query constructEligibleClusterListQuery(SortOrder sortOrder,
                                                        SortKey sortKey) {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();

    // Set the name of the query.
    query.setQueryName("prism_pcdr:cluster");
    // service_list values which identifies whether the cluster is a PE or not.
    List<String> valueList = Collections.singletonList(
      Constants.SERVICE_LIST_PE);
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
      .setEntityTypeName(Constants.CLUSTER));

    // Creating a boolean expression to filter only PE's.
    final BooleanExpression clusterIsPE = IDFUtil.constructBooleanExpression(
      IDFUtil.getColumnExpression(Constants.SERVICE_LIST),
      IDFUtil.getValueStringListExpression(valueList),
      ComparisonExpression.Operator.kContains
    );

    // hypervisor_types values which identifies whether the cluster is HYPERV
    // or not
    List<String> hypervisorValueList = Collections.singletonList(
      ManagementServer.HypervisorType.kHyperv.name());

    final BooleanExpression clusterHyperV = IDFUtil
      .constructBooleanExpression(
        IDFUtil.getColumnExpression(Constants.HYPERVISOR_TYPES),
        IDFUtil.getValueStringListExpression(hypervisorValueList),
        ComparisonExpression.Operator.kContains
    );
    final BooleanExpression clusterNotHyperV = BooleanExpression.newBuilder()
      .setLhs(clusterHyperV)
      .setOperator(BooleanExpression.Operator.kNot)
      .build();

    final BooleanExpression serviceAndHyperV =
      IDFUtil.constructBooleanExpression(clusterIsPE, clusterNotHyperV,
        BooleanExpression.Operator.kAnd);

    query.setWhereClause(serviceAndHyperV);

    // Creating group by Builder.
    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Create a list containing rawColumns to query from IDF.
    List<String> rawColumns = new ArrayList<>();
    rawColumns.add(Constants.CLUSTER_NAME);
    rawColumns.add(Constants.CLUSTER_UUID);
    rawColumns.add(Constants.CLUSTER_VERSION);
    rawColumns.add(Constants.SSD_FREE_BYTES);
    // Set raw Columns in query
    IDFUtil.addRawColumnsInGroupByBuilder(rawColumns, groupBy);
    // Set order by in query
    final QueryOrderBy.Builder orderBy = QueryOrderBy.newBuilder();
    orderBy.setSortColumn(Constants.SSD_FREE_BYTES);
    orderBy.setSortOrder(sortOrder);
    orderBy.setSortKey(sortKey);
    groupBy.setRawSortOrder(orderBy);
    groupBy.setGroupByColumn(Constants.NUM_NODES);
    final QueryOrderBy.Builder orderByForGroup = QueryOrderBy.newBuilder();
    orderByForGroup.setSortColumn(Constants.NUM_NODES);
    orderByForGroup.setSortOrder(sortOrder);
    groupBy.setGroupSortOrder(orderByForGroup);

    groupBy.addLookupQuery(constructComputeOnlyLookupQuery());
    // Set groupby in query
    query.setGroupBy(groupBy);
    return query.build();
  }

  /**
   * Create a lookup query for node table which will be used to fetch total
   * number of compute only nodes in a cluster.
   * @return - returns a query object.
   */
  public static Query constructComputeOnlyLookupQuery() {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    query.addEntityList(EntityGuid.newBuilder().setEntityTypeName(
        Constants.NODE_TABLE));
    QueryGroupBy.Builder groupByBuilder = QueryGroupBy.newBuilder();
    IDFUtil.addRawColumnsInGroupByBuilder(
        Collections.singletonList(
            Constants.IS_COMPUTE_ONLY_ATTRIBUTE), groupByBuilder);
    query.setGroupBy(groupByBuilder);
    final BooleanExpression matchClusterUuid =
        IDFUtil.constructBooleanExpression(
            IDFUtil.getColumnExpression(Constants.CLUSTER_UUID),
            IDFUtil.getColumnExpression(Constants.CLUSTER_UUID),
            ComparisonExpression.Operator.kEQ);

    final BooleanExpression isComputeOnly =
        IDFUtil.constructBooleanExpression(
            IDFUtil.getColumnExpression(Constants.IS_COMPUTE_ONLY_ATTRIBUTE),
            IDFUtil.getValueExpression(1L),
            ComparisonExpression.Operator.kEQ);

    final BooleanExpression whereClause =
        IDFUtil.constructBooleanExpression(matchClusterUuid, isComputeOnly,
                                           BooleanExpression.Operator.kAnd);
    query.setWhereClause(whereClause);
    return query.build();
  }

  /**
   * Creates query - the PE entities from cluster entity_type
   * @return query for filtering PE clusters.
   **/
  public static Query constructPEClusterCountQuery() {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();

    // Set the name of the query.
    query.setQueryName("prism_pcdr:cluster");
    // service_list values which identifies whether the cluster is a PE or not.
    List<String> valueList = Collections.singletonList("AOS");
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder().setEntityTypeName("cluster"));

    // Creating a boolean expression to filter only PE's using
    // IDFUtil.constructBooleanExpression(lhs, rhs, operand) to be added to
    // where clause. LHS defines the query column, RHS defines the value list
    // and Operand defines the operation between LHS and RHS
    final BooleanExpression clusterIsPE = IDFUtil.constructBooleanExpression(
        IDFUtil.getColumnExpression(Constants.SERVICE_LIST),
        IDFUtil.getValueStringListExpression(valueList),
        ComparisonExpression.Operator.kContains
    );
    query.setWhereClause(clusterIsPE);

    // Flag to fetch only the count of entities
    query.setFlags(2);
    return query.build();
  }

  /**
   * Creates a for query virtual_nic from IDF.
   * @param vmUuidList - list of vms for the PC cluster.
   * @return - returns a map of query containing vmUuid.
   */
  public static Query constructVirtualNicQuery(final List<String> vmUuidList) {
    EntityDbServiceHelper entityDbServiceHelper = new EntityDbServiceHelper();
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();

    // Set the name of the query.
    query.setQueryName("prism_pcdr:virtual_nic");
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
      .setEntityTypeName(Constants.VIRTUAL_NIC));

    // Creating a boolean expression to filter only PE's.
    final BooleanExpression virtualNicsofVMs = entityDbServiceHelper
      .constructInBooleanExpression(Constants.VM_TABLE,
        new HashSet<>(vmUuidList)
      );
    query.setWhereClause(virtualNicsofVMs);

    // Creating group by Builder.
    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Create a list containing rawColumns to query from IDF.
    List<String> rawColumns = new ArrayList<>();
    rawColumns.add(Constants.VM_TABLE);
    rawColumns.add(Constants.VIRTUAL_NETWORK);
    // Set raw Columns in query
    IDFUtil.addRawColumnsInGroupByBuilder(rawColumns, groupBy);
    // Set groupby in query
    query.setGroupBy(groupBy);

    return query.build();
  }

  /**
   * Constructs IDF query to return specific pc_backup_specs entity.
   * @return - IDF Query.
   */
  public static Query constructPcBackupSpecsProtoQuery(String pcClusterUuid,
                                                  List<String> rawColumns) {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    // Set the name of the query.
    query.setQueryName(Constants.PCDR_QUERY);
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
      .setEntityTypeName(Constants.PC_BACKUP_SPECS_TABLE)
      .setEntityId(pcClusterUuid));
    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Set raw Columns in query
    IDFUtil.addRawColumnsInGroupByBuilder(rawColumns, groupBy);
    query.setGroupBy(groupBy);
    return query.build();
  }

  /**
   * Constructs IDF query to return all the pc_backup_metadata entities.
   * @return - IDF Query.
   */
  public static Query constructPcBackupSpecsProtoQuery() {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    // Set the name of the query.
    query.setQueryName("prism_pcdr:pc_backup_specs");
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
                                  .setEntityTypeName(Constants.PC_BACKUP_SPECS_TABLE));

    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Set groupby in query
    query.setGroupBy(groupBy);
    return query.build();
  }

  public static Query constructPCAbacUserCapabilityQueryForAdmin() {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    // Set the name of the query.
    query.setQueryName("prism_pcdr:abac_user_capability");
    query.addEntityList(EntityGuid
                            .newBuilder()
                            .setEntityTypeName(
                                Constants.ABAC_USER_CAPABILITY_TABLE));
    // Creating a boolean expression to filter only PE's.
    final BooleanExpression userTypeExpr = IDFUtil
        .constructBooleanExpression(
            IDFUtil.getColumnExpression(Constants.USER_TYPE_ATTRIBUTE),
            IDFUtil.getValueExpression(
                AbacTypes.AbacUserCapability.sourceType.kLOCAL.name()),
            ComparisonExpression.Operator.kEQ);
    final BooleanExpression usernameExpr = IDFUtil
        .constructBooleanExpression(
            IDFUtil.getColumnExpression(Constants.USERNAME_ATTRIBUTE),
            IDFUtil.getValueExpression(
                Constants.ADMIN_USERNAME),
            ComparisonExpression.Operator.kEQ);
    final BooleanExpression localAdminUserBoolExpr = IDFUtil
        .constructBooleanExpression(
            userTypeExpr,
            usernameExpr,
            BooleanExpression.Operator.kAnd);
    query.setWhereClause(localAdminUserBoolExpr);
    return query.build();
  }

  /**
   * Function to create query for trust data
   * @param restoreInternalOpaque The object containing the replica PE UUID
   * @param pcClusterId PC Cluster UUID
   * @param pcdrYamlAdapter Yaml adapter object
   * @return PCRestoreDataQuery containing filepaths and zookeeper nodes
   */
  public static PCRestoreDataQuery getPCTrustDataQuery(RestoreInternalOpaque
    restoreInternalOpaque, String pcClusterId,
    PCDRYamlAdapterImpl pcdrYamlAdapter){
    PCRestoreDataQuery pcRestoreDataQuery = new PCRestoreDataQuery();

    log.debug(pcdrYamlAdapter.getFileSystemNodes().toString());
    List<String> filePaths =
      pcdrYamlAdapter.getFileSystemNodes().getGeneralNodes().stream()
        .filter(p->p.getDataType().equals(Constants.TRUST_DATA_TYPE_IN_YAML)).map(
        PCDRYamlAdapterImpl.PathNode::getPath).collect(Collectors.toList());
    log.info("Files in PCDataQuery: "+ filePaths);
    pcRestoreDataQuery.setFilePaths(filePaths);
    pcRestoreDataQuery.setPcClusterId(pcClusterId);
    List<String> zkIdfEntities =
      pcdrYamlAdapter.getZookeeperNodes().stream()
        .filter(p->p.getDataType().equals(Constants.TRUST_DATA_TYPE_IN_YAML)).map(
        PCDRYamlAdapterImpl.PathNode::getPath).collect(Collectors.toList());
    List<String> customZkEntities = new LinkedList<>();
    for(String zkEntity: zkIdfEntities){
      if(zkEntity.equals(Constants.CLUSTEREXTERNALSTATE_PATH)){
        zkEntity = zkEntity + "/" + restoreInternalOpaque.getPeUuid();
      }
      customZkEntities.add(zkEntity);
    }
    pcRestoreDataQuery.setZkIdfEntities(customZkEntities);
    log.info("ZkNodes in PCDataQuery: "+ customZkEntities);
    return pcRestoreDataQuery;
  }

  /**
   * Create a cluster query to get the version, number of nodes, name of the
   * cluster.
   * @param replicaPEEntities - uuid of the replicaPEs
   * @return - returns GetEntitiesWithMetricsArg object
   */
  public static GetEntitiesWithMetricsArg createGetEntitiesClusterWithNodes(
      List<EntityGuid> replicaPEEntities) {
    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    IDFUtil.addRawColumnsInGroupByBuilder(
        Arrays.asList(CLUSTER_VERSION, NUM_NODES, CLUSTER_UUID, CLUSTER_NAME),
        groupBy);
    groupBy.addLookupQuery(QueryUtil.constructComputeOnlyLookupQuery());
    Query query =
        Query.newBuilder()
             .addAllEntityList(replicaPEEntities)
             .setGroupBy(groupBy)
             .setQueryName("prism_pcdr:cluster_node_version_query")
             .build();
    return GetEntitiesWithMetricsArg.newBuilder().setQuery(query).build();
  }

  public static GetEntitiesWithMetricsArg createGetObjectStoreEntitiesTimestamp() {
    QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    IDFUtil.addRawColumnsInGroupByBuilder(
        Collections.singletonList(OBJECT_STORE_SYNC_COMPLETED_ATTRIBUTE), groupBy);
    return GetEntitiesWithMetricsArg
        .newBuilder().setQuery(
            Query.newBuilder()
                 .addEntityList(EntityGuid
                                    .newBuilder()
                                    .setEntityTypeName(OBJECT_STORE_SYNC_MARKER_TABLE)
                               )
                 .setQueryName("prism_pcdr:object_store_entities_lastsynctime")
                 .setGroupBy(groupBy)).build();
  }

  /**
   * Constructs IDF query to return all the pc_seed_data entities.
   * @return - IDF Query.
   */
  public static Query constructPcSeedDataQuery() {
    // Creating a query builder
    final Query.Builder query = Query.newBuilder();
    // Set the name of the query.
    query.setQueryName("prism_pcdr:pc_seed_data");
    // Add the entity type for the query.
    query.addEntityList(EntityGuid.newBuilder()
                                  .setEntityTypeName(PC_SEED_DATA_ENTITY_TYPE));

    final QueryGroupBy.Builder groupBy = QueryGroupBy.newBuilder();
    // Set groupby in query
    query.setGroupBy(groupBy);
    return query.build();
  }
}
