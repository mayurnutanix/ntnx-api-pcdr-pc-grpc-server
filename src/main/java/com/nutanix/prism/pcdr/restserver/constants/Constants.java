package com.nutanix.prism.pcdr.restserver.constants;

public class Constants {

  private Constants() {}

  public static final String ENDPOINT_ADDRESS_RESTORE_IDF = "endpoint_address";
  public static final String ENDPOINT_FLAVOUR_RESTORE_IDF = "endpoint_flavour";
  public static final String ENDPOINT_NAME_RESTORE_IDF = "endpoint_name";

  public static final String PULSE_METRIC_BACKUP_PAUSED_TIMESTAMP =
      "backup_paused_timestamp";
  public static final String PULSE_METRICS_BACKUP_PAUSED_MESSAGE =
      "backup_paused_message";
  public static final String PULSE_METRICS_BACKUP_START_TIMESTAMP =
      "backup_start_timestamp";
  public static final String PULSE_METRICS_TOTAL_BACKUP_ENTITIES_COUNT =
      "total_backup_entities_count";
  public static final String PULSE_METRICS_TOTAL_VMS_MANAGED_BY_PC =
      "total_vms_managed_by_pc";
  public static final String PULSE_METRICS_TOTAL_PE_CLUSTERS_MANAGED_BY_PC =
      "total_pe_clusters_managed_by_pc";
  public static final String PULSE_METRICS_VERSION = "version";
  public static final String PULSE_METRICS_NUM_NODES = "num_nodes";
  public static final String PAUSED_MESSAGE = "was already paused";
  public static final String IN_PROGRESS_MESSAGE = "is already in progress";
  public static final String PAUSED = "paused";
  public static final String RESUMED = "resumed";
  public static final String OBJECT_STORE_SYNC_MARKER_TABLE =
      "pc_object_store_backup_sync_marker";
  public static final String OBJECT_STORE_SYNC_COMPLETED_ATTRIBUTE =
      "sync_completed_till_usecs";
  public static final String PC_SEED_DATA_OBJECT_KEY_STR =
          "pcdr/%s/seed-data/pc_seed_data.dat";
  public static final String PCDR_BASE_OBJECT_KEY_STR = "pc_backup/%s";
  public static final String PCDR_ENDPOINT_ADDRESS_SPLIT_REGEX = "/pc_backup/";
  public static final String PC_BACKUP_CONFIG_ENDPOINT_ATTR =
      "endpoint_address";
  public static final String FILE_EXISTS_TEXT = "File exists.";
  public static final String PULSE_PC_ENDPOINT_FLAVOUR_ATTRIBUTE =
      "pc_endpoint_flavour";
  public static final String PULSE_ENDPOINT_ADDRESS_ATTRIBUTE =
      "objectstore_endpoint_address";
  public static final String PULSE_ENDPOINT_NAME_ATTRIBUTE =
      "objectstore_endpoint_name";
  public static final String SERVICES_ENABLEMENT_PATH =
      "/appliance/logical/services_enablement_status";
  public static final String TRUST_ZK_NODE =
      "/appliance/physical/trustsetup/clusterexternalstate";
  public static final String RESTORE_ALREADY_IN_PROGRESS =
      "Restore Prism Central task in progress. Operation not allowed.";

  public static final int CONNECTION_TIMEOUT = 1000;
  public static final int READ_TIMEOUT = 30000;
  public static final int PC_MERCURY_PORT = 9444;
  public static final int RETRY_DELAY_FOR_FETCHING_CONFIG_MS = 1000;
  public static final int MAX_ATTEMPT_FOR_BACKUP_CONFIG_FETCH = 2;
  public static final int FILE_EXIST_TIMEOUT_MILLISECONDS = 1000;

  public static final int DEFAULT_OBJECTSTORE_BACKUP_RETENTION_IN_DAYS = 31;

  // PC seed data entity idf constants
  public static final String PC_SEED_DATA_ENTITY_TYPE = "pc_seed_data";
  public static final String SEED_DATA_ATTRIBUTE = "seed_data";
  public static final String SEED_DATA_LAST_UPDATE_ATTRIBUTE =
      "last_modified_timestamp_usecs";

  // Object store metadata constants
  public static final String OBJECTSTORE_PC_METADATA_BASE_PREFIX = "pc_backup/";
  public static final String OBJECTSTORE_PC_METADATA_PREFIX = "pc_metadata";
  public static final String OBJECTSTORE_PC_METADATA_SPLIT_REGEX = "!*";
  public static final String BUCKET_ACCESS = "bucket_access";
  public static final String BUCKET_POLICY = "bucket_policy";
  public static final String OBJECT_LOCK = "object_lock";
  public static final int BUCKET_VALIDATION_THREAD_COUNT = 3;
  public static final String CREDENTIAL_KEY_ID = "credentials_key_id";
  public static final String ENABLED_STR = "Enabled";
  public static final String PAUSE_BACKUP_ATTRIBUTE = "pause_backup";
  public static final String PAUSE_BACKUP_MESSAGE_ATTRIBUTE =
      "pause_backup_message";
  public static final String VM_TABLE_GROUPS = "mh_vm" ;
  public static final String VIRTUAL_NIC_TABLE_GROUPS = "virtual_nic" ;
  public static final String  IDF_NOT_FOUND = "%s not found in the idf.";
  public static final String PCBR_SUBJECT_NAME = "ntnx.prism.adonis.pcbr.v4_async_api";
  public static final String PCBR_SUBJECT_DURABLE_KEY= "DURABLE_PCBR_ASYNC_SUBN_NAME";

  public static final String CREATE_BACKUP_TARGET = "Create Backup Target";
  public static final String UPDATE_BACKUP_TARGET = "Update Backup Target";
  public static final String DELETE_BACKUP_TARGET = "Delete Backup Target";
  public static final String DELETE_BACKUP_TARGET_OPERATION_TYPE = "kDeleteBackupTarget";
  public static final String PCBR_QUEUE_NAME = "processor-pcbr-queue-group";
  public static final String IS_PAGINATED = "isPaginated";
  public static final String IS_TRUNCATED = "isTruncated";
  public static final String HAS_ERROR = "hasError";
  public static final String SELF_LINK_RELATION = "self";
  public static final String TASK_LINK_RELATION = "task-created";
  public static final String SERVICE_INFO = "ServiceInfo";
  public static final String SERVICE_NAME = "ServiceName";
  public static final String SERVICE_VERSION = "ServiceVersion";
  public static final String PORTFOLIO_SERVICE_ALERT_MSG = "The following services were not started. Please follow " +
          "the given KBs to restore the services to their right versions and then start them";

  public static final String DEFAULT_ACCEPT_LANGUAGE_HEADER = "en-US";
}
