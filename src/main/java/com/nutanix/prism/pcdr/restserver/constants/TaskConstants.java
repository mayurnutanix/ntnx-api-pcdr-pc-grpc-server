package com.nutanix.prism.pcdr.restserver.constants;

public class TaskConstants {
  private TaskConstants() {}

  public enum TaskOperationType {
    kCleanIDF("Clean stale database values.",
              2,
              "clean_stale_data"),
    kInitialised("PC Restore task initialised.",
                 -1,
                 ""),
    kNoTaskFound("No PC Restore task found.",
                 -1,
                 ""),
    kPCRestore("Prism Central Restore.",
               8,
               ""),
    kRestoreBasicTrust("Restore basic trust.",
                       3,
                       "restore_basic_trust"),
    kRestoreDataAndReconcile("Restore Prism Central data and reconcile.",
                             7,
                             "restore_zk_and_files_data"),
    kRestoreIDF("Restore database.",
                3,
                "data_restore"),
    kRestoreTaskCompleted("PC restore task completed.",
                          -1,
                          ""),
    kResumeBackup("Resume backup on restored Prism Central.",
                  2,
                  "resume_backup"),
    kIamRestore("Restore IAM on restored Prism Central.",
                1,
                "restore_iam"),
    kResetArithmosSync("Finalizing configurations on restored Prism Central.",
                       2,
                       "reset_arithmos_sync"),
    kRestartPcServices("Restarting services on Prism Central",
                       3,
                       "restart_pc_services");

    String taskName;
    String completedZkName;
    int totalTaskStep;

    TaskOperationType(String taskName, int totalTaskStep,
                      String completedZkName) {
      this.taskName = taskName;
      this.totalTaskStep = totalTaskStep;
      this.completedZkName = completedZkName;
    }

    public String getTaskName() {
      return taskName;
    }

    public int getTotalTaskStep() {
      return totalTaskStep;
    }

    public String getCompletedZkName() {
      return completedZkName;
    }
  }

  public enum CMSPTaskState {
    SUCCEEDED,
    QUEUED,
    RUNNING,
    FAILED,
    ROLLING_BACK,
    CANCELED,
    ABORTED
  }

  public static final int STORE_HOSTING_PE_CA_CERT_STEP_PERCENTAGE = 5;
  public static final int RESTORE_HOSTING_PE_CA_CERT_STEP_PERCENTAGE = 60;

  // Percentage complete constants for tasks in PC restore tasks at PC
  public static final int FETCH_TRUST_DATA_PERCENTAGE = 30;
  public static final int WRITE_TRUST_DATA_PERCENTAGE = 80;
  public static final int RESTART_SERVICES_AFTER_TRUST_DATA_PERCENTAGE = 100;
  public static final int BASIC_TRUST_ALL_STEPS_COMPLETE_PERCENTAGE = 100;

  public static final int RESTORE_IDF_API_PERCENTAGE = 40;
  public static final int RESTORE_IDF_WAIT_PERCENTAGE = 100;
  public static final int RESTORE_IAM_WAIT_PERCENTAGE = 100;

  // RestoreAndReconsileSubtask steps completion percentages
  public static final int RESTORE_ZK_NODES_AND_FILES_PERCENTAGE = 50;
  public static final int REMOVE_PE_TRUST_PERCENTAGE = 70;
  public static final int VERIFY_PRISM_DATA_PERCENTAGE = 80;
  public static final int APPLY_LIMITS_OVERRIDE = 90;
  public static final int RECONCILE_LCM_SERVICES_OVERRIDE = 100;

  // RestartServicesSubtask steps completion percentages
  public static final int DELETE_SERVICES_ZK_NODE = 35;
  public static final int ENABLE_CMSP_BASED_SERVICES = 70;
  public static final int RESTART_SERVICES_AFTER_RESTORE = 100;

  // ResetArithmosSyncSubtask steps completion percentages
  public static final int RECONCILE_WAIT_PERCENTAGE = 50;
  public static final int RESET_ARITHMOS_SYNC_PERCENTAGE = 100;

  public static final int CLEAN_IDF_API_PERCENTAGE = 50;
  public static final int CLEAN_IDF_PRISM_CENTRAL_STALE_VALUES = 100;

  public static final int RESTORE_REPLICAS_STEP_PERCENTAGE = 70;
  public static final int RESUME_BACKUP_SCHEDULER_STEP_PERCENTAGE = 100;

  // Percentage for root PC restore task.
  public static final int BASIC_TRUST_PERCENTAGE = 10;
  public static final int RESTORE_IDF_PERCENTAGE = 50;
  public static final int CLEAN_IDF_PERCENTAGE = 60;
  public static final int RESTORE_AND_RECONCILE_PERCENTAGE = 75;
  public static final int IAM_RESTORE_PERCENTAGE  = 85;
  public static final int RESTART_SERVICES_PERCENTAGE  = 90;
  public static final int RESET_ARITHMOS_SYNC_TASK_PERCENTAGE = 95;
  public static final int RESUME_BACKUP_PERCENTAGE = 100;

}
