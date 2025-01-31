package com.nutanix.prism.pcdr.restserver.util;

import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import dp1.pri.prism.v4.management.BackupTarget;

public interface BackupTargetValidator {

    void validateBackupTarget(BackupTarget backupTarget) throws PCResilienceException;
}
