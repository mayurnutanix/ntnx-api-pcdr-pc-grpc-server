package com.nutanix.prism.pcdr.restserver.util;

import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCodeArgumentMapper;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.restserver.converters.PrismCentralBackupConverter;
import com.nutanix.prism.pcdr.restserver.services.api.PCBackupService;
import dp1.pri.prism.v4.management.BackupTarget;
import dp1.pri.prism.v4.protectpc.BackupTargetsInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class BackupTargetUtil {

  private PCBackupService pcBackupService;

  private PrismCentralBackupConverter prismCentralBackupConverter;

  public BackupTargetUtil(PCBackupService pcBackupService, PrismCentralBackupConverter prismCentralBackupConverter) {
    this.pcBackupService = pcBackupService;
    this.prismCentralBackupConverter = prismCentralBackupConverter;
  }

  public BackupTarget getBackupTarget(String extId) throws PCResilienceException {
    BackupTargetsInfo backupTargetsInfo = pcBackupService.getReplicas();
    List<BackupTarget> backupTargetList = prismCentralBackupConverter.
            getListBackupTargetFromBackupTargetsInfo(backupTargetsInfo);
    Optional<BackupTarget> result =
            backupTargetList.stream().filter(backupTarget -> org.apache.commons.lang3.StringUtils.equals(extId, backupTarget.getExtId()))
                    .findFirst();
    if (!result.isPresent()) {
      Map<String, String> errorArguments = new HashMap<>();
      errorArguments.put(ErrorCodeArgumentMapper.ARG_BACKUP_TARGET_EXT_ID, extId);
      throw new PCResilienceException(ErrorMessages.BACKUP_TARGET_WITH_ENTITY_ID_NOT_FOUND,
              ErrorCode.PCBR_BACKUP_TARGET_NOT_FOUND, HttpStatus.NOT_FOUND,errorArguments);
    }
    return result.get();
  }
}
