package com.nutanix.prism.pcdr.restserver.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * This class helps in tracking all the feature flags and other flags which
 * are being used at multiple places in PCDR.
 * Use the getter methods in this class to get the value of whether a feat is
 * enabled or not, or check the value of a flag defined.
 */
@Slf4j
@Component
public class FlagHelperUtil {
  // This flag is responsible for enabling/disabling the backupLimit feat.
  private final boolean isBackupLimitFeatEnabled;

  public FlagHelperUtil(@Value("${prism.pcdr.feat.isBackupLimitEnabled:true}")
                            boolean isBackupLimitFeatEnabled) {
    this.isBackupLimitFeatEnabled = isBackupLimitFeatEnabled;
  }

  /**
   * Is backup limit feature enabled? It returns the current value defined
   * for the feature flag.
   * @return - returns boolean.
   */
  public boolean isBackupLimitFeatEnabled() {
    return isBackupLimitFeatEnabled;
  }
}
