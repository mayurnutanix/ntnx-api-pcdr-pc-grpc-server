package com.nutanix.prism.pcdr.restserver.util;

import com.nutanix.alerts.notifications.Notification;
import com.nutanix.alerts.notifications.NotificationsDef;
import com.nutanix.alerts.notifications.Notifier;
import com.nutanix.insights.exception.InsightsInterfaceException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;

@Slf4j
@Service
public class AlertUtil {
  @Value("${prism.pcdr.clusterHealthHostIp:127.0.0.1}")
  private String clusterHealthHostIp;

  private Notifier notifier;

  @PostConstruct
  public void init(){
    notifier =  new com.nutanix.alerts.notifications.RpcBasedClusterHealthNotifier
            .Builder("PCDR").clusterHealthHostIp(clusterHealthHostIp).build();
  }

  /**
   * The method gets all the PEs on which backup Limit is exceeded.
   * @param backupLimitExceededPes - Pes on which backup Limit exceeded.
   * @return - returns boolean indication whether an alert was raised or not.
   * @throws InsightsInterfaceException - can throw insights_interface
   * exception.
   */
  public void raiseAlertAsBackupLimitExceeded(
      List<String> backupLimitExceededPes){
    // PcBackupLimitCheck proto is defined in serviceability branch
    // notifications.def file.
    // And the config file which is required for this is defined with
    // PcBackupLimitCheck.json file in master repo.
    Notification notification = new NotificationsDef.PcBackupLimitCheck(
        backupLimitExceededPes.toString());
    boolean status = notifier.notify(notification);
    if(!status){
      log.warn("unable to raise an alert even the backup limit exceeded " +
               "on these Pes" + backupLimitExceededPes);
    }
  }


  public void raiseAlertForLCMVersionMismatch(String versionMismatchAlertMsg){
    // PcRestoreServiceVersionMismatchCheck proto is defined in serviceability branch
    // notifications.def file.
    // And the config file which is required for this is defined with
    // PcRestoreServiceVersionMismatchCheck.json file in master repo.
    Notification notification = new NotificationsDef.PcBackupLimitCheck(
        versionMismatchAlertMsg);
    /*Notification notification = new NotificationsDef.PcRestoreServiceVersionMismatchCheck(
        versionMismatchAlertMsg);*/
    boolean status = notifier.notify(notification);
    if(!status){
      log.warn("unable to raise an alert for version mismatch for LCM services" + versionMismatchAlertMsg);
    }
  }
}
