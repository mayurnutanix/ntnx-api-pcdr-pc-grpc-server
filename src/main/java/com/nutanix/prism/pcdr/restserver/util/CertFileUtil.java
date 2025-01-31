package com.nutanix.prism.pcdr.restserver.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.nutanix.api.utils.json.JsonUtils;
import com.nutanix.prism.pcdr.dto.ObjectStoreEndPointDto;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorCode;
import com.nutanix.prism.pcdr.exceptions.v4.ErrorMessages;
import com.nutanix.prism.pcdr.proxy.InstanceServiceFactory;
import com.nutanix.prism.pcdr.util.ZookeeperServiceHelperPc;
import com.nutanix.prism.pcdr.util.ZookeeperUtil;
import dp1.pri.prism.v4.protectpc.PcvmRestoreFile;
import dp1.pri.prism.v4.protectpc.PcvmRestoreFiles;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.constants.Constants.*;

@Slf4j
@Service
public class CertFileUtil {

  @Autowired
  private InstanceServiceFactory instanceServiceFactory;

  @Autowired
  @Qualifier("adonisServiceThreadPool")
  private ExecutorService adonisServiceThreadPool;

  @Autowired
  private PCRetryHelper pcRetryHelper;

  @Autowired
  private ZookeeperServiceHelperPc zookeeperServiceHelper;

  /**
   * function used internally in this class functions, to copy files on all PCVMs
   * @param pcvmRestoreFiles sdk object to be passed to files api
   * @param shouldFailIfAtLeastOneCopyFails true if fail and raise exception if at least one copy fails
   * @throws PCResilienceException
   */
  private void createCertFileOnAllPCVM(PcvmRestoreFiles pcvmRestoreFiles, List<String> backupUuids,
                                       boolean shouldFailIfAtLeastOneCopyFails)
      throws PCResilienceException {
    List<String> svmIps = new ArrayList<>(ZookeeperUtil.getSvmIpVmUuidMap(
        instanceServiceFactory.getZeusConfig().getAllNodes()).values());

    if (svmIps.size() > 1) {
      // scaleout PC
      createCertFileOnGivenPCVMs(pcvmRestoreFiles, svmIps, backupUuids, shouldFailIfAtLeastOneCopyFails);
    } else {
      // single PCVM always fail with exception if unable to create cert file
      createCertFileOnGivenPCVMs(pcvmRestoreFiles, svmIps, backupUuids, true);
    }
  }

  /**
   * function used internally in this class functions, to copy files on given list of PCVMs
   * Creates zk node marking IP of PCVM on which copy failed.
   * If able to copy on all given PCVMs then delete corresponding zk node
   * @param pcvmRestoreFiles sdk object to be passed to files api
   * @param svmIps List of string of IPs on which to copy files
   * @param shouldFailIfAtLeastOneCopyFails true if fail and raise exception if at least one copy fails
   * @throws PCResilienceException
   */
  private void createCertFileOnGivenPCVMs(PcvmRestoreFiles pcvmRestoreFiles, List<String> svmIps,
                                      List<String> backupUuids, boolean shouldFailIfAtLeastOneCopyFails)
      throws PCResilienceException {

    log.info(String.format("List of svm-ips for on which Objects certs file will be created: %s for backup target with uuid %s",
        svmIps, backupUuids));
    List<CompletableFuture<Object>> allFutures = new ArrayList<>();
    for (String svmIp : svmIps) {
      log.info("Making request for creating Objects cert file on SVM IP {}", svmIp);
      // The files data needs to be base64 encoded
      allFutures.add(CompletableFuture
          .supplyAsync(() -> pcRetryHelper
              .restoreFilesApiRetryable(svmIp,
                  pcvmRestoreFiles), adonisServiceThreadPool)
          .handle((result, ex) -> {
            if (ex != null) {
              log.error("Unable to write files due to the" +
                  " following exception:", ex);
              return false;
            }
            // Result is null in this scenario
            // cause 204(NO_CONTENT) request.
            return result;
          }));
    }
    CompletableFuture.allOf(allFutures.toArray(
        new CompletableFuture[0])).join();
    int ipReached = 0;
    List<String> errorList = new ArrayList<>();
    List<String> errorSvmIps = new ArrayList<>();
    for (CompletableFuture<Object> restoreFilesApiResponse : allFutures) {
      try {
        if (restoreFilesApiResponse.get() != null) {
          errorList.add(String.format("Unable to create Objects cert " +
              "file on the svmIP %s", svmIps.get(ipReached)));
          errorSvmIps.add(svmIps.get(ipReached));
        }
      } catch (ExecutionException | InterruptedException e) {
        throw new PCResilienceException(ErrorMessages.CERTIFICATE_COPY_ON_ALL_PCVM_ERROR,
            ErrorCode.PCBR_CERTIFICATE_COPY_ON_ALL_PCVM_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
      }
      ipReached++;
    }

    List<String> successfulSvmIps = new ArrayList<>(svmIps);
    if (!errorList.isEmpty()) {
      // Raise exception if we want to fail api if copy of certs on one svmIp fails (in case of restore)
      // or if failure occurred on PCVMs more than or equal to quorum (2)
      if (shouldFailIfAtLeastOneCopyFails || ((NUMBER_OF_PCVM_IN_SCALEOUT - errorList.size()) < CERT_CREATE_QUORUM)) {
        log.info(errorList.toString());
        throw new PCResilienceException(ErrorMessages.CERTIFICATE_COPY_ON_ALL_PCVM_ERROR,
            ErrorCode.PCBR_CERTIFICATE_COPY_ON_ALL_PCVM_ERROR, HttpStatus.INTERNAL_SERVER_ERROR);
      }
      successfulSvmIps = svmIps.stream()
          .filter(item -> !errorSvmIps.contains(item))  // Remove elements in removeList
          .collect(Collectors.toList());
    }
    log.info("Successfully created Objects cert file on SvmIPs: {}", successfulSvmIps);
    // Create or Update corresponding zk nodes if present
    this.updateRequiredZkNodeForCertCopyFail(backupUuids, errorSvmIps);
  }

  public void createCertFileOnAllPCVM(List<ObjectStoreEndPointDto> objectStoreEndPointDtoList, boolean shouldFailIfAtLeastOneCopyFails)
      throws PCResilienceException {
    if (ObjectUtils.isEmpty(objectStoreEndPointDtoList)) {
      return;
    }
    PcvmRestoreFiles pcvmRestoreFiles = new PcvmRestoreFiles();
    List<PcvmRestoreFile> pcvmRestoreFileList = new LinkedList<>();
    for (ObjectStoreEndPointDto objectStoreEndPointDto : objectStoreEndPointDtoList) {
      if (ObjectUtils.isEmpty(objectStoreEndPointDto.getCertificatePath())) {
        continue;
      }
      PcvmRestoreFile pcvmRestoreFile = new PcvmRestoreFile();
      pcvmRestoreFile.setFileContent(
          ByteString.copyFrom(
              Base64.getEncoder().encode(objectStoreEndPointDto.getCertificateContent().getBytes())
          ).toStringUtf8()
      );
      pcvmRestoreFile.setFilePath(objectStoreEndPointDto.getCertificatePath());
      pcvmRestoreFile.setIsEncrypted(false);
      pcvmRestoreFile.setKeyId("");
      pcvmRestoreFile.setEncryptionVersion("");
      pcvmRestoreFileList.add(pcvmRestoreFile);
    }
    pcvmRestoreFiles.setFileList(pcvmRestoreFileList);

    List<String> backupUuids = objectStoreEndPointDtoList.stream()
            .map(ObjectStoreEndPointDto::getBackupUuid)
                .collect(Collectors.toList());

    createCertFileOnAllPCVM(pcvmRestoreFiles, backupUuids, shouldFailIfAtLeastOneCopyFails);

  }

  public void createCertFileOnPCVMs(ObjectStoreEndPointDto objectStoreEndPointDto, List<String> svmIps,
                                    boolean shouldFailIfAtLeastOneCopyFails)
      throws PCResilienceException {
    if (ObjectUtils.isEmpty(objectStoreEndPointDto) || ObjectUtils.isEmpty(svmIps)) {
      return;
    }
    PcvmRestoreFiles pcvmRestoreFiles = new PcvmRestoreFiles();
    List<PcvmRestoreFile> pcvmRestoreFileList = new LinkedList<>();
    if (ObjectUtils.isEmpty(objectStoreEndPointDto.getCertificatePath())) {
      return;
    }
    PcvmRestoreFile pcvmRestoreFile = new PcvmRestoreFile();
    pcvmRestoreFile.setFileContent(
        ByteString.copyFrom(
            Base64.getEncoder().encode(objectStoreEndPointDto.getCertificateContent().getBytes())
        ).toStringUtf8()
    );
    pcvmRestoreFile.setFilePath(objectStoreEndPointDto.getCertificatePath());
    pcvmRestoreFile.setIsEncrypted(false);
    pcvmRestoreFile.setKeyId("");
    pcvmRestoreFile.setEncryptionVersion("");
    pcvmRestoreFileList.add(pcvmRestoreFile);
    pcvmRestoreFiles.setFileList(pcvmRestoreFileList);
    createCertFileOnGivenPCVMs(pcvmRestoreFiles, svmIps,
        Collections.singletonList(objectStoreEndPointDto.getBackupUuid()),
        shouldFailIfAtLeastOneCopyFails);
  }

  private void updateRequiredZkNodeForCertCopyFail(List<String> backupUuids, List<String> svmIps) throws PCResilienceException {
    String backupCertCopyFailedZkNode = null;
    // creating zk node with necessary information of failed ip list
    for (String backupUuid : backupUuids) {
      try {
        ObjectMapper objectMapper = JsonUtils.getObjectMapper();
        byte[] data = objectMapper.writeValueAsBytes(svmIps);
        backupCertCopyFailedZkNode = String.format("%s/%s",CERTS_COPY_FAILED_ZK_NODE, backupUuid);
        Stat stat = zookeeperServiceHelper.exists(backupCertCopyFailedZkNode, false);
        if (svmIps.isEmpty()) {
          // svmIps with error not present.
          // Two cases if zk node exist then delete it, else do nothing
          log.info(String.format("Able to copy certs on all required svmIps for backup target with uuid %s. Deleting zk node %s if exists.",
              backupUuid, backupCertCopyFailedZkNode));
          if (!ObjectUtils.isEmpty(stat)) {
            zookeeperServiceHelper.delete(backupCertCopyFailedZkNode, -1);
          }
        } else if (stat == null) {
          // zknode not exist and svmIps with error present
          log.info("{} zkNode does not exists so creating the corresponding zknode", backupCertCopyFailedZkNode);
          PCUtil.createAllParentZkNodes(backupCertCopyFailedZkNode, zookeeperServiceHelper);
          zookeeperServiceHelper.createZkNode(backupCertCopyFailedZkNode,
              data,
              ZooDefs.Ids.OPEN_ACL_UNSAFE,
              CreateMode.PERSISTENT);
        } else {
          // zk node already exists and svmIps with error present so update the zk node.
          log.info("{} zkNode already exists, so updating the" +
              " existing zkNode", backupCertCopyFailedZkNode);
          zookeeperServiceHelper.setData(backupCertCopyFailedZkNode,
              data,
              stat.getVersion());
        }
      } catch (Exception e) {
        log.error(String.format("Error encounter while setting up %s " +
            "zkNode:", backupCertCopyFailedZkNode), e);
        throw new PCResilienceException(ErrorMessages.INTERNAL_SERVER_ERROR);
      }
    }
  }

  public boolean validateCertContent(String certContent) {
    boolean isValidCerts = true;
    // for removing ^M
    certContent = certContent.replace("\r", "");
    return isValidCerts;
  }

}
