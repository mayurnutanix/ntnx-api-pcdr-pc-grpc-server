package com.nutanix.prism.pcdr.restserver.util;

import com.nutanix.prism.pcdr.PcBackupSpecsProto.PCVMFiles;
import com.nutanix.prism.pcdr.exceptions.PCResilienceException;
import com.nutanix.prism.pcdr.restserver.adapters.impl.PCDRYamlAdapterImpl.PathNode;
import com.nutanix.prism.pcdr.util.FileUtil;
import lombok.Generated;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.nutanix.prism.pcdr.restserver.constants.Constants.FILE_EXISTS_TEXT;
import static com.nutanix.prism.pcdr.restserver.constants.Constants.FILE_EXIST_TIMEOUT_MILLISECONDS;

@Slf4j
@Component
public class PCVMFileHelper {

  private final FileUtil fileUtil;
  private ExecutorService adonisServiceThreadPool;

  @Autowired
  public PCVMFileHelper(FileUtil fileUtil,@Qualifier("adonisServiceThreadPool") ExecutorService adonisServiceThreadPool) {
    this.fileUtil = fileUtil;
    this.adonisServiceThreadPool = adonisServiceThreadPool;
  }

  /**
   * Based on the paths given for the files, retrieve the file content and
   * enter it into the proto PCVMFiles.
   * @param pathNodes - path of files.
   * @param keyId - key id for encryption/decryption
   * @param encryptionVersion - version of encryption
   * @return - PCVMFiles object containing the path and content of file.
   */
  public PCVMFiles constructPCVMFilesProto(final List<PathNode> pathNodes,
                                           String keyId,
                                           String encryptionVersion) {
    // Retrieving the pathNodes with actual path using async parallel calls.
    final List<PathNode> pathNodesWithActualPath =
      getPathNodesWithActualPath(pathNodes);
    log.debug("Path nodes with actual paths: {}", pathNodesWithActualPath);
    // Retrieving pcvmFiles object with updated file content using async
    // parallel calls.
    return getPcvmFilesFromPathNodesContainingActualPath(
      pathNodesWithActualPath, keyId);
  }

  /**
   * Based on the given paths(including the regex one) fetch the actual path
   * from the regex path.
   * @param pathNodes - List of PathNode objects
   * @return - returns a list of pathNode object containing actual paths.
   */
  private List<PathNode> getPathNodesWithActualPath(
    final List<PathNode> pathNodes) {
    List<CompletableFuture<Boolean>> allFuturesPaths = new ArrayList<>();
    log.debug("Initialised a synchronizedList.");
    final List<PathNode> syncPathNodes =
      Collections.synchronizedList(new ArrayList<>());
    for (final PathNode node : pathNodes) {
      // public nodes are not encrypted.
      allFuturesPaths.add(CompletableFuture.supplyAsync(
        () -> getFilesPath(node), adonisServiceThreadPool).thenApply(paths -> {
        paths.forEach(path -> {
          PathNode pathNodeWithActualPath = new PathNode();
          pathNodeWithActualPath.setSecure(node.getSecure());
          pathNodeWithActualPath.setPath(path.toString());
          synchronized (syncPathNodes) {
            syncPathNodes.add(pathNodeWithActualPath);
            log.debug("Successfully added {} in syncPathNodes",
              pathNodeWithActualPath);
          }
        });
        return true;
      }));
    }
    // Join so that it waits for all the task to complete.
    CompletableFuture.allOf(allFuturesPaths.toArray(
      new CompletableFuture[0])).join();
    return syncPathNodes;
  }

  /**
   * Returns the file path list for the given pathNode.
   * @param node - PathNode object
   * @return - returns a list of paths for the given pathNode.
   */
  private List<Path> getFilesPath(final PathNode node) {
    try {
      // Will help us skip the heavy I/O operation if we will know that
      // whether a regex is defined by default or not.
      if (node.getRegex()!= null && node.getRegex()) {
        log.debug("Get files from regex file name called.");
        return fileUtil.getFilesFromRegexFileName(node.getPath());
      } else {
        // It will verify that the path present is a valid path.
        Path filePath = Paths.get(node.getPath());
        if (fileExists(filePath)) {
          return Collections.singletonList(filePath);
        }
        else {
          log.warn("The file {} is not present on the system.",
                   filePath);
        }
      }
    } catch (IOException e) {
      log.error("Input/Output exception encountered while finding files " +
        "using regex: ", e);
    }
    return Collections.emptyList();
  }

  /**
   * Gets the list of pathNodes containing actual paths.
   * @param pathNodes - list of pathNode objects containing valid path.
   * @return - returns a PCVMFiles object.
   */
  private PCVMFiles getPcvmFilesFromPathNodesContainingActualPath(
    final List<PathNode> pathNodes, String keyId) {
    final List<PCVMFiles.PCVMFile.Builder> pcvmFileBuilderList =
      new ArrayList<>();
    List<CompletableFuture<Boolean>> allFuturesContent = new ArrayList<>();
    for (final PathNode pathNode : pathNodes) {
      final PCVMFiles.PCVMFile.Builder pcvmFileBuilder =
        PCVMFiles.PCVMFile.newBuilder();
      pcvmFileBuilder.setEncrypted(pathNode.getSecure());
      pcvmFileBuilder.setFilePath(pathNode.getPath());
      pcvmFileBuilderList.add(pcvmFileBuilder);
      log.debug("Adding pcvmFileBuilder with path {} to allFuturesContent",
        pathNode);
      allFuturesContent.add(setFileContentInPcvmFileBuilder(
        pcvmFileBuilder, pathNode, keyId));
    }
    CompletableFuture.allOf(allFuturesContent.toArray(
      new CompletableFuture[0])).join();
    log.debug("Shutting down the pool for" +
      " getPcvmFilesFromPathNodesContainingActualPath");
    return PCVMFiles.newBuilder().addAllPcvmFiles(pcvmFileBuilderList.stream()
      .map(PCVMFiles.PCVMFile.Builder::build)
      .collect(Collectors.toList())).build();
  }

  private CompletableFuture<Boolean> setFileContentInPcvmFileBuilder(
    final PCVMFiles.PCVMFile.Builder pcvmFileBuilder, final PathNode pathNode,
    String keyId) {
    return CompletableFuture.supplyAsync(() -> {
      try {
        pcvmFileBuilder.setFileContent(
          fileUtil.retrieveFileContent(
            Paths.get(pathNode.getPath()),
            pathNode.getSecure(), keyId
          )
        );
        log.debug("Setting the content of the file in pcvmFileBuilder is a " +
          "success for path {}.", pathNode);
      } catch (IOException | PCResilienceException | InterruptedException e) {
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
        // Some of the errors can be ignored, but better be checked.
        log.error("Exception encountered while reading file " +
          "content: ", e);
      }
      return true;
    }, adonisServiceThreadPool);
  }

  /**
   * This method is responsible for checking whether the file exists or not.
   * @param path - path of the file
   * @return - returns true if file exists else false
   * @throws IOException - can throw IOException.
   */
  public boolean fileExists(Path path) throws IOException {
    File file = path.toFile();
    if (file.exists()) {
      return true;
    } else {
      Process process = runTestFileExists(path.toString());
      try {
        if (process.waitFor(FILE_EXIST_TIMEOUT_MILLISECONDS,
                            TimeUnit.MILLISECONDS)) {
          BufferedReader bufferedReader =
              new BufferedReader(
                  new InputStreamReader(process.getInputStream()));
          String output = bufferedReader.readLine();
          return !StringUtils.isEmpty(output) &&
                 FILE_EXISTS_TEXT.compareTo(output) == 0;
        } else {
          log.warn("Unable to check whether the file {} exists or not, " +
                   "checking the file took more than {} milliseconds.",
                   path, FILE_EXIST_TIMEOUT_MILLISECONDS);
        }
      }
      catch (Exception e) {
        log.error("Unable to check whether {} file exists or not.", path, e);
      }
    }
    return false;
  }

  /**
   * Run process builder for checking if file exists command on bash.
   * @return - returns the process
   * @throws IOException - can raise IOException
   */
  @Generated
  public Process runTestFileExists(String filePath) throws IOException {
    String command = String.format("sudo test -f %s && echo '%s'", filePath,
                                   FILE_EXISTS_TEXT);
    ProcessBuilder processBuilder = new ProcessBuilder();
    processBuilder.command("bash", "-c", command);
    log.info("Invoking 'sudo test -f {}' command to check if file exists.",
             filePath);
    return processBuilder.start();
  }
}
