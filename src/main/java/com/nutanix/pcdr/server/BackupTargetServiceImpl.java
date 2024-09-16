package com.nutanix.pcdr.server;

import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import proto3.prism.v4.management.BackupTarget;
import proto3.prism.v4.management.DomainManagerBackupsServiceGrpc;

@Slf4j
@GrpcService
public class BackupTargetServiceImpl extends DomainManagerBackupsServiceGrpc.DomainManagerBackupsServiceImplBase {

    @Override
    public void createBackupTarget(proto3.prism.v4.management.CreateBackupTargetArg request,
                                   io.grpc.stub.StreamObserver<proto3.prism.v4.management.CreateBackupTargetRet> responseObserver) {

      BackupTarget backupTarget = request.getBody();
      log.info("payload is :" + backupTarget);
    }

}
