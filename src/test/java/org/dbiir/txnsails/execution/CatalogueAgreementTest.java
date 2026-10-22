package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.dbiir.txnsails.analysis.ClosedWorkload;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.common.messages.*;
import org.dbiir.txnsails.execution.transaction.*;
import org.dbiir.txnsails.execution.validation.CertificationContext;
import org.dbiir.txnsails.worker.MetaWorker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CatalogueAgreementTest {
  @TempDir Path directory;

  private WorkloadConfiguration configuration(int id, boolean closed) throws Exception {
    WorkloadConfiguration configuration = new WorkloadConfiguration();
    configuration.setInstanceID(id);
    configuration.setInstances(
        List.of(
            new DBInstance(0, "jdbc:postgresql://localhost/a", "localhost", "", ""),
            new DBInstance(1, "jdbc:postgresql://localhost/b", "localhost", "", "")));
    configuration.setTableName2DbList(Map.of("usertable", List.of(0, 1)));
    if (closed) {
      SchemaInfo schema = new SchemaInfo("config/ycsb.sql");
      MetaWorker.getINSTANCE().setSchema(schema);
      Path file = directory.resolve("workload.xml");
      Files.writeString(
          file,
          """
          <workload version="1" closedWorld="true">
            <program name="read_pair">
              <statement id="0" operation="read" relation="usertable"><sql>SELECT field1 FROM usertable WHERE ycsb_key = ?</sql></statement>
            </program>
          </workload>
          """);
      configuration.setClosedWorkload(ClosedWorkload.load(file, schema, configuration));
    }
    return configuration;
  }

  @Test
  void emptyValidationRequestsMustCarryTheOwnersCatalogue() throws Exception {
    WorkloadConfiguration configuration = configuration(1, true);
    String hash = configuration.getClosedWorkload().fingerprint();
    TxnSailsRPCServer server = new TxnSailsRPCServer(0, configuration);
    Thread thread = new Thread(server);
    thread.start();
    try (TxnSailsRPCClient client =
        new TxnSailsRPCClient(configuration.getInstances().get(1), server.getPort())) {
      CatalogueResponse agreement = (CatalogueResponse) client.request(new CatalogueRequest());
      assertEquals(1, agreement.getInstanceID());
      assertEquals(hash, agreement.getFingerprint());
      assertNotNull(
          ((ValidationResponse)
                  client.request(new ValidationRequest(1, List.of(), CertificationContext.FULL)))
              .getException());
      assertNotNull(
          ((ValidationResponse)
                  client.request(
                      new ValidationRequest(1, List.of(), CertificationContext.FULL, "wrong")))
              .getException());
      assertNull(
          ((ValidationResponse)
                  client.request(
                      new ValidationRequest(1, List.of(), CertificationContext.FULL, hash)))
              .getException());
      assertNull(
          ((FinalResponse) client.request(new FinalRequest(1, false, List.of()))).getException());
    } finally {
      server.closeServer();
      thread.join(2000);
    }
  }

  @Test
  void managerChecksEveryOwnerAndRejectsMixedDeploymentBeforeBeginning() throws Exception {
    WorkloadConfiguration local = configuration(0, true);
    TxnSailsRPCServer remote = new TxnSailsRPCServer(0, configuration(1, false));
    Thread remoteThread = new Thread(remote);
    remoteThread.start();
    local.getInstances().get(0).setRpcPort(0);
    local.getInstances().get(1).setRpcPort(remote.getPort());
    TransactionManager manager = new TransactionManager();
    manager.init(local);
    try {
      assertThrows(java.sql.SQLException.class, manager::ensureCatalogueAgreement);
      assertThrows(java.sql.SQLException.class, manager::ensureCatalogueAgreement);
    } finally {
      manager.close();
      remote.closeServer();
      remoteThread.join(2000);
    }
  }
}
