package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.common.TransactionIdentity;
import org.dbiir.txnsails.common.messages.*;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.transaction.*;
import org.dbiir.txnsails.execution.validation.*;
import org.junit.jupiter.api.Test;

class DistributedRPCTest {
  @Test
  void equalLocalSequencesOnDifferentCoordinatorsHaveIndependentRpcState() throws Exception {
    var table = ValidationMetaTable.getInstance();
    table.initDistributed("ycsb", List.of());
    var server = new TxnSailsRPCServer(0, new WorkloadConfiguration());
    var thread = new Thread(server);
    thread.start();
    var owner = new DBInstance(0, "", "localhost", "", "");
    try (var client = new TxnSailsRPCClient(owner, server.getPort())) {
      int[] coordinators = {0, 256, Integer.MAX_VALUE};
      for (int i = 0; i < coordinators.length; i++) {
        var identity = new TransactionIdentity(coordinators[i], 17);
        var items = List.of(new ValidationItem("usertable", 7 + i, LockType.EX, 1));
        assertNull(
            ((ValidationResponse)
                    client.request(
                        new ValidationRequest(identity, items, CertificationContext.FULL, null)))
                .getException());
      }
      // Finalizing coordinator 256 cannot publish or release coordinators 0 or MAX_VALUE.
      assertNull(
          ((FinalResponse)
                  client.request(
                      new FinalRequest(new TransactionIdentity(256, 17), true, List.of())))
              .getException());
      assertEquals(1, table.getDistributedVersion("usertable", "8"));
      assertEquals(-1, table.getDistributedVersion("usertable", "7"));
      assertEquals(-1, table.getDistributedVersion("usertable", "9"));
      var collision = new TransactionIdentity(256, 18);
      assertNotNull(
          ((ValidationResponse)
                  client.request(
                      new ValidationRequest(
                          collision,
                          List.of(new ValidationItem("usertable", 7, LockType.EX, 2)),
                          CertificationContext.FULL,
                          null)))
              .getException());
      client.request(new FinalRequest(collision, false, List.of()));
      client.request(new FinalRequest(new TransactionIdentity(0, 17), false, List.of()));
      client.request(
          new FinalRequest(new TransactionIdentity(Integer.MAX_VALUE, 17), true, List.of()));
      assertEquals(-1, table.getDistributedVersion("usertable", "7"));
      assertEquals(1, table.getDistributedVersion("usertable", "9"));
    } finally {
      server.closeServer();
      thread.join(2000);
    }
  }

  @Test
  void replayWatermarksUseFullCoordinatorIdsAndRetainActiveOldSequences() throws Exception {
    ValidationMetaTable.getInstance().initDistributed("ycsb", List.of());
    var server = new TxnSailsRPCServer(0, new WorkloadConfiguration());
    var thread = new Thread(server);
    thread.start();
    var owner = new DBInstance(0, "", "localhost", "", "");
    try (var client = new TxnSailsRPCClient(owner, server.getPort())) {
      var active = new TransactionIdentity(0, 1);
      var write = List.of(new ValidationItem("usertable", 7, LockType.EX, 1));
      assertNull(
          ((ValidationResponse)
                  client.request(
                      new ValidationRequest(active, write, CertificationContext.FULL, null)))
              .getException());
      for (long sequence = 2; sequence <= 4200; sequence++) {
        client.request(new FinalRequest(new TransactionIdentity(0, sequence), false, List.of()));
      }
      assertNotNull(
          ((ValidationResponse)
                  client.request(
                      new ValidationRequest(
                          new TransactionIdentity(0, 2),
                          List.of(),
                          CertificationContext.FULL,
                          null)))
              .getException());
      for (int coordinator : new int[] {256, Integer.MAX_VALUE}) {
        var independent = new TransactionIdentity(coordinator, 2);
        assertNull(
            ((ValidationResponse)
                    client.request(
                        new ValidationRequest(
                            independent, List.of(), CertificationContext.FULL, null)))
                .getException());
        assertNull(
            ((FinalResponse) client.request(new FinalRequest(independent, true, List.of())))
                .getException());
      }
      assertNull(
          ((ValidationResponse)
                  client.request(
                      new ValidationRequest(active, write, CertificationContext.FULL, null)))
              .getException());
      assertNull(
          ((FinalResponse) client.request(new FinalRequest(active, false, List.of())))
              .getException());
    } finally {
      server.closeServer();
      thread.join(2000);
    }
  }

  @Test
  void concurrentFinalizationCannotAcknowledgeConflictingDecisions() throws Exception {
    ValidationMetaTable.getInstance().initDistributed("ycsb", List.of());
    TxnSailsRPCServer server = new TxnSailsRPCServer(0, new WorkloadConfiguration());
    Thread thread = new Thread(server);
    thread.start();
    DBInstance owner = new DBInstance(0, "", "localhost", "", "");
    var callers = java.util.concurrent.Executors.newFixedThreadPool(2);
    try (TxnSailsRPCClient client = new TxnSailsRPCClient(owner, server.getPort())) {
      var write = List.of(new ValidationItem("usertable", 7, LockType.EX, 1));
      assertNull(
          ((ValidationResponse) client.request(new ValidationRequest(1, write))).getException());
      var recordsField = TxnSailsRPCServer.class.getDeclaredField("records");
      recordsField.setAccessible(true);
      Object record = ((java.util.Map<?, ?>) recordsField.get(server)).values().iterator().next();
      java.util.concurrent.Future<FinalResponse> commit;
      java.util.concurrent.Future<FinalResponse> rollback;
      synchronized (record) {
        commit =
            callers.submit(
                () -> {
                  try (var connection = new TxnSailsRPCClient(owner, server.getPort())) {
                    return (FinalResponse) connection.request(new FinalRequest(1, true, List.of()));
                  }
                });
        rollback =
            callers.submit(
                () -> {
                  try (var connection = new TxnSailsRPCClient(owner, server.getPort())) {
                    return (FinalResponse)
                        connection.request(new FinalRequest(1, false, List.of()));
                  }
                });
        // Both RPC handlers must capture the same active record before either finalizes it.
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        var threads = java.lang.management.ManagementFactory.getThreadMXBean();
        while (true) {
          long blocked =
              java.util.Arrays.stream(threads.getThreadInfo(threads.getAllThreadIds()))
                  .filter(
                      info ->
                          info != null
                              && info.getThreadState() == Thread.State.BLOCKED
                              && info.getLockInfo() != null
                              && info.getLockInfo().getIdentityHashCode()
                                  == System.identityHashCode(record))
                  .count();
          if (blocked == 2) break;
          assertTrue(
              System.nanoTime() < deadline, "Both finalizers must wait on the retained record");
          Thread.sleep(10);
        }
      }
      var first = commit.get(5, java.util.concurrent.TimeUnit.SECONDS).getException();
      var second = rollback.get(5, java.util.concurrent.TimeUnit.SECONDS).getException();
      assertTrue(
          (first == null) != (second == null), "Exactly one final decision may be acknowledged");
      assertEquals(
          "08007", ((java.sql.SQLException) (first != null ? first : second)).getSQLState());
    } finally {
      callers.shutdownNow();
      server.closeServer();
      thread.join(2000);
    }
  }

  @Test
  void optimizedProfilesAndGlobalWatermarkSurviveReconnectAndFinalReplay() throws Exception {
    ValidationMetaTable.getInstance().initDistributed("ycsb", List.of());
    TxnSailsRPCServer server = new TxnSailsRPCServer(0, new WorkloadConfiguration());
    Thread thread = new Thread(server);
    thread.start();
    DBInstance owner = new DBInstance(0, "", "localhost", "", "");
    CertificationContext local =
        new CertificationContext(false, false, java.sql.Connection.TRANSACTION_SERIALIZABLE, false);
    CertificationContext localWriter =
        new CertificationContext(false, true, java.sql.Connection.TRANSACTION_SERIALIZABLE, false);
    CertificationContext globalWriter =
        new CertificationContext(true, true, java.sql.Connection.TRANSACTION_SERIALIZABLE, false);
    List<ValidationItem> read0 = List.of(new ValidationItem("usertable", 7, LockType.SH, 0));
    List<ValidationItem> write1 = List.of(new ValidationItem("usertable", 7, LockType.EX, 1));
    List<ValidationItem> write2 = List.of(new ValidationItem("usertable", 7, LockType.EX, 2));
    try {
      try (TxnSailsRPCClient client = new TxnSailsRPCClient(owner, server.getPort())) {
        assertNull(
            ((ValidationResponse) client.request(new ValidationRequest(1, read0, local)))
                .getException());
        assertNull(
            ((ValidationResponse) client.request(new ValidationRequest(2, write1, localWriter)))
                .getException());
      }
      try (TxnSailsRPCClient client = new TxnSailsRPCClient(owner, server.getPort())) {
        client.request(new FinalRequest(2, true, List.of()));
        assertNotNull(
            ((ValidationResponse) client.request(new ValidationRequest(3, write2, globalWriter)))
                .getException());
        client.request(new FinalRequest(3, false, List.of()));
        client.request(new FinalRequest(1, false, List.of()));
        assertNull(
            ((ValidationResponse) client.request(new ValidationRequest(4, write2, globalWriter)))
                .getException());
      }
      List<ValidationItem> read1 = List.of(new ValidationItem("usertable", 7, LockType.SH, 1));
      try (TxnSailsRPCClient client = new TxnSailsRPCClient(owner, server.getPort())) {
        assertNotNull(
            ((ValidationResponse) client.request(new ValidationRequest(5, read1, local)))
                .getException());
        client.request(new FinalRequest(5, false, List.of()));
        assertNull(
            ((FinalResponse) client.request(new FinalRequest(4, true, List.of()))).getException());
      }
      try (TxnSailsRPCClient client = new TxnSailsRPCClient(owner, server.getPort())) {
        assertNull(
            ((FinalResponse) client.request(new FinalRequest(4, true, List.of()))).getException());
        assertNotNull(
            ((ValidationResponse) client.request(new ValidationRequest(6, read1, local)))
                .getException());
        client.request(new FinalRequest(6, false, List.of()));
        var read2 = List.of(new ValidationItem("usertable", 7, LockType.SH, 2));
        assertNull(
            ((ValidationResponse) client.request(new ValidationRequest(7, read2, local)))
                .getException());
        client.request(new FinalRequest(7, true, List.of()));
      }
    } finally {
      server.closeServer();
      thread.join(2000);
    }
  }

  @Test
  void replayCannotChangeCertificationPolicyAndOwnerChecksIsolation() throws Exception {
    ValidationMetaTable.getInstance().initDistributed("ycsb", List.of());
    WorkloadConfiguration configuration = new WorkloadConfiguration();
    DBInstance owner = new DBInstance(0, "", "localhost", "", "");
    owner.setIsolation("SER");
    configuration.setInstances(List.of(owner));
    TxnSailsRPCServer server = new TxnSailsRPCServer(0, configuration);
    Thread thread = new Thread(server);
    thread.start();
    CertificationContext local =
        new CertificationContext(false, false, java.sql.Connection.TRANSACTION_SERIALIZABLE, false);
    CertificationContext global =
        new CertificationContext(true, false, java.sql.Connection.TRANSACTION_SERIALIZABLE, false);
    List<ValidationItem> items = List.of(new ValidationItem("usertable", 7, LockType.SH, 0));
    try (TxnSailsRPCClient client = new TxnSailsRPCClient(owner, server.getPort())) {
      assertNull(
          ((ValidationResponse) client.request(new ValidationRequest(1, items, local)))
              .getException());
      assertNull(
          ((ValidationResponse) client.request(new ValidationRequest(1, items, local)))
              .getException());
      assertNotNull(
          ((ValidationResponse) client.request(new ValidationRequest(1, items, global)))
              .getException());
      client.request(new FinalRequest(1, false, List.of()));
      CertificationContext wrong =
          new CertificationContext(
              false, false, java.sql.Connection.TRANSACTION_REPEATABLE_READ, false);
      assertNotNull(
          ((ValidationResponse) client.request(new ValidationRequest(2, items, wrong)))
              .getException());
      client.request(new FinalRequest(2, false, List.of()));
    } finally {
      server.closeServer();
      thread.join(2000);
    }
  }

  @Test
  void retainedRemoteLocksSurviveDisconnectAndFinalizationIsIdempotent() throws Exception {
    ValidationMetaTable table = ValidationMetaTable.getInstance();
    table.initDistributed("ycsb", List.of());
    table.updateHotspotVersion("usertable", 7, 5);
    TxnSailsRPCServer server = new TxnSailsRPCServer(0, new WorkloadConfiguration());
    Thread thread = new Thread(server);
    thread.start();
    DBInstance instance = new DBInstance(0, "", "localhost", "", "");
    List<ValidationItem> read = List.of(new ValidationItem("usertable", 7, LockType.SH, 5));
    List<ValidationItem> write = List.of(new ValidationItem("usertable", 7, LockType.EX, 6));
    try (TxnSailsRPCClient first = new TxnSailsRPCClient(instance, server.getPort());
        TxnSailsRPCClient second = new TxnSailsRPCClient(instance, server.getPort())) {
      assertNull(
          ((ValidationResponse) first.request(new ValidationRequest(1, read))).getException());
      assertNull(
          ((ValidationResponse) first.request(new ValidationRequest(1, read))).getException());
      assertNotNull(
          ((ValidationResponse) first.request(new ValidationRequest(1, write))).getException());
      first.close();
      assertNotNull(
          ((ValidationResponse) second.request(new ValidationRequest(2, write))).getException());
      second.request(new FinalRequest(1, false, read));
      assertNull(
          ((ValidationResponse) second.request(new ValidationRequest(3, write))).getException());
      second.request(new FinalRequest(1, false, read));
      assertNotNull(
          ((ValidationResponse) second.request(new ValidationRequest(4, read))).getException());
      second.request(new FinalRequest(3, true, write));
      assertEquals(6, table.getHotspotVersion("usertable", 7));
      assertNull(
          ((ValidationResponse) second.request(new ValidationRequest(5, write))).getException());
      second.request(new FinalRequest(5, false, write));
    } finally {
      server.closeServer();
      thread.join(2000);
    }
  }

  @Test
  void finalizedHistoryIsBoundedAndCannotReviveOldTransactions() throws Exception {
    ValidationMetaTable.getInstance().initDistributed("ycsb", List.of());
    TxnSailsRPCServer server = new TxnSailsRPCServer(0, new WorkloadConfiguration());
    Thread thread = new Thread(server);
    thread.start();
    DBInstance instance = new DBInstance(0, "", "localhost", "", "");
    List<ValidationItem> write = List.of(new ValidationItem("usertable", 7, LockType.EX, 1));
    try (TxnSailsRPCClient client = new TxnSailsRPCClient(instance, server.getPort())) {
      assertNull(
          ((ValidationResponse) client.request(new ValidationRequest(256, write))).getException());
      for (long sequence = 2; sequence <= 4200; sequence++) {
        client.request(new FinalRequest(sequence << 8, false, List.of()));
      }
      var historyField = TxnSailsRPCServer.class.getDeclaredField("completed");
      historyField.setAccessible(true);
      assertTrue(((java.util.Map<?, ?>) historyField.get(server)).size() <= 4096);
      assertNotNull(
          ((ValidationResponse) client.request(new ValidationRequest(512, write))).getException());
      assertNull(
          ((ValidationResponse) client.request(new ValidationRequest(256, write))).getException());
      client.request(new FinalRequest(256, false, write));
    } finally {
      server.closeServer();
      thread.join(2000);
    }
  }
}
