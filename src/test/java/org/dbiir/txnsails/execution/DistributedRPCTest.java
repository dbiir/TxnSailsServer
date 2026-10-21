package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.common.messages.*;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.transaction.*;
import org.dbiir.txnsails.execution.validation.*;
import org.junit.jupiter.api.Test;

class DistributedRPCTest {
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
