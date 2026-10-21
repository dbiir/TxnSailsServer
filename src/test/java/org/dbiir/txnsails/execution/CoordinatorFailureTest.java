package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import org.dbiir.txnsails.common.*;
import org.dbiir.txnsails.common.messages.*;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.transaction.*;
import org.dbiir.txnsails.execution.validation.*;
import org.junit.jupiter.api.Test;

class CoordinatorFailureTest {
  private final List<String> events = new ArrayList<>();
  private String faultState = "08006";

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(strings = {"08006", "57P01", "XX000"})
  void lostPrepareAcknowledgementRetainsLocksAndPreventsRollback(String state) throws Exception {
    faultState = state;
    TransactionManager manager = manager();
    Transaction transaction = transaction(21, "PREPARE TRANSACTION");
    manager.addTransaction(transaction);
    SQLException prepare = assertThrows(SQLException.class, () -> manager.prepare(21));
    assertEquals("08007", prepare.getSQLState());
    assertThrows(SQLException.class, () -> manager.rollback(21, null));
    assertThrows(
        SQLException.class, () -> new ValidationItem("usertable", 0, LockType.EX, 2).validate(22));
    assertFalse(events.stream().anyMatch(event -> event.contains("ROLLBACK")));
  }

  @Test
  void definitivePrepareRejectionRollsBackAlreadyPreparedBranches() throws Exception {
    faultState = "40001";
    TransactionManager manager = manager();
    manager.addTransaction(transaction(61, "PREPARE TRANSACTION"));
    assertEquals(
        "40001", assertThrows(SQLException.class, () -> manager.prepare(61)).getSQLState());
    manager.rollback(61, null);
    manager.doAfterCommit(61, false);
    assertTrue(events.stream().anyMatch(event -> event.contains("ROLLBACK PREPARED")));
    new ValidationItem("usertable", 0, LockType.EX, 2).validate(62);
  }

  @Test
  void failedRollbackCannotBePreparedAgain() throws Exception {
    TransactionManager manager = manager();
    Transaction transaction =
        new Transaction(31, List.of(new Participant(0, connection(0, "ROLLBACK"))));
    manager.addTransaction(transaction);
    assertThrows(SQLException.class, () -> manager.rollback(31, null));
    assertThrows(SQLException.class, () -> manager.prepare(31));
  }

  @Test
  void commitFailureRetainsLocksAndForbidsRollback() throws Exception {
    TransactionManager manager = manager();
    Transaction transaction = transaction(41, "COMMIT PREPARED");
    manager.addTransaction(transaction);
    manager.prepare(41);
    SQLException failure = assertThrows(SQLException.class, () -> manager.commit(41, null));
    assertEquals("08007", failure.getSQLState());
    assertThrows(SQLException.class, () -> manager.rollback(41, null));
    assertThrows(SQLException.class, () -> manager.doAfterCommit(41, true));
    assertThrows(SQLException.class, () -> manager.prepare(41));
    assertThrows(
        SQLException.class, () -> new ValidationItem("usertable", 0, LockType.EX, 2).validate(42));
  }

  @Test
  void finalizationFollowsCommitOfEveryDatabase() throws Exception {
    TransactionManager manager = manager();
    manager.addTransaction(transaction(51, "never"));
    manager.prepare(51);
    manager.commit(51, null);
    assertFalse(events.contains("FINAL"));
    manager.doAfterCommit(51, true);
    assertTrue(events.get(events.size() - 1).equals("FINAL"));
    assertTrue(events.stream().filter(event -> event.contains("COMMIT PREPARED")).count() == 2);
    assertEquals(1, ValidationMetaTable.getInstance().getHotspotVersion("usertable", 0));
    new ValidationItem("usertable", 0, LockType.EX, 2).validate(52);
  }

  private TransactionManager manager() throws Exception {
    ValidationMetaTable.getInstance().initDistributed("ycsb", List.of());
    TransactionManager manager = new TransactionManager();
    var field = TransactionManager.class.getDeclaredField("clients");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    Map<Integer, TxnSailsRPCClient> clients = (Map<Integer, TxnSailsRPCClient>) field.get(manager);
    clients.put(
        1,
        new TxnSailsRPCClient(new DBInstance(1, "", "localhost", "", ""), 0) {
          @Override
          public Message request(Message request) {
            if (request instanceof ValidationRequest validation)
              return new ValidationResponse(validation.getTid());
            events.add("FINAL");
            return new FinalResponse(((FinalRequest) request).getTid());
          }
        });
    return manager;
  }

  private Transaction transaction(long tid, String failure) throws Exception {
    Participant first = new Participant(0, connection(0, "never"));
    first.addValidationItem("usertable", 0, LockType.EX, 1);
    return new Transaction(tid, List.of(first, new Participant(1, connection(1, failure))));
  }

  private Connection connection(int id, String failure) {
    return (Connection)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              if (!method.getName().equals("prepareStatement")) return null;
              String sql = (String) args[0];
              return Proxy.newProxyInstance(
                  getClass().getClassLoader(),
                  new Class<?>[] {PreparedStatement.class},
                  (statement, operation, values) -> {
                    if (operation.getName().equals("execute")) {
                      events.add(id + ":" + sql);
                      if (sql.startsWith(failure))
                        throw new SQLException("simulated lost response", faultState);
                      return false;
                    }
                    return null;
                  });
            });
  }
}
