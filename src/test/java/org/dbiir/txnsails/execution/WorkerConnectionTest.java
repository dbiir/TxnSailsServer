package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.worker.MetaWorker;
import org.dbiir.txnsails.worker.OnlineWorker;
import org.junit.jupiter.api.Test;

class WorkerConnectionTest {
  @Test
  void opensParticipantsByConfiguredIdAndAppliesEachIsolation() throws Exception {
    Map<String, Connection> opened = new HashMap<>();
    Driver driver =
        new Driver() {
          public Connection connect(String url, Properties info) {
            if (!acceptsURL(url)) return null;
            int[] isolation = {Connection.TRANSACTION_READ_COMMITTED};
            boolean[] autoCommit = {true};
            Connection connection =
                (Connection)
                    Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) ->
                            switch (method.getName()) {
                              case "setTransactionIsolation" -> {
                                isolation[0] = (int) args[0];
                                yield null;
                              }
                              case "getTransactionIsolation" -> isolation[0];
                              case "setAutoCommit" -> {
                                autoCommit[0] = (boolean) args[0];
                                yield null;
                              }
                              case "getAutoCommit" -> autoCommit[0];
                              case "isClosed" -> false;
                              default -> null;
                            });
            opened.put(url, connection);
            return connection;
          }

          public boolean acceptsURL(String url) {
            return url.startsWith("jdbc:txnsails-test:");
          }

          public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
            return new DriverPropertyInfo[0];
          }

          public int getMajorVersion() {
            return 1;
          }

          public int getMinorVersion() {
            return 0;
          }

          public boolean jdbcCompliant() {
            return false;
          }

          public Logger getParentLogger() {
            return Logger.getGlobal();
          }
        };
    DriverManager.registerDriver(driver);
    try {
      MetaWorker.getINSTANCE().setSchema(new SchemaInfo("config/ycsb.sql"));
      DBInstance first = new DBInstance(2, "jdbc:txnsails-test:a", "localhost", "", "");
      DBInstance second = new DBInstance(5, "jdbc:txnsails-test:b", "localhost", "", "");
      first.setIsolation("RC");
      second.setIsolation("SI");
      WorkloadConfiguration configuration = new WorkloadConfiguration();
      configuration.setInstances(List.of(first, second));
      OnlineWorker worker = new OnlineWorker(configuration, 1);
      try {
        assertEquals(2, opened.size());
        assertEquals(
            Connection.TRANSACTION_READ_COMMITTED,
            opened.get(first.getUrl()).getTransactionIsolation());
        assertEquals(
            Connection.TRANSACTION_REPEATABLE_READ,
            opened.get(second.getUrl()).getTransactionIsolation());
        assertTrue(opened.get(first.getUrl()).getAutoCommit());
      } finally {
        worker.closeWorker();
      }
    } finally {
      DriverManager.deregisterDriver(driver);
    }
  }
}
