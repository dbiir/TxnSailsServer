package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.worker.MetaWorker;
import org.dbiir.txnsails.worker.OnlineWorker;
import org.junit.jupiter.api.Test;

class WorkerConnectionTest {
  @Test
  void thousandsOfSparseOwnersDoNotOpenUntouchedBusinessConnections() throws Exception {
    AtomicInteger opens = new AtomicInteger();
    Driver driver = new CountingDriver(opens);
    DriverManager.registerDriver(driver);
    try {
      MetaWorker.getINSTANCE().setSchema(new SchemaInfo("config/ycsb.sql"));
      List<DBInstance> owners = new ArrayList<>();
      for (int n = 0; n < 4096; n++) {
        var owner =
            new DBInstance(Integer.MAX_VALUE - n, "jdbc:txnsails-lazy:" + n, "localhost", "", "");
        owner.setIsolation(n % 2 == 0 ? "SER" : "SI");
        owners.add(owner);
      }
      WorkloadConfiguration configuration = new WorkloadConfiguration();
      configuration.setInstances(owners);
      configuration.setInstanceID(Integer.MAX_VALUE);
      OnlineWorker worker = assertDoesNotThrow(() -> new OnlineWorker(configuration, 1));
      try {
        assertEquals(0, opens.get(), "Untouched nodes must not consume JDBC connections");
      } finally {
        worker.closeWorker();
      }
      assertEquals(0, opens.get());
    } finally {
      DriverManager.deregisterDriver(driver);
    }
  }

  private record CountingDriver(AtomicInteger opens) implements Driver {
    public Connection connect(String url, Properties info) throws SQLException {
      if (!acceptsURL(url)) return null;
      opens.incrementAndGet();
      throw new SQLException("Business connection should be opened only for a touched owner");
    }

    public boolean acceptsURL(String url) {
      return url != null && url.startsWith("jdbc:txnsails-lazy:");
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
  }
}
