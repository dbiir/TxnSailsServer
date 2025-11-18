package org.dbiir.txnsails.partition;

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.execution.WorkloadConfiguration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class PartitionMeta {
  @Getter
  private final int partitionId;
  private final List<Connection> connectionList;
  @Getter
  private final CCType isolation;
  private final WorkloadConfiguration workConf;

  public PartitionMeta(WorkloadConfiguration workConf, int id, CCType isolation) {
    this.workConf = workConf;
    this.partitionId = id;
    int terminalNum = workConf.getTerminals();
    this.connectionList = new ArrayList<>(terminalNum + 5);
    this.isolation = isolation;

    try {
      for  (int i = 0; i < terminalNum + 5; i++) {
        Connection conn = makeConnection();
        conn.setAutoCommit(true);
        setConnectionIsolation(conn, isolation);
        connectionList.add(conn);
      }
    } catch (SQLException ex) {
      throw new RuntimeException("Failed to connect to database", ex);
    }
  }

  private final Connection makeConnection() throws SQLException {
    if (StringUtils.isEmpty(workConf.getUsername())) {
      return DriverManager.getConnection(workConf.getUrl());
    } else {
      return DriverManager.getConnection(
              workConf.getUrl(), workConf.getUsername(), workConf.getPassword());
    }
  }

  private void setConnectionIsolation(Connection conn, CCType isolation) {
    try {
      switch (isolation) {
        case RC:
        case RC_TAILOR:
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
          break;
        case SI:
        case SI_TAILOR:
          conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
          break;
        case SER:
          conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      }
    } catch (SQLException ex) {
      throw new RuntimeException("Failed to connect to database", ex);
    }
  }

  public Connection getConnection(int workId) {
    return this.connectionList.get(workId);
  }

  public Connection reconnect(int workId) throws SQLException {
    if (this.connectionList.get(workId) != null) {
      try {
        this.connectionList.get(workId).close();
      } catch (SQLException e) {
        this.connectionList.set(workId, null);
        throw new RuntimeException(e);
      }
    }
    Connection conn = makeConnection();
    conn.setAutoCommit(false);
    setConnectionIsolation(conn, isolation);
    this.connectionList.set(workId, conn);
    return conn;
  }
}

