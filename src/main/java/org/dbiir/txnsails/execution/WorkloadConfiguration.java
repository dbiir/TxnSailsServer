/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dbiir.txnsails.execution;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.DatabaseType;
import org.dbiir.txnsails.common.JacksonXmlConfiguration;
import org.dbiir.txnsails.partition.Partition;
import org.dbiir.txnsails.partition.PartitionConfig;
import org.dbiir.txnsails.worker.Adapter;

@Setter @Getter
public class WorkloadConfiguration {

  private DatabaseType databaseType;
  private String benchmarkName;
  private String url;
  private String username;
  private String password;
  private String driverClass;
  private int batchSize;
  private int maxRetries;
  private int randomSeed = -1;
  private double scaleFactor = 1.0;
  private double selectivity = -1.0;
  private int terminals;
  private JacksonXmlConfiguration xmlConfig = null;
  private boolean parallelExecution;
  private int instanceID;
  private List<DBInstance> instances = new ArrayList<>();
  private Map<String,List<Integer>> tableName2DbList = new HashMap<>();

  @Getter
  private int isolationMode =
          Connection.TRANSACTION_SERIALIZABLE; // always providing serializable execution

  @Getter private CCType concurrencyControlType = CCType.SER;

  /**
   * If true, establish a new connection for each transaction, otherwise use one persistent
   * connection per client session. This is useful to measure the connection overhead. -- SETTER --
   * Used by the configuration loader at startup. Changing it any other time is probably
   * dangeroues. @see newConnectionPerTxn member docs for behavior.
   *
   * @param newConnectionPerTxn
   */
  @Setter private boolean newConnectionPerTxn = false;

  /**
   * If true, attempt to catch connection closed exceptions and reconnect. This allows the benchmark
   * to recover like a typical application would in the case of a replicated cluster
   * primary-secondary failover. -- SETTER -- Used by the configuration loader at startup. Changing
   * it any other time is probably dangeroues. @see reconnectOnConnectionFailure member docs for
   * behavior.
   *
   * @param reconnectOnConnectionFailure
   */
  @Setter private boolean reconnectOnConnectionFailure = false;

  public void setConcurrencyControlType(String type) {
    switch (type) {
      case "SERIALIZABLE":
        this.concurrencyControlType = CCType.SER;
        break;
      case "SI_TAILOR":
        this.concurrencyControlType = CCType.SI_TAILOR;
        break;
      case "RC_TAILOR":
        this.concurrencyControlType = CCType.RC_TAILOR;
        break;
      case "DYNAMIC":
        this.concurrencyControlType = CCType.DYNAMIC;
        Adapter.getInstance().setUsed(true);
        break;
      case "RC":
        this.concurrencyControlType = CCType.RC;
        break;
      case "SI":
        this.concurrencyControlType = CCType.SI;
        break;
      default:
        this.concurrencyControlType = CCType.NUM_CC;
    }
  }

  /**
   * @return @see newConnectionPerTxn member docs for behavior.
   */
  public boolean getNewConnectionPerTxn() {
    return newConnectionPerTxn;
  }

  /**
   * @return @see reconnectOnConnectionFailure member docs for behavior.
   */
  public boolean getReconnectOnConnectionFailure() {
    return reconnectOnConnectionFailure;
  }

  /** A utility method that init the phaseIterator and dialectMap */
  public void init() {
    try {
      Class.forName(this.driverClass);
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Failed to initialize JDBC driver '" + this.driverClass + "'", ex);
    }
  }

  public String getIsolationString() {
    if (this.isolationMode == Connection.TRANSACTION_SERIALIZABLE) {
      return "TRANSACTION_SERIALIZABLE";
    } else if (this.isolationMode == Connection.TRANSACTION_READ_COMMITTED) {
      return "TRANSACTION_READ_COMMITTED";
    } else if (this.isolationMode == Connection.TRANSACTION_REPEATABLE_READ) {
      return "TRANSACTION_REPEATABLE_READ";
    } else if (this.isolationMode == Connection.TRANSACTION_READ_UNCOMMITTED) {
      return "TRANSACTION_READ_UNCOMMITTED";
    } else if (this.isolationMode == Connection.TRANSACTION_NONE) {
      return "TRANSACTION_NONE";
    } else {
      return "TRANSACTION_SERIALIZABLE";
    }
  }

  @Override
  public String toString() {
    return "WorkloadConfiguration{"
            + ", databaseType="
            + databaseType
            + ", benchmarkName='"
            + benchmarkName
            + '\''
            + ", url='"
            + url
            + '\''
            + ", username='"
            + username
            + '\''
            + ", password='"
            + password
            + '\''
            + ", driverClass='"
            + driverClass
            + '\''
            + ", batchSize="
            + batchSize
            + ", maxRetries="
            + maxRetries
            + ", scaleFactor="
            + scaleFactor
            + ", selectivity="
            + selectivity
            + ", terminals="
            + terminals
            + '\''
            + '}';
  }
}
