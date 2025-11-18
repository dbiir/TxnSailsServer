package org.dbiir.txnsails.common;

import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;

@Getter
@Setter
public class AsyncResultWrapper {
  private Connection connection;
  private int partitionId;
  private Exception exception;
  private boolean prepared;

  public AsyncResultWrapper(Connection connection, int partitionId, Exception exception) {
    this.connection = connection;
    this.partitionId = partitionId;
    this.exception = exception;
    this.prepared = false;
  }

  public AsyncResultWrapper(Connection connection, int partitionId) {
    this.connection = connection;
    this.partitionId = partitionId;
    this.exception = null;
    this.prepared = false;
  }

  public void reset() {
    this.exception = null;
    this.prepared = false;
  }

  public boolean isSuccess() {
    return exception == null;
  }
};
