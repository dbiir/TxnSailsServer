package org.dbiir.txnsails.partition;

import lombok.Getter;
import org.dbiir.txnsails.common.DistributedState;

import java.sql.Connection;

public class ParticipantInfo {
  @Getter
  private Connection connection;
  private DistributedState state;
  @Getter
  private long[] versions = new long[10];

  public ParticipantInfo(Connection connection) {
    this.connection = connection;
    this.state = DistributedState.IDLE;
  }

  public void begin() {
    this.state = DistributedState.INITIALIZED;
  }

  public boolean isPrepared() {
    return state == DistributedState.PREPARED;
  }

  public boolean isCommitted() {
    return state == DistributedState.COMMITTED;
  }

  public boolean isAborted() {
    return state == DistributedState.ABORTED;
  }
}
