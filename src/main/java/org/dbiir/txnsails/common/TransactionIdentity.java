package org.dbiir.txnsails.common;

import java.io.Serializable;

/** A coordinator's durable local sequence, namespaced by its complete configured ID. */
public record TransactionIdentity(int coordinatorId, long sequence) implements Serializable {
  private static final long serialVersionUID = 1L;

  public TransactionIdentity {
    if (coordinatorId < 0) throw new IllegalArgumentException("Coordinator ID must be nonnegative");
    if (sequence < 0)
      throw new IllegalArgumentException("Transaction sequence must be nonnegative");
  }
}
