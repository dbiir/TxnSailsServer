package org.dbiir.txnsails.execution.validation;

import java.io.Serializable;
import java.sql.Connection;

/** Frozen after execution; global means more than one actual database participant. */
public record CertificationContext(
    boolean global, boolean hasBusinessWrite, int isolation, boolean conservative)
    implements Serializable {
  private static final long serialVersionUID = 1L;
  public static final CertificationContext FULL =
      new CertificationContext(true, true, Connection.TRANSACTION_SERIALIZABLE, true);

  public CertificationContext {
    if (isolation != Connection.TRANSACTION_READ_COMMITTED
        && isolation != Connection.TRANSACTION_REPEATABLE_READ
        && isolation != Connection.TRANSACTION_SERIALIZABLE) {
      throw new IllegalArgumentException("Unsupported certification isolation " + isolation);
    }
  }

  /** Every SI dangerous-structure pivot writes, so all SI read/write reads are sufficient. */
  public boolean strictRead() {
    return conservative
        || global
        || isolation == Connection.TRANSACTION_READ_COMMITTED
        || (isolation == Connection.TRANSACTION_REPEATABLE_READ && hasBusinessWrite);
  }
}
