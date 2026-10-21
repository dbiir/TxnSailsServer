package org.dbiir.txnsails.common;

public enum TransactionStatus {
  IDLE("Idle"), // none transaction
  ACTIVE("Active"), // in processing transaction
  PREPARED("Prepared"), // prepared to commit
  PREPARE_UNKNOWN("PrepareUnknown"),
  ROLLING_BACK("RollingBack"),
  PREPARE_FAILED("PrepareFailed"), // prepare failed
  COMMITTING("Committing"),
  COMMITTED("Committed"),
  ROLLBACK("Rollback");

  private final String description;

  TransactionStatus(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return description;
  }
}
