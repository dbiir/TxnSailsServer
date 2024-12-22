package org.dbiir.txnsails.common;

public enum TransactionStatus {
    IDLE("Idle"), // none transaction
    ACTIVE("Active"), // in processing transaction
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
