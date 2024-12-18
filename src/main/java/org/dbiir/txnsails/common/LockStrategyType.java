package org.dbiir.txnsails.common;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum LockStrategyType {
    NO_WAIT("No wait"),
    WAIT_DIE("Wait die");

    private final String description;

    LockStrategyType(String description) {
        this.description = description;
    }

    @Override
    public String toString() {
        return description;
    }
}

