package org.dbiir.txnsails.common.types;

import java.util.Locale;

/** Selects the execution protocol independently of the configured number of logical owners. */
public enum DeploymentMode {
  SINGLE,
  DISTRIBUTED,
  MOCK;

  public static DeploymentMode parse(String value) {
    if (value == null) return SINGLE;
    return switch (value.trim().toLowerCase(Locale.ROOT)) {
      case "single" -> SINGLE;
      case "distributed" -> DISTRIBUTED;
      case "mock" -> MOCK;
      default -> throw new IllegalArgumentException("Mode must be single, distributed or mock");
    };
  }
}
