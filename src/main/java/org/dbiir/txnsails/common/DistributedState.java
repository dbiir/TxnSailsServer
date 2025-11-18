package org.dbiir.txnsails.common;

import lombok.Getter;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

@Getter
public enum DistributedState {
  IDLE("IDLE"),
  INITIALIZED("INITIALIZED"),
  PREPARED("PREPARED"),
  COMMITTED("COMMITTED"),
  ABORTED("ABORTED");

  private final String name;

  DistributedState(String name) {
    this.name = name;
  }

  static final Map<Integer, DistributedState> idx_lookup = new HashMap<>();
  static final Map<String, DistributedState> name_lookup = new HashMap<>();

  static {
    for (DistributedState vt : EnumSet.allOf(DistributedState.class)) {
      DistributedState.idx_lookup.put(vt.ordinal(), vt);
      DistributedState.name_lookup.put(vt.name().toUpperCase(), vt);
    }
  }

  public static DistributedState get(String name) {
    return (DistributedState.name_lookup.get(name.toUpperCase()));
  }
}
