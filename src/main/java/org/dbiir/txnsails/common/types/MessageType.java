package org.dbiir.txnsails.common.types;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum MessageType {
  VALIDATION_REQ("VALIDATION_REQ"),
  VALIDATION_RESP("VALIDATION_RESP"),
  PREPARE_REQ("PREPARE_REQ"),
  PREPARE_RESP("PREPARE_RESP"),
  COMMIT_REQ("COMMIT_REQ"),
  COMMIT_RESP("COMMIT_RESP"),
  ROLLBACK_REQ("ROLLBACK_REQ"),
  ROLLBACK_RESP("ROLLBACK_RESP"),
  FINAL_REQ("FINAL_REQ"),
  FINAL_RESP("FINAL_RESP"),
  DONE_REQ("DONE_REQ"),
  DONE_RESP("DONE_RESP"),
  NUM_MESSAGE("NUM_MESSAGE");
  private final String name;

  MessageType(String name) {
    this.name = name;
  }

  static final Map<Integer, MessageType> idx_lookup = new HashMap<>();
  static final Map<String, MessageType> name_lookup = new HashMap<>();

  static {
    for (MessageType vt : EnumSet.allOf(MessageType.class)) {
      MessageType.idx_lookup.put(vt.ordinal(), vt);
      MessageType.name_lookup.put(vt.name().toUpperCase(), vt);
    }
  }

  public String getName() {
    return name;
  }

  public static MessageType get(String name) {
    return (MessageType.name_lookup.get(name.toUpperCase()));
  }
}
