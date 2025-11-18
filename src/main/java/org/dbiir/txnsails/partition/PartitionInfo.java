package org.dbiir.txnsails.partition;

import lombok.Getter;

import java.util.HashMap;

public class PartitionInfo {
  @Getter
  private static final PartitionInfo INSTANCE = new PartitionInfo();

  static public HashMap<String, Integer> tableNameToPartitionId = new HashMap<>();

  public void addItem(String tableName, int partitionId) {
    tableNameToPartitionId.put(tableName, partitionId);
  }

  public int getPartitionId(String tableName) {
    return tableNameToPartitionId.get(tableName);
  }
}
