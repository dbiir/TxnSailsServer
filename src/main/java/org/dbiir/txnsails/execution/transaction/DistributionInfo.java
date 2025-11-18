package org.dbiir.txnsails.execution.transaction;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.execution.validation.ValidationMeta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DistributionInfo {
  @Getter
  private static final DistributionInfo INSTANCE = new DistributionInfo();

  @Setter
  static public Map<String, List<Integer>> tableName2DbList = new HashMap<>();

  public int getInstanceID(String tableName, int uniqueId) {
    List<Integer> list = tableName2DbList.get(tableName);
    return list.get(uniqueId%list.size());
  }

  public int getInstanceID(ValidationMeta meta) {
    List<Integer> list = tableName2DbList.get(meta.getTemplateSQL().getTable());
    return list.get(meta.getIdForValidation() % list.size());
  }
}
