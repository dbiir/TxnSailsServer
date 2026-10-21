package org.dbiir.txnsails.execution.transaction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.execution.validation.ValidationMeta;

public class DistributionInfo {
  @Getter private static final DistributionInfo INSTANCE = new DistributionInfo();

  @Setter public static Map<String, List<Integer>> tableName2DbList = new HashMap<>();

  public int getInstanceID(String tableName, int uniqueId) {
    List<Integer> list = tableName2DbList.get(tableName.toLowerCase(java.util.Locale.ROOT));
    return list.get(Math.floorMod(uniqueId, list.size()));
  }

  public int getInstanceID(ValidationMeta meta) {
    List<Integer> list =
        tableName2DbList.get(meta.getTemplateSQL().getTable().toLowerCase(java.util.Locale.ROOT));
    return list.get(Math.floorMod(meta.getIdForValidation(), list.size()));
  }
}
