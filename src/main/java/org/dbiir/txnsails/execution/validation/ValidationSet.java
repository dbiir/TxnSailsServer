package org.dbiir.txnsails.execution.validation;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dbiir.txnsails.common.types.LockType;

/** One validation item per logical row, with read and write versions kept separately. */
public class ValidationSet {
  private record RowIdentity(String table, String identity) {}

  private final List<ValidationItem> items;
  private final Map<RowIdentity, ValidationItem> byIdentity;
  private boolean hasWrites;

  public ValidationSet(int capacity) {
    items = new ArrayList<>(capacity);
    byIdentity = new HashMap<>(capacity);
  }

  public int getItemCount() {
    return items.size();
  }

  public void addValidationItems(List<ValidationItem> additions) throws SQLException {
    for (ValidationItem item : additions) addValidationItem(item);
  }

  public void addValidationItem(ValidationItem addition) throws SQLException {
    RowIdentity identity = new RowIdentity(addition.getRelationName(), addition.getIdentity());
    ValidationItem existing = byIdentity.get(identity);
    if (existing != null) {
      existing.merge(addition);
      hasWrites |= existing.getWriteVersion() >= 0;
      return;
    }
    ValidationItem copy = new ValidationItem();
    copy.copy(addition);
    items.add(copy);
    byIdentity.put(identity, copy);
    hasWrites |= copy.getWriteVersion() >= 0;
  }

  public void addValidationItem(String table, long key, LockType type, long version)
      throws SQLException {
    addValidationItem(new ValidationItem(table, key, type, version));
  }

  public List<ValidationItem> getValidationItems() {
    return List.copyOf(items);
  }

  public ValidationItem get(int index) {
    return items.get(index);
  }

  public void reset() {
    items.clear();
    byIdentity.clear();
    hasWrites = false;
  }

  public boolean hasWrites() {
    return hasWrites;
  }

  public boolean isEmpty() {
    return items.isEmpty();
  }
}
