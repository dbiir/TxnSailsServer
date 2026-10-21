package org.dbiir.txnsails.execution.validation;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.dbiir.txnsails.common.types.LockType;

/** One validation item per logical row, with read and write versions kept separately. */
public class ValidationSet {
  private final List<ValidationItem> items;

  public ValidationSet(int capacity) {
    items = new ArrayList<>(capacity);
  }

  public int getItemCount() {
    return items.size();
  }

  public void addValidationItems(List<ValidationItem> additions) throws SQLException {
    for (ValidationItem item : additions) addValidationItem(item);
  }

  public void addValidationItem(ValidationItem addition) throws SQLException {
    for (ValidationItem item : items) {
      if (item.getIdentity().equals(addition.getIdentity())
          && item.getRelationName().equals(addition.getRelationName())) {
        item.merge(addition);
        return;
      }
    }
    ValidationItem copy = new ValidationItem();
    copy.copy(addition);
    items.add(copy);
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
  }

  public boolean isEmpty() {
    return items.isEmpty();
  }
}
