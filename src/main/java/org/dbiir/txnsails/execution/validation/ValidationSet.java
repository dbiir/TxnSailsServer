package org.dbiir.txnsails.execution.validation;

import lombok.Getter;
import org.dbiir.txnsails.common.types.LockType;

import java.util.ArrayList;
import java.util.List;

public class ValidationSet {
  private final List<ValidationItem> items;
  @Getter
  private int itemCount;

  public ValidationSet(int capacity) {
    this.items = new ArrayList<>(capacity);
    this.itemCount = 0;
    for (int i = 0; i < capacity; i++) {
      this.items.add(new ValidationItem());
    }
  }

  public void addValidationItems(List<ValidationItem> validationItems) {
    for (ValidationItem item: validationItems) {
      this.items.get(itemCount).copy(item);
      this.itemCount++;
    }
  }

  public void addValidationItem(ValidationItem validationItem) {
    this.items.get(itemCount).copy(validationItem);
    this.itemCount++;
  }

  public void addValidationItem(String relationName, long key, LockType lockType, long oldVersion) {
    this.items.get(itemCount).copy(relationName, key, lockType, oldVersion);
    this.itemCount++;
  }

  public List<ValidationItem> getValidationItems() {
    return this.items.subList(0, itemCount);
  }

  public ValidationItem get(int idx) {
    return this.items.get(idx);
  }

  public void reset() {
    this.itemCount = 0;
  }

  public boolean isEmpty() {
    return itemCount == 0;
  }
}
