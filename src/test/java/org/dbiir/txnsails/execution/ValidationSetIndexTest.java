package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.validation.ValidationItem;
import org.dbiir.txnsails.execution.validation.ValidationSet;
import org.junit.jupiter.api.Test;

class ValidationSetIndexTest {
  @Test
  void identityLookupWorkGrowsLinearlyForDistinctRows() throws Exception {
    ValidationSet set = new ValidationSet(1);
    List<CountingItem> additions = new ArrayList<>();
    for (int key = 0; key < 512; key++) {
      CountingItem item = new CountingItem(key);
      additions.add(item);
      set.addValidationItem(item);
    }
    assertEquals(512, set.getItemCount());
    assertTrue(
        additions.stream().mapToInt(item -> item.identityReads).sum() <= 1024,
        "Each incoming row should need a bounded number of identity lookups");
  }

  @Test
  void mergeUsesFullIdentityAndRetainsFirstAccessOrder() throws Exception {
    ValidationSet set = new ValidationSet(1);
    set.addValidationItem(row("one", 2, LockType.SH, 4));
    set.addValidationItem(row("one", 1, LockType.SH, 6));
    set.addValidationItem(row("two", 2, LockType.SH, 9));
    set.addValidationItem(row("one", 2, LockType.EX, 5));
    assertEquals(3, set.getItemCount());
    assertEquals("{id=2}", set.get(0).getIdentity());
    assertEquals("{id=1}", set.get(1).getIdentity());
    assertEquals("two", set.get(2).getRelationName());
    assertEquals(4, set.get(0).getReadVersion());
    assertEquals(5, set.get(0).getWriteVersion());
    set.reset();
    set.addValidationItem(row("one", 2, LockType.SH, 20));
    assertEquals(1, set.getItemCount());
    assertEquals(20, set.get(0).getReadVersion());
  }

  @Test
  void reportsWritesAfterAnUpgradeAndClearsTheFlagOnReset() throws Exception {
    ValidationSet set = new ValidationSet(1);
    assertFalse(set.hasWrites());
    set.addValidationItem(row("one", 1, LockType.SH, 0));
    assertFalse(set.hasWrites());
    set.addValidationItem(row("one", 1, LockType.EX, 1));
    assertTrue(set.hasWrites());
    set.reset();
    assertFalse(set.hasWrites());
  }

  private ValidationItem row(String table, long key, LockType type, long version) {
    return new ValidationItem(table, 0, type, version).withPrimaryKey(Map.of("id", key));
  }

  private static class CountingItem extends ValidationItem {
    private int identityReads;

    CountingItem(int key) {
      super("usertable", key, LockType.SH, 0);
    }

    @Override
    public String getIdentity() {
      identityReads++;
      return super.getIdentity();
    }
  }
}
