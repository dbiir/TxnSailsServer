package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.List;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.transaction.Participant;
import org.dbiir.txnsails.execution.transaction.Transaction;
import org.dbiir.txnsails.execution.transaction.TransactionManager;
import org.dbiir.txnsails.execution.utils.TransactionIdGenerator;
import org.dbiir.txnsails.execution.validation.ValidationItem;
import org.dbiir.txnsails.execution.validation.ValidationLock;
import org.dbiir.txnsails.execution.validation.ValidationMetaTable;
import org.dbiir.txnsails.execution.validation.ValidationSet;
import org.junit.jupiter.api.Test;

class DistributedRegressionTest {
  @Test
  void globalValidationExcludesConcurrentWriters() {
    ValidationLock lock = new ValidationLock(1);
    assertEquals(1, lock.tryLock(1, LockType.EX, CCType.SER));
    assertEquals(-1, lock.tryLock(2, LockType.EX, CCType.SER));
  }

  @Test
  void repeatedAccessUsesOneValidationLock() throws Exception {
    ValidationSet set = new ValidationSet(1);
    set.addValidationItem("usertable", 1, LockType.SH, 4);
    set.addValidationItem("usertable", 1, LockType.EX, 5);
    assertEquals(1, set.getItemCount());
    assertEquals(LockType.EX, set.get(0).getLockType());
    assertEquals(5, set.get(0).getOldVersion());
  }

  @Test
  void validationSetGrowsBeyondInitialCapacity() throws Exception {
    ValidationSet set = new ValidationSet(1);
    for (int key = 0; key < 25; key++) {
      set.addValidationItem("usertable", key, LockType.SH, 0);
    }
    assertEquals(25, set.getValidationItems().size());
  }

  @Test
  void successfulPreparationMarksTransactionPrepared() throws Exception {
    Transaction transaction = new Transaction(123, List.of(new Participant(0)));
    TransactionManager manager = new TransactionManager();
    manager.addTransaction(transaction);
    manager.prepare(123);
    assertTrue(transaction.isPrepared());
  }

  @Test
  void readModifyWriteRetainsTheExternalReadVersion() throws Exception {
    ValidationSet set = new ValidationSet(1);
    set.addValidationItem("usertable", 1, LockType.SH, 0);
    set.addValidationItem("usertable", 1, LockType.EX, 2);
    assertEquals(0, set.get(0).getReadVersion());
    assertEquals(2, set.get(0).getWriteVersion());
    ValidationMetaTable table = ValidationMetaTable.getInstance();
    table.initDistributed("ycsb", List.of());
    table.updateHotspotVersion("usertable", 1, 1);
    assertThrows(SQLException.class, () -> set.get(0).validate(10));
    new ValidationItem("usertable", 1, LockType.EX, 3).validate(11);
  }

  @Test
  void readYourWritesDoesNotCreateAnExternalRead() throws Exception {
    ValidationSet set = new ValidationSet(1);
    set.addValidationItem("usertable", 1, LockType.EX, 1);
    set.addValidationItem("usertable", 1, LockType.SH, 1);
    assertEquals(-1, set.get(0).getReadVersion());
    assertEquals(1, set.getItemCount());
  }

  @Test
  void nonRepeatableReadsAbort() throws Exception {
    ValidationSet set = new ValidationSet(1);
    set.addValidationItem("usertable", 1, LockType.SH, 0);
    assertThrows(SQLException.class, () -> set.addValidationItem("usertable", 1, LockType.SH, 1));
  }

  @Test
  void compositeRowsWithTheSamePlacementKeyRemainDistinct() throws Exception {
    ValidationSet set = new ValidationSet(1);
    set.addValidationItem(
        new ValidationItem("order_line", 0, LockType.EX, 1)
            .withPrimaryKey(java.util.Map.of("ol_w_id", 1L, "ol_number", 1L)));
    set.addValidationItem(
        new ValidationItem("order_line", 0, LockType.SH, 1)
            .withPrimaryKey(java.util.Map.of("ol_number", 2L, "ol_w_id", 1L)));
    assertEquals(2, set.getItemCount());
    assertEquals(1, set.get(1).getReadVersion());
    assertEquals(-1, set.get(1).getWriteVersion());
  }

  @Test
  void durableIdsAreNotReusedAfterReconfiguration(
      @org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
    java.nio.file.Path checkpoint = directory.resolve("instance7.ids");
    TransactionIdGenerator.configure(checkpoint, 7);
    long first = TransactionIdGenerator.generateTransactionId(7);
    TransactionIdGenerator.configure(checkpoint, 7);
    long second = TransactionIdGenerator.generateTransactionId(7);
    assertTrue(second > first);
    assertEquals(7, TransactionIdGenerator.extractId(second));
    java.nio.file.Files.writeString(checkpoint, "corrupt");
    assertThrows(java.io.IOException.class, () -> TransactionIdGenerator.configure(checkpoint, 7));
  }

  @Test
  void transactionIdsAreUniqueWithinOneMillisecond() {
    long previous = TransactionIdGenerator.generateTransactionId(0);
    for (int i = 0; i < 10_000; i++) {
      long current = TransactionIdGenerator.generateTransactionId(0);
      assertNotEquals(previous, current);
      previous = current;
    }
  }
}
