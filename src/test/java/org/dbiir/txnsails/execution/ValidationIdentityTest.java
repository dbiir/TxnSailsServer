package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.List;
import org.dbiir.txnsails.common.TransactionIdentity;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.transaction.Participant;
import org.dbiir.txnsails.execution.validation.CertificationContext;
import org.dbiir.txnsails.execution.validation.DistributedValidationEntry;
import org.dbiir.txnsails.execution.validation.ValidationItem;
import org.dbiir.txnsails.execution.validation.ValidationMetaTable;
import org.junit.jupiter.api.Test;

class ValidationIdentityTest {
  @Test
  void equalSequencesOnFullCoordinatorIdsNeverAliasLockHolders() {
    var entry = new DistributedValidationEntry();
    var first = new TransactionIdentity(0, 17);
    var second = new TransactionIdentity(256, 17);
    var third = new TransactionIdentity(Integer.MAX_VALUE, 17);
    assertTrue(entry.tryAcquire(first, false, true, CertificationContext.FULL));
    assertFalse(entry.tryAcquire(second, false, true, CertificationContext.FULL));
    entry.release(second);
    entry.release(third);
    assertFalse(entry.tryAcquire(third, false, true, CertificationContext.FULL));
    entry.release(first);
    assertTrue(entry.tryAcquire(second, false, true, CertificationContext.FULL));
    entry.release(first);
    assertFalse(entry.tryAcquire(third, false, true, CertificationContext.FULL));
    entry.release(second);
    assertTrue(entry.tryAcquire(third, false, true, CertificationContext.FULL));
    entry.release(third);
  }

  @Test
  void itemFinalizationByAnotherCoordinatorCannotPublishOrLoseItsLock() throws Exception {
    var table = ValidationMetaTable.getInstance();
    table.initDistributed("ycsb", List.of());
    var first = new TransactionIdentity(0, 17);
    var other = new TransactionIdentity(256, 17);
    var writer = new ValidationItem("usertable", 7, LockType.EX, 1);
    var contender = new ValidationItem("usertable", 7, LockType.EX, 2);
    writer.validate(first);
    writer.releaseValidationLock(other);
    writer.doAfterCommit(other, true);
    assertEquals(-1, table.getDistributedVersion("usertable", "7"));
    assertThrows(SQLException.class, () -> writer.validate(other));
    assertThrows(SQLException.class, () -> contender.validate(other));
    writer.doAfterCommit(first, true);
    assertEquals(1, table.getDistributedVersion("usertable", "7"));
    contender.validate(other);
    contender.doAfterCommit(other, false);
  }

  @Test
  void participantFailureReleasesOnlyItsOwnFullIdentity() throws Exception {
    ValidationMetaTable.getInstance().initDistributed("ycsb", List.of());
    var first = new TransactionIdentity(0, 17);
    var second = new TransactionIdentity(256, 17);
    var third = new TransactionIdentity(Integer.MAX_VALUE, 17);
    var owner = new Participant(0);
    owner.addValidationItem("usertable", 7, LockType.EX, 1);
    owner.validate(first);
    var failing = new Participant(0);
    failing.addValidationItem("usertable", 8, LockType.EX, 1);
    failing.addValidationItem("usertable", 7, LockType.EX, 1);
    assertThrows(SQLException.class, () -> failing.validate(second));
    var conflict = new ValidationItem("usertable", 7, LockType.EX, 2);
    assertThrows(SQLException.class, () -> conflict.validate(third));
    var released = new ValidationItem("usertable", 8, LockType.EX, 2);
    released.validate(third);
    released.doAfterCommit(third, false);
    owner.doAfterCommit(first, false);
    conflict.validate(third);
    conflict.doAfterCommit(third, false);
  }
}
