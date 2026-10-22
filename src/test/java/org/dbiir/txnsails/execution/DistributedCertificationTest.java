package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.validation.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DistributedCertificationTest {
  private static final CertificationContext LOCAL_SER =
      context(false, true, Connection.TRANSACTION_SERIALIZABLE);
  private static final CertificationContext LOCAL_SI_RO =
      context(false, false, Connection.TRANSACTION_REPEATABLE_READ);
  private static final CertificationContext LOCAL_SI_RW =
      context(false, true, Connection.TRANSACTION_REPEATABLE_READ);
  private static final CertificationContext GLOBAL =
      context(true, false, Connection.TRANSACTION_SERIALIZABLE);

  private static CertificationContext context(boolean global, boolean writes, int isolation) {
    return new CertificationContext(global, writes, isolation, false);
  }

  @BeforeEach
  void reset() {
    ValidationMetaTable.getInstance().initDistributed("ycsb", List.of());
  }

  private ValidationItem read(long version) {
    return new ValidationItem("usertable", 1, LockType.SH, version);
  }

  private ValidationItem write(long version) {
    return new ValidationItem("usertable", 1, LockType.EX, version);
  }

  @Test
  void policyProtectsSiPivotsAndAllRcReads() {
    assertTrue(LOCAL_SI_RW.strictRead());
    assertFalse(LOCAL_SI_RO.strictRead());
    assertFalse(LOCAL_SER.strictRead());
    assertTrue(GLOBAL.strictRead());
    assertTrue(context(false, false, Connection.TRANSACTION_READ_COMMITTED).strictRead());
    assertTrue(CertificationContext.FULL.strictRead());
  }

  @Test
  void localSerAndSiReadOnlySkipLocalVersionChecksWithoutAuxiliaryQueries() throws Exception {
    for (CertificationContext relaxed : List.of(LOCAL_SER, LOCAL_SI_RO)) {
      ValidationItem writer = write(2);
      writer.validate(1, LOCAL_SER);
      writer.doAfterCommit(1, true);
      ValidationItem reader = read(0);
      reader.validate(2, relaxed);
      reader.doAfterCommit(2, true);
    }
  }

  @Test
  void globalWatermarkSurvivesLocalOverwritesAndIdleLocks() throws Exception {
    ValidationItem global = write(1);
    global.validate(1, GLOBAL);
    global.doAfterCommit(1, true);
    ValidationItem local = write(2);
    local.validate(2, LOCAL_SER);
    local.doAfterCommit(2, true);
    assertThrows(SQLException.class, () -> read(0).validate(3, LOCAL_SER));
    ValidationItem reader = read(1);
    reader.validate(4, LOCAL_SER);
    reader.doAfterCommit(4, true);
  }

  @Test
  void failedGlobalWritesDoNotPublishWatermarks() throws Exception {
    ValidationItem writer = write(1);
    writer.validate(1, GLOBAL);
    writer.doAfterCommit(1, false);
    ValidationItem reader = read(0);
    reader.validate(2, LOCAL_SER);
    reader.doAfterCommit(2, true);
  }

  @Test
  void compatibilityIsSymmetricAndRetainsReadModifyWriteRoles() throws Exception {
    for (CertificationContext readerContext :
        List.of(LOCAL_SER, LOCAL_SI_RO, LOCAL_SI_RW, GLOBAL)) {
      for (CertificationContext writerContext : List.of(LOCAL_SER, GLOBAL)) {
        for (boolean writerFirst : List.of(false, true)) {
          DistributedValidationEntry entry = new DistributedValidationEntry();
          boolean conflict = readerContext.strictRead() || writerContext.global();
          if (writerFirst) {
            assertTrue(entry.tryAcquire(1, false, true, writerContext));
            assertEquals(!conflict, entry.tryAcquire(2, true, false, readerContext));
          } else {
            assertTrue(entry.tryAcquire(1, true, false, readerContext));
            assertEquals(!conflict, entry.tryAcquire(2, false, true, writerContext));
          }
          entry.release(1);
          entry.release(2);
          assertTrue(entry.tryAcquire(3, true, true, GLOBAL));
          assertFalse(entry.tryAcquire(4, true, false, LOCAL_SER));
          entry.release(3);
          assertTrue(entry.tryAcquire(4, true, false, LOCAL_SER));
        }
      }
    }
  }

  @Test
  void repeatedAcquisitionMustKeepItsOriginalProfile() {
    DistributedValidationEntry entry = new DistributedValidationEntry();
    assertTrue(entry.tryAcquire(1, true, false, LOCAL_SER));
    assertTrue(entry.tryAcquire(1, true, false, LOCAL_SER));
    assertFalse(entry.tryAcquire(1, true, false, GLOBAL));
    entry.release(2);
    assertFalse(entry.tryAcquire(3, false, true, GLOBAL));
    entry.release(1);
    assertTrue(entry.tryAcquire(3, false, true, GLOBAL));
  }

  @Test
  void strictReadsStillRejectLocalOverwritesAndReleaseOnFailure() throws Exception {
    ValidationMetaTable.getInstance().updateDistributedVersion("usertable", "1", 2);
    for (CertificationContext strict : List.of(GLOBAL, LOCAL_SI_RW, CertificationContext.FULL)) {
      assertThrows(SQLException.class, () -> read(0).validate(1, strict));
      ValidationItem writer = write(3);
      writer.validate(2, GLOBAL);
      writer.doAfterCommit(2, false);
    }
  }
}
