package org.dbiir.txnsails.execution.validation;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.dbiir.txnsails.common.TransactionIdentity;
import org.dbiir.txnsails.common.types.LockType;

public class ValidationItem implements Serializable {
  private static final long serialVersionUID = 501L;
  private String relationName;
  private long key;
  private long oldVersion;
  private LockType lockType;
  private long readVersion = -1;
  private long writeVersion = -1;
  private transient boolean locked;
  private transient DistributedValidationEntry lockedEntry;
  private transient TransactionIdentity lockedBy;
  private transient CertificationContext certificationContext;
  private String identity;
  private String versionQuery;
  private List<Long> primaryKeyValues = List.of();

  public ValidationItem() {}

  public ValidationItem(String table, long key, LockType type, long version) {
    this.relationName = table.toLowerCase(java.util.Locale.ROOT);
    this.key = key;
    this.lockType = type;
    this.oldVersion = version;
    if (type == LockType.SH) readVersion = version;
    else writeVersion = version;
  }

  public ValidationItem(String table, long key, LockType type) {
    this(table, key, type, -1);
  }

  public String getRelationName() {
    return relationName;
  }

  public long getKey() {
    return key;
  }

  public long getOldVersion() {
    return oldVersion;
  }

  public LockType getLockType() {
    return lockType;
  }

  public long getReadVersion() {
    return readVersion;
  }

  public long getWriteVersion() {
    return writeVersion;
  }

  public String getIdentity() {
    return identity == null ? Long.toString(key) : identity;
  }

  /** The benchmark integer selects placement; full primary-key tuples identify validation rows. */
  public ValidationItem withPrimaryKey(Map<String, Long> values) {
    Map<String, Long> ordered = new TreeMap<>(values);
    identity = ordered.toString();
    primaryKeyValues = List.copyOf(ordered.values());
    versionQuery =
        "SELECT vid FROM "
            + relationName
            + " WHERE "
            + ordered.keySet().stream()
                .map(column -> column + " = ?")
                .collect(java.util.stream.Collectors.joining(" AND "));
    return this;
  }

  public boolean sameAccess(ValidationItem other) {
    return relationName.equals(other.relationName)
        && getIdentity().equals(other.getIdentity())
        && lockType == other.lockType
        && readVersion == other.readVersion
        && writeVersion == other.writeVersion;
  }

  /** Canonical descriptor compiled once per SQL template. */
  public ValidationItem withPrimaryKey(
      String canonicalIdentity, String versionQuery, List<Long> values) {
    this.identity = canonicalIdentity;
    this.versionQuery = versionQuery;
    this.primaryKeyValues = List.copyOf(values);
    return this;
  }

  public void merge(ValidationItem addition) throws SQLException {
    if (addition.readVersion >= 0) {
      if (writeVersion >= 0) {
        if (addition.readVersion != writeVersion) {
          throw new SQLException("Read did not observe the transaction's own write", "40001");
        }
      } else if (readVersion >= 0 && readVersion != addition.readVersion) {
        throw new SQLException("Non-repeatable distributed read", "40001");
      } else {
        readVersion = addition.readVersion;
      }
    }
    if (addition.writeVersion >= 0) {
      writeVersion = addition.writeVersion;
      lockType = LockType.EX;
    }
    oldVersion = writeVersion >= 0 ? writeVersion : readVersion;
  }

  public void validate(long tid) throws SQLException {
    validate(new TransactionIdentity(0, tid), CertificationContext.FULL);
  }

  public void validate(long tid, CertificationContext context) throws SQLException {
    validate(new TransactionIdentity(0, tid), context);
  }

  public void validate(TransactionIdentity transaction) throws SQLException {
    validate(transaction, CertificationContext.FULL);
  }

  public void validate(TransactionIdentity transaction, CertificationContext context)
      throws SQLException {
    if (locked && !transaction.equals(lockedBy))
      throw new SQLException("Validation item is already held by another transaction", "40001");
    ValidationMetaTable table = ValidationMetaTable.getInstance();
    lockedEntry =
        table.acquireDistributedValidationLock(
            relationName, transaction, getIdentity(), readVersion >= 0, writeVersion >= 0, context);
    lockedBy = transaction;
    certificationContext = context;
    locked = true;
    try {
      if (readVersion >= 0) {
        long latest =
            context.strictRead()
                ? lockedEntry.latestVersion()
                : lockedEntry.latestGlobalWriteVersion();
        if (context.strictRead() && latest < 0) {
          latest =
              versionQuery == null
                  ? table.fetchUnknownVersionCache(relationName, (int) key)
                  : table.fetchDistributedVersion(
                      relationName, getIdentity(), versionQuery, primaryKeyValues);
        }
        if (context.strictRead() ? latest != readVersion : latest > readVersion) {
          throw new SQLException(
              "Stale distributed read: " + relationName + " key=" + key, "40001");
        }
      }
    } catch (SQLException | RuntimeException ex) {
      releaseValidationLock(transaction);
      throw ex;
    }
  }

  public void doAfterCommit(long tid, boolean committed) {
    doAfterCommit(new TransactionIdentity(0, tid), committed);
  }

  public void doAfterCommit(TransactionIdentity transaction, boolean committed) {
    if (!locked || !transaction.equals(lockedBy)) return;
    if (committed && writeVersion >= 0) {
      lockedEntry.publish(writeVersion, certificationContext.global());
    }
    releaseValidationLock(transaction);
  }

  public void releaseValidationLock(long tid) {
    releaseValidationLock(new TransactionIdentity(0, tid));
  }

  public void releaseValidationLock(TransactionIdentity transaction) {
    if (locked && transaction.equals(lockedBy)) {
      lockedEntry.release(transaction);
      locked = false;
      lockedEntry = null;
      lockedBy = null;
      certificationContext = null;
    }
  }

  public void copy(ValidationItem item) {
    relationName = item.relationName;
    key = item.key;
    oldVersion = item.oldVersion;
    lockType = item.lockType;
    readVersion = item.readVersion;
    writeVersion = item.writeVersion;
    identity = item.identity;
    versionQuery = item.versionQuery;
    primaryKeyValues = item.primaryKeyValues;
    locked = false;
    lockedEntry = null;
    lockedBy = null;
    certificationContext = null;
  }
}
