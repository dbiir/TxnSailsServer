package org.dbiir.txnsails.execution.validation;

import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.LockType;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.logging.Logger;

public class ValidationItem implements Serializable {
  private static final long serialVersionUID = 500L;

  private static final Logger logger = Logger.getLogger(ValidationItem.class.getName());
  private String relationName;
  private long key;
  private long oldVersion;
  private LockType lockType;

  public String getRelationName() { return relationName; }
  public void setRelationName(String relationName) { this.relationName = relationName; }

  public long getKey() { return key; }
  public void setKey(long key) { this.key = key; }

  public long getOldVersion() { return oldVersion; }
  public void setOldVersion(long oldVersion) { this.oldVersion = oldVersion; }

  public LockType getLockType() { return lockType; }
  public void setLockType(LockType lockType) { this.lockType = lockType; }

  public ValidationItem(String relationName, long key, LockType lockType,long oldVersion) {
    this.relationName = relationName;
    this.key = key;
    this.oldVersion = oldVersion;
    this.lockType = lockType;
  }

  public ValidationItem(String relationName, long key, LockType lockType) {
    this.relationName = relationName;
    this.key = key;
    this.oldVersion = -1;
    this.lockType = lockType;
  }

  public ValidationItem() {
    this.relationName = null;
    this.key = -1;
    this.oldVersion = -1;
    this.lockType = null;
  }

  public void validate(long tid) throws SQLException {
    // add validation lock
    // System.out.println(Thread.currentThread().getName() + " tid #" + tid  + " try validation lock #" + key + " relation #" + relationName + " type: " + lockType);
    ValidationMetaTable.getInstance().tryValidationLock(relationName, tid, key, lockType, CCType.NUM_CC);
    if (lockType == LockType.EX) {
      return;
    }
    // validation
    long lastestVersion = ValidationMetaTable.getInstance().getHotspotVersion(relationName, key);
    if (lastestVersion < 0) {
      lastestVersion = ValidationMetaTable.getInstance().fetchUnknownVersionCache(relationName, (int) key);
    }
    if (lastestVersion != oldVersion) {
      String msg = String.format("Validation failed for key #%d, %s, lastestVersion: %d, oldVersion: %d", key, relationName, lastestVersion, oldVersion);
      throw new SQLException(msg, "500");
    }
  }

  public void doAfterCommit(long tid, boolean isCommit) {
    if (isCommit && lockType == LockType.EX) {
      ValidationMetaTable.getInstance().updateHotspotVersion(relationName, key, oldVersion);
      // System.out.println(Thread.currentThread().getName() + " update #" + key + " relation #" + relationName + " oldVersion: " + oldVersion);
    }
    ValidationMetaTable.getInstance().releaseValidationLock(relationName, key, lockType);
    // System.out.println(Thread.currentThread().getName() + " release #" + key + " relation #" + relationName + " type: " + lockType);
  }

  public void releaseValidationLock(long tid) {
    ValidationMetaTable.getInstance().releaseValidationLock(relationName, key, lockType);
  }

  public void copy(ValidationItem item) {
    this.relationName = item.relationName;
    this.key = item.key;
    this.oldVersion = item.oldVersion;
    this.lockType = item.lockType;
  }

  public void copy(String relationName, long key, LockType lockType, long oldVersion) {
    this.relationName = relationName;
    this.key = key;
    this.oldVersion = oldVersion;
    this.lockType = lockType;
  }

  @Override
  public String toString() {
    return "ValidationItem[" +
            "table='" + relationName +
            ", key=" + key +
            ", version=" + oldVersion +
            ", lock=" + lockType +
            ']';
  }
}

