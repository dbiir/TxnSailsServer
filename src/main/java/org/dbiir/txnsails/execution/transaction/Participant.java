package org.dbiir.txnsails.execution.transaction;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.TransactionStatus;
import org.dbiir.txnsails.common.ValidationStatus;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.validation.ValidationItem;
import org.dbiir.txnsails.execution.validation.ValidationSet;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

@Getter
@Setter
public class Participant {
  private static final int MAX_VALIDATION_ITEM_COUNT = 10;
  private int instanceID; // the instance id of the participant
  private TransactionStatus status;
  private Connection connection;
  private ValidationStatus validationStatus;
  private ValidationSet validationSet;

  public Participant(int instanceId, TransactionStatus status, Connection connection, ValidationSet validationSet) {
    this.instanceID = instanceId;
    this.status = status;
    this.connection = connection;
    this.validationSet = validationSet;
    this.validationStatus = ValidationStatus.VALIDATION_STATUS_NUM;
  }

  public Participant(int instanceId, Connection connection) {
    this.instanceID = instanceId;
    this.status = TransactionStatus.ACTIVE;
    this.connection = connection;
    this.validationSet = new ValidationSet(MAX_VALIDATION_ITEM_COUNT);
    this.validationStatus = ValidationStatus.VALIDATION_STATUS_NUM;
  }

  public Participant(int instanceId) {
    this.instanceID = instanceId;
    this.status = TransactionStatus.ACTIVE;
    this.connection = null;
    this.validationSet = new ValidationSet(MAX_VALIDATION_ITEM_COUNT);
    this.validationStatus = ValidationStatus.VALIDATION_STATUS_NUM;
  }

  public void addValidationItem(ValidationItem item) {
    this.validationSet.addValidationItem(item);
  }

  public void addValidationItem(String relationName, long key, LockType lockType, long oldVersion) {
    this.validationSet.addValidationItem(relationName, key, lockType, oldVersion);
  }

  public void addValidationItems(List<ValidationItem> items) {
    this.validationSet.addValidationItems(items);
  }

  public void validate(long tid) throws SQLException {
    // System.out.println("local validate " + tid);
    int itemCount = validationSet.getItemCount();
    for (int validationPhase = 0; validationPhase < itemCount; validationPhase++) {
      try {
        validationSet.get(validationPhase).validate(tid);
      } catch (SQLException ex) {
        releaseValidationLocks(tid, validationPhase);
        validationStatus = ValidationStatus.FAILED;
        throw ex;
      }
    }
    validationStatus = ValidationStatus.VALIDATED;
    // System.out.println("local validate finished " + validationStatus);
  }

  private void releaseValidationLocks(long tid, int validationPhase) {
    for (int i = 0; i < validationPhase; i++) {
      validationSet.get(i).releaseValidationLock(tid);
    }
  }

  public void doAfterCommit(long tid, boolean isCommit) {
    int itemCount = validationSet.getItemCount();
    for (int i = 0; i < itemCount; i++) {
      validationSet.get(i).doAfterCommit(tid, isCommit);
    }
  }

  public void reset() {
    validationSet.reset();
    status = TransactionStatus.ACTIVE;
    validationStatus = ValidationStatus.VALIDATION_STATUS_NUM;
  }
}
