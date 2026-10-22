package org.dbiir.txnsails.common.messages;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.TransactionIdentity;
import org.dbiir.txnsails.common.types.MessageType;
import org.dbiir.txnsails.execution.validation.CertificationContext;
import org.dbiir.txnsails.execution.validation.ValidationItem;

@Getter
@Setter
public class ValidationRequest extends Message {
  private long tid;
  private int coordinatorId;
  private ValidationItem[] items;
  private CertificationContext context;
  private String catalogueFingerprint;

  public ValidationRequest(long tid, List<ValidationItem> items) {
    this(tid, items, CertificationContext.FULL);
  }

  public ValidationRequest(long tid, List<ValidationItem> items, CertificationContext context) {
    this(tid, items, context, null);
  }

  public ValidationRequest(
      long tid,
      List<ValidationItem> items,
      CertificationContext context,
      String catalogueFingerprint) {
    this(new TransactionIdentity(0, tid), items, context, catalogueFingerprint);
  }

  public ValidationRequest(
      TransactionIdentity transaction,
      List<ValidationItem> items,
      CertificationContext context,
      String catalogueFingerprint) {
    super(MessageType.VALIDATION_REQ);
    this.tid = transaction.sequence();
    this.coordinatorId = transaction.coordinatorId();
    this.items = items.toArray(new ValidationItem[0]);
    this.context = context;
    this.catalogueFingerprint = catalogueFingerprint;
  }

  public TransactionIdentity getTransactionIdentity() {
    return new TransactionIdentity(coordinatorId, tid);
  }

  @Override
  public String toString() {
    String itemString = "";
    for (ValidationItem item : this.items) {
      itemString += item.getKey();
      itemString += " ";
    }
    return "Tid #" + tid + "; Items: " + itemString;
  }
}
