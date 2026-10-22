package org.dbiir.txnsails.common.messages;

import java.util.List;
import lombok.Getter;
import org.dbiir.txnsails.common.TransactionIdentity;
import org.dbiir.txnsails.common.types.MessageType;
import org.dbiir.txnsails.execution.validation.ValidationItem;

@Getter
public class FinalRequest extends Message {
  long tid;
  int coordinatorId;
  boolean isCommit;
  ValidationItem[] items;

  public FinalRequest(long tid, boolean isCommit, List<ValidationItem> items) {
    this(new TransactionIdentity(0, tid), isCommit, items);
  }

  public FinalRequest(
      TransactionIdentity transaction, boolean isCommit, List<ValidationItem> items) {
    super(MessageType.FINAL_REQ);
    this.tid = transaction.sequence();
    this.coordinatorId = transaction.coordinatorId();
    this.isCommit = isCommit;
    this.items = items.toArray(new ValidationItem[0]);
  }

  public TransactionIdentity getTransactionIdentity() {
    return new TransactionIdentity(coordinatorId, tid);
  }
}
