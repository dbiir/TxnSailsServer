package org.dbiir.txnsails.common.messages;

import lombok.Getter;
import org.dbiir.txnsails.common.types.MessageType;
import org.dbiir.txnsails.execution.validation.ValidationItem;

import java.util.List;

@Getter
public class FinalRequest extends Message {
  long tid;
  boolean isCommit;
  ValidationItem[] items;

  public FinalRequest(long tid, boolean isCommit, List<ValidationItem> items) {
    super(MessageType.FINAL_REQ);
    this.tid = tid;
    this.isCommit = isCommit;
    this.items = items.toArray(new ValidationItem[0]);
  }
}