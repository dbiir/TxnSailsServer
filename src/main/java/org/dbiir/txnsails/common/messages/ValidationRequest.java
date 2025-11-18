package org.dbiir.txnsails.common.messages;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.types.MessageType;
import org.dbiir.txnsails.execution.validation.ValidationItem;

import java.util.List;

@Getter
@Setter
public class ValidationRequest extends Message {
  private long tid;
  private ValidationItem[] items;

  public ValidationRequest(long tid, List<ValidationItem> items) {
    super(MessageType.VALIDATION_REQ);
    this.tid = tid;
    this.items = items.toArray(new ValidationItem[0]);
  }

  @Override
  public String toString() {
    String itemString = "";
    for (ValidationItem item: this.items) {
      itemString += item.getKey();
      itemString += " ";
    }
    return "Tid #" + tid + "; Items: " + itemString;
  }
}
