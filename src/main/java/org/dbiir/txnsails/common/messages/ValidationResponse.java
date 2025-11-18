package org.dbiir.txnsails.common.messages;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.types.MessageType;

@Setter
@Getter
public class ValidationResponse extends Message {
  long tid;
  private Exception exception;

  public ValidationResponse(Exception exception) {
    super(MessageType.VALIDATION_RESP);
    this.exception = exception;
  }

  public ValidationResponse(long tid) {
    super(MessageType.VALIDATION_RESP);
    this.tid = tid;
    this.exception = null;
  }
}
