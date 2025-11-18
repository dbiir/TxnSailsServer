package org.dbiir.txnsails.common.messages;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.types.MessageType;

@Setter
@Getter
public class FinalResponse extends Message {
  long tid;
  private Exception exception;

  public FinalResponse(Exception exception) {
    super(MessageType.FINAL_RESP);
    this.exception = exception;
  }

  public FinalResponse(long tid) {
    super(MessageType.FINAL_RESP);
    this.tid = tid;
    this.exception = null;
  }
}