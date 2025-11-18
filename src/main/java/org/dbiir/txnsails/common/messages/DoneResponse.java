package org.dbiir.txnsails.common.messages;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.types.MessageType;

@Setter
@Getter
public class DoneResponse extends Message {
  private int instanceID;
  private Exception exception;

  public DoneResponse(int instanceId, Exception exception) {
    super(MessageType.DONE_RESP);
    this.instanceID = instanceId;
    this.exception = exception;
  }

  public DoneResponse(int instanceId) {
    super(MessageType.DONE_RESP);
    this.instanceID = instanceId;
    this.exception = null;
  }
}