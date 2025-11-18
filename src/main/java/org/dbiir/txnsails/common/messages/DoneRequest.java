package org.dbiir.txnsails.common.messages;

import lombok.Getter;
import org.dbiir.txnsails.common.types.MessageType;

@Getter
public class DoneRequest extends Message {
  int instanceID;

  public DoneRequest(int instanceId) {
    super(MessageType.DONE_REQ);
    this.instanceID = instanceId;
  }
}
