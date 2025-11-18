package org.dbiir.txnsails.common.messages;

import lombok.Getter;
import org.dbiir.txnsails.common.types.MessageType;

import java.io.Serializable;

public class Message implements Serializable {
  @Getter
  MessageType type;

  public Message(MessageType type) {
    this.type = type;
  }
}
