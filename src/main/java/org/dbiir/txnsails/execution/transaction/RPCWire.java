package org.dbiir.txnsails.execution.transaction;

import java.io.*;
import org.dbiir.txnsails.common.messages.Message;

final class RPCWire {
  private static final int MAX_MESSAGE_BYTES = 8 * 1024 * 1024;

  private RPCWire() {}

  static void write(DataOutputStream output, Message message) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream objects = new ObjectOutputStream(bytes)) {
      objects.writeObject(message);
    }
    if (bytes.size() > MAX_MESSAGE_BYTES) throw new IOException("RPC message is too large");
    output.writeInt(bytes.size());
    bytes.writeTo(output);
    output.flush();
  }

  static Message read(DataInputStream input) throws IOException, ClassNotFoundException {
    int length = input.readInt();
    if (length <= 0 || length > MAX_MESSAGE_BYTES)
      throw new IOException("Invalid RPC message length");
    byte[] bytes = new byte[length];
    input.readFully(bytes);
    try (ObjectInputStream objects = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return (Message) objects.readObject();
    }
  }
}
