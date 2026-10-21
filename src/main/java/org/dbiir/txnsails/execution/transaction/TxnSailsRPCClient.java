package org.dbiir.txnsails.execution.transaction;

import java.io.*;
import java.net.*;
import java.sql.SQLException;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.common.messages.Message;

/** A serialized request/response channel; connections are established lazily. */
public class TxnSailsRPCClient implements AutoCloseable {
  private final String host;
  private final int port;
  private Socket socket;
  private DataInputStream input;
  private DataOutputStream output;

  public TxnSailsRPCClient(DBInstance instance, int port) {
    this.host = instance.getHost();
    this.port = port;
  }

  public synchronized Message request(Message request) throws SQLException {
    try {
      if (socket == null || socket.isClosed()) {
        socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 2000);
        socket.setSoTimeout(10000);
        input = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
      }
      RPCWire.write(output, request);
      Message response = RPCWire.read(input);
      if (!response.getType().name().equals(request.getType().name().replace("_REQ", "_RESP"))) {
        throw new IOException("Unexpected RPC response " + response.getType());
      }
      return response;
    } catch (IOException | ClassNotFoundException | RuntimeException ex) {
      close();
      throw new SQLException(
          "Validation owner communication failed: " + host + ":" + port, "08006", ex);
    }
  }

  @Override
  public synchronized void close() {
    if (socket != null) {
      try {
        socket.close();
      } catch (IOException ignored) {
      }
      socket = null;
    }
  }
}
