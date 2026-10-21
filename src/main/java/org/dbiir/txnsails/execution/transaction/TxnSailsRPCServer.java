package org.dbiir.txnsails.execution.transaction;

import java.io.*;
import java.net.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import org.dbiir.txnsails.common.messages.*;
import org.dbiir.txnsails.execution.WorkloadConfiguration;

public class TxnSailsRPCServer implements Runnable {
  public static final int DEFAULT_PORT = 23333;
  private final ServerSocket serverSocket;
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
  private final ConcurrentMap<Long, ValidationRecord> records = new ConcurrentHashMap<>();
  private volatile boolean closed;
  private static final int COMPLETED_HISTORY_LIMIT = 4096;
  private final Map<Long, Boolean> completed = new LinkedHashMap<>();
  private final Map<Integer, Long> rejectedThrough = new HashMap<>();

  private static class ValidationRecord {
    Participant participant;
    SQLException failure;
    boolean finished;
    org.dbiir.txnsails.execution.validation.ValidationItem[] items;
  }

  public TxnSailsRPCServer(int port, WorkloadConfiguration configuration) {
    try {
      serverSocket = new ServerSocket(port);
    } catch (IOException ex) {
      throw new IllegalStateException("Cannot bind validation RPC port " + port, ex);
    }
  }

  public int getPort() {
    return serverSocket.getLocalPort();
  }

  @Override
  public void run() {
    try {
      while (!closed) {
        Socket socket = serverSocket.accept();
        sockets.add(socket);
        executor.submit(() -> handle(socket));
      }
    } catch (IOException ex) {
      if (!closed) throw new IllegalStateException("Validation RPC server failed", ex);
    } finally {
      executor.shutdown();
    }
  }

  private void handle(Socket socket) {
    try (socket;
        DataInputStream input =
            new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        DataOutputStream output =
            new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
      while (!closed) {
        Message request = RPCWire.read(input);
        Message response =
            switch (request.getType()) {
              case VALIDATION_REQ -> validate((ValidationRequest) request);
              case FINAL_REQ -> finish((FinalRequest) request);
              case DONE_REQ -> new DoneResponse(((DoneRequest) request).getInstanceID());
              default -> throw new IOException("Unsupported RPC request " + request.getType());
            };
        RPCWire.write(output, response);
      }
    } catch (EOFException | SocketException ignored) {
      // Validated locks survive the connection: the coordinator may have prepared or committed.
    } catch (IOException | ClassNotFoundException ex) {
      if (!closed) System.err.println("Validation RPC communication failed: " + ex.getMessage());
    } finally {
      sockets.remove(socket);
    }
  }

  private ValidationResponse validate(ValidationRequest request) {
    ValidationResponse response = new ValidationResponse(request.getTid());
    ValidationRecord record;
    synchronized (completed) {
      int coordinator = (int) (request.getTid() & 255);
      if (completed.containsKey(request.getTid())
          || (!records.containsKey(request.getTid())
              && request.getTid() <= rejectedThrough.getOrDefault(coordinator, -1L))) {
        response.setException(
            new SQLException("Transaction is finalized or too old to validate", "40001"));
        return response;
      }
      record = records.computeIfAbsent(request.getTid(), ignored -> new ValidationRecord());
    }
    synchronized (record) {
      if (record.finished) {
        response.setException(new SQLException("Transaction was already finalized", "25000"));
      } else if (record.participant != null || record.failure != null) {
        boolean same = record.items != null && record.items.length == request.getItems().length;
        for (int i = 0; same && i < record.items.length; i++) {
          same = record.items[i].sameAccess(request.getItems()[i]);
        }
        response.setException(
            same
                ? record.failure
                : new SQLException("Duplicate transaction ID with different accesses", "40001"));
      } else {
        record.participant = new Participant(0);
        record.items = request.getItems();
        try {
          for (var item : request.getItems()) record.participant.addValidationItem(item);
          record.participant.validate(request.getTid());
        } catch (SQLException | RuntimeException ex) {
          record.failure =
              ex instanceof SQLException sql
                  ? sql
                  : new SQLException("Validation failed", "40001", ex);
          response.setException(record.failure);
        }
      }
    }
    return response;
  }

  private FinalResponse finish(FinalRequest request) {
    ValidationRecord record;
    synchronized (completed) {
      if (completed.containsKey(request.getTid())) {
        FinalResponse response = new FinalResponse(request.getTid());
        if (completed.get(request.getTid()) != request.isCommit()) {
          response.setException(new SQLException("Conflicting finalization decision", "08007"));
        }
        return response;
      }
      record = records.computeIfAbsent(request.getTid(), ignored -> new ValidationRecord());
    }
    synchronized (record) {
      if (!record.finished) {
        if (record.participant != null && record.failure == null) {
          record.participant.doAfterCommit(request.getTid(), request.isCommit());
        }
        record.participant = null;
        record.items = null;
        record.finished = true;
        synchronized (completed) {
          records.remove(request.getTid(), record);
          completed.put(request.getTid(), request.isCommit());
          while (completed.size() > COMPLETED_HISTORY_LIMIT) {
            long evicted = completed.keySet().iterator().next();
            completed.remove(evicted);
            int coordinator = (int) (evicted & 255);
            rejectedThrough.merge(coordinator, evicted, Math::max);
          }
        }
      }
    }
    return new FinalResponse(request.getTid());
  }

  public void closeServer() {
    closed = true;
    try {
      serverSocket.close();
    } catch (IOException ignored) {
    }
    sockets.forEach(
        socket -> {
          try {
            socket.close();
          } catch (IOException ignored) {
          }
        });
    executor.shutdownNow();
  }
}
