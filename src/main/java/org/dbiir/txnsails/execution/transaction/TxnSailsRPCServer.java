package org.dbiir.txnsails.execution.transaction;

import java.io.*;
import java.net.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.*;
import org.dbiir.txnsails.common.TransactionIdentity;
import org.dbiir.txnsails.common.messages.*;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.validation.CertificationContext;

public class TxnSailsRPCServer implements Runnable {
  public static final int DEFAULT_PORT = 23333;
  private final ServerSocket serverSocket;
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
  private final ConcurrentMap<TransactionIdentity, ValidationRecord> records =
      new ConcurrentHashMap<>();
  private volatile boolean closed;
  private static final int COMPLETED_HISTORY_LIMIT = 4096;
  private final Map<TransactionIdentity, Boolean> completed = new LinkedHashMap<>();
  private final Map<Integer, Long> rejectedThrough = new HashMap<>();
  private final Integer nativeIsolation;
  private final String catalogueFingerprint;
  private final int instanceID;

  private static class ValidationRecord {
    Participant participant;
    SQLException failure;
    boolean finished;
    boolean committed;
    org.dbiir.txnsails.execution.validation.ValidationItem[] items;
    CertificationContext context;
    String catalogueFingerprint;
  }

  public TxnSailsRPCServer(int port, WorkloadConfiguration configuration) {
    instanceID = configuration.getInstanceID();
    catalogueFingerprint =
        configuration.getClosedWorkload() == null
            ? null
            : configuration.getClosedWorkload().fingerprint();
    try {
      var owner =
          configuration.getInstances().stream()
              .filter(instance -> instance.getId() == configuration.getInstanceID())
              .findFirst();
      nativeIsolation =
          owner.isPresent()
              ? owner.get().jdbcIsolation(configuration.getConcurrencyControlType())
              : null;
      serverSocket = new ServerSocket(port);
    } catch (IOException | SQLException ex) {
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
              case CATALOGUE_REQ -> new CatalogueResponse(instanceID, catalogueFingerprint);
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
    TransactionIdentity transaction = request.getTransactionIdentity();
    if (!Objects.equals(catalogueFingerprint, request.getCatalogueFingerprint())) {
      response.setException(
          new SQLException("Distributed workload catalogue does not match owner", "55000"));
      return response;
    }
    CertificationContext context = request.getContext();
    if (context == null
        || (!context.conservative()
            && nativeIsolation != null
            && context.isolation() != nativeIsolation)) {
      response.setException(
          new SQLException("Certification isolation does not match owner", "40001"));
      return response;
    }
    ValidationRecord record;
    synchronized (completed) {
      if (completed.containsKey(transaction)
          || (!records.containsKey(transaction)
              && transaction.sequence()
                  <= rejectedThrough.getOrDefault(transaction.coordinatorId(), -1L))) {
        response.setException(
            new SQLException("Transaction is finalized or too old to validate", "40001"));
        return response;
      }
      record = records.computeIfAbsent(transaction, ignored -> new ValidationRecord());
    }
    synchronized (record) {
      if (record.finished) {
        response.setException(new SQLException("Transaction was already finalized", "25000"));
      } else if (record.participant != null || record.failure != null) {
        boolean same =
            context.equals(record.context)
                && Objects.equals(record.catalogueFingerprint, request.getCatalogueFingerprint())
                && record.items != null
                && record.items.length == request.getItems().length;
        for (int i = 0; same && i < record.items.length; i++) {
          same = record.items[i].sameAccess(request.getItems()[i]);
        }
        response.setException(
            same
                ? record.failure
                : new SQLException("Duplicate transaction ID with different accesses", "40001"));
      } else {
        record.participant = new Participant(0);
        record.context = context;
        record.catalogueFingerprint = request.getCatalogueFingerprint();
        record.participant.setCertificationContext(context);
        record.items = request.getItems();
        try {
          for (var item : request.getItems()) record.participant.addValidationItem(item);
          record.participant.validate(transaction);
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
    TransactionIdentity transaction = request.getTransactionIdentity();
    FinalResponse response = new FinalResponse(request.getTid());
    ValidationRecord record;
    synchronized (completed) {
      if (completed.containsKey(transaction)) {
        if (completed.get(transaction) != request.isCommit()) {
          response.setException(new SQLException("Conflicting finalization decision", "08007"));
        }
        return response;
      }
      record = records.computeIfAbsent(transaction, ignored -> new ValidationRecord());
    }
    synchronized (record) {
      if (record.finished) {
        if (record.committed != request.isCommit())
          response.setException(new SQLException("Conflicting finalization decision", "08007"));
      } else {
        if (record.participant != null && record.failure == null) {
          record.participant.doAfterCommit(transaction, request.isCommit());
        }
        record.participant = null;
        record.items = null;
        record.finished = true;
        record.committed = request.isCommit();
        synchronized (completed) {
          records.remove(transaction, record);
          completed.put(transaction, request.isCommit());
          while (completed.size() > COMPLETED_HISTORY_LIMIT) {
            TransactionIdentity evicted = completed.keySet().iterator().next();
            completed.remove(evicted);
            rejectedThrough.merge(evicted.coordinatorId(), evicted.sequence(), Math::max);
          }
        }
      }
    }
    return response;
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
