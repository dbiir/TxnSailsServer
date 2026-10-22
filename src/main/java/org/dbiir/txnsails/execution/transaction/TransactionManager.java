package org.dbiir.txnsails.execution.transaction;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.dbiir.txnsails.common.*;
import org.dbiir.txnsails.common.messages.*;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.validation.CertificationContext;

/** Coordinates database 2PC and owner-side validation. Commit uncertainty fails closed. */
public class TransactionManager {
  private static final TransactionManager INSTANCE = new TransactionManager();
  private final Map<Long, Transaction> transactions = new ConcurrentHashMap<>();
  private final Map<Integer, TxnSailsRPCClient> clients = new ConcurrentHashMap<>();
  private int instanceID;
  private TxnSailsRPCServer rpcServer;
  private Thread rpcServerThread;
  private boolean optimizedValidation;
  private String catalogueFingerprint;
  private volatile boolean catalogueAgreed;

  public void init(WorkloadConfiguration configuration) {
    instanceID = configuration.getInstanceID();
    optimizedValidation = configuration.isOptimizedDistributedValidation();
    catalogueFingerprint =
        configuration.getClosedWorkload() == null
            ? null
            : configuration.getClosedWorkload().fingerprint();
    catalogueAgreed = false;
    DBInstance local =
        configuration.getInstances().stream()
            .filter(instance -> instance.getId() == instanceID)
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException("Missing local database instance " + instanceID));
    rpcServer = new TxnSailsRPCServer(local.getRpcPort(), configuration);
    rpcServerThread = new Thread(rpcServer, "txnsails-validation-" + instanceID);
    rpcServerThread.start();
    for (DBInstance instance : configuration.getInstances()) {
      if (instance.getId() != instanceID) {
        clients.put(instance.getId(), new TxnSailsRPCClient(instance, instance.getRpcPort()));
      }
    }
  }

  public void close() {
    clients.values().forEach(TxnSailsRPCClient::close);
    if (rpcServer != null) rpcServer.closeServer();
    if (rpcServerThread != null) rpcServerThread.interrupt();
  }

  /**
   * Immutable catalogue epochs require agreement from every owner before any pruned work starts.
   */
  public synchronized void ensureCatalogueAgreement() throws SQLException {
    if (catalogueFingerprint == null || catalogueAgreed) return;
    for (var peer : clients.entrySet()) {
      CatalogueResponse response =
          (CatalogueResponse) peer.getValue().request(new CatalogueRequest());
      if (response.getInstanceID() != peer.getKey()
          || !catalogueFingerprint.equals(response.getFingerprint())) {
        throw new SQLException(
            "Distributed workload catalogue does not match owner " + peer.getKey(), "55000");
      }
    }
    catalogueAgreed = true;
  }

  public void setParticipantsFinish(int ignored) {}

  public void setParticipantsFinishAck(int ignored) {}

  public void initTransaction(long tid) {
    addTransaction(new Transaction(tid));
  }

  public void addTransaction(Transaction transaction) {
    if (transactions.putIfAbsent(transaction.getTid(), transaction) != null) {
      throw new IllegalStateException("Duplicate transaction ID " + transaction.getTid());
    }
  }

  public void removeTransaction(Transaction transaction) {
    transactions.remove(transaction.getTid(), transaction);
  }

  public void removeTransaction(long tid) {
    transactions.remove(tid);
  }

  private Transaction getTransaction(long tid) throws SQLException {
    Transaction transaction = transactions.get(tid);
    if (transaction == null) throw new SQLException("Unknown transaction " + tid, "25000");
    return transaction;
  }

  private List<Participant> participants(Transaction transaction) {
    return transaction.getParticipants().stream()
        .sorted(Comparator.comparingInt(Participant::getInstanceID))
        .toList();
  }

  private TxnSailsRPCClient client(int id) throws SQLException {
    TxnSailsRPCClient client = clients.get(id);
    if (client == null) throw new SQLException("No validation owner for database " + id, "08001");
    return client;
  }

  public void prepare(long tid) throws SQLException {
    ensureCatalogueAgreement();
    Transaction transaction = getTransaction(tid);
    if (transaction.getStatus() == TransactionStatus.COMMITTING
        || transaction.getStatus() == TransactionStatus.COMMITTED
        || transaction.getStatus() == TransactionStatus.PREPARE_UNKNOWN) {
      throw new SQLException("Transaction outcome is unresolved: " + tid, "08007");
    }
    if (transaction.getStatus() != TransactionStatus.ACTIVE) {
      throw new SQLException("Transaction is not active", "25000");
    }
    if (transaction.isRollbackOnly())
      throw new SQLException("Transaction is rollback-only", "25P02");
    List<Participant> ordered = participants(transaction);
    boolean global = ordered.size() > 1;
    boolean writes = ordered.stream().anyMatch(p -> p.getValidationSet().hasWrites());
    // Freeze every branch's policy before acquiring the first validation lock.
    for (Participant participant : ordered) {
      participant.setCertificationContext(
          optimizedValidation
              ? new CertificationContext(global, writes, participant.getNativeIsolation(), false)
              : CertificationContext.FULL);
    }
    for (Participant participant : ordered) {
      if (participant.getInstanceID() == instanceID) {
        participant.validate(identity(tid));
      } else {
        participant.setValidationStatus(ValidationStatus.VALIDATING);
        ValidationResponse response =
            (ValidationResponse)
                client(participant.getInstanceID())
                    .request(
                        new ValidationRequest(
                            identity(tid),
                            participant.getValidationSet().getValidationItems(),
                            participant.getCertificationContext(),
                            catalogueFingerprint));
        if (response.getException() != null) {
          participant.setValidationStatus(ValidationStatus.FAILED);
          throw new SQLException("Remote validation failed", "40001", response.getException());
        }
        participant.setValidationStatus(ValidationStatus.VALIDATED);
      }
    }
    if (transaction.getParticipants().size() > 1) {
      for (Participant participant : ordered) {
        try {
          execute(participant, "PREPARE TRANSACTION '" + branchName(tid, participant) + "'");
          participant.setStatus(TransactionStatus.PREPARED);
        } catch (SQLException ex) {
          String state = ex.getSQLState();
          boolean rejected =
              state != null
                  && (state.startsWith("22")
                      || state.startsWith("23")
                      || state.startsWith("40")
                      || state.startsWith("42")
                      || state.equals("0A000")
                      || state.equals("25P02")
                      || state.equals("55000"));
          if (!rejected) {
            participant.setStatus(TransactionStatus.PREPARE_UNKNOWN);
            transaction.setStatus(TransactionStatus.PREPARE_UNKNOWN);
            throw new SQLException("Distributed prepare outcome is in doubt: " + tid, "08007", ex);
          }
          participant.setStatus(TransactionStatus.PREPARE_FAILED);
          throw ex;
        }
      }
    }
    transaction.setPrepared(true);
    transaction.setStatus(TransactionStatus.PREPARED);
  }

  public void commit(long tid, Map<Integer, AsyncResultWrapper> results) throws SQLException {
    Transaction transaction = getTransaction(tid);
    List<Participant> ordered = participants(transaction);
    if (!transaction.isPrepared() || transaction.isRollbackOnly()) {
      throw new SQLException("Transaction has not prepared successfully", "25000");
    }
    for (Participant participant : ordered) {
      if (participant.getValidationStatus() != ValidationStatus.VALIDATED) {
        throw new SQLException("Participant is not validated", "25000");
      }
    }
    transaction.setStatus(TransactionStatus.COMMITTING);
    SQLException failure = null;
    for (Participant participant : ordered) {
      try {
        if (participant.getStatus() == TransactionStatus.COMMITTED) continue;
        execute(
            participant,
            participant.getStatus() == TransactionStatus.PREPARED
                ? "COMMIT PREPARED '" + branchName(tid, participant) + "'"
                : "COMMIT");
        participant.setStatus(TransactionStatus.COMMITTED);
      } catch (SQLException ex) {
        // PostgreSQL has definitively rolled back these ordinary-COMMIT failures.
        // A native prepared branch still follows the irrevocable global decision.
        if (ordered.size() == 1
            && participant.getStatus() != TransactionStatus.PREPARED
            && ("40001".equals(ex.getSQLState()) || "40P01".equals(ex.getSQLState()))) {
          participant.setStatus(TransactionStatus.ROLLBACK);
          transaction.setRollbackOnly(true);
          transaction.setStatus(TransactionStatus.ROLLBACK);
          throw ex;
        }
        if (results != null && results.get(participant.getInstanceID()) != null) {
          results.get(participant.getInstanceID()).setException(ex);
        }
        if (failure == null)
          failure = new SQLException("Distributed commit outcome is in doubt: " + tid, "08007", ex);
        else failure.addSuppressed(ex);
      }
    }
    if (failure != null) throw failure;
    transaction.setStatus(TransactionStatus.COMMITTED);
  }

  public void rollback(long tid, Map<Integer, AsyncResultWrapper> results) throws SQLException {
    Transaction transaction = getTransaction(tid);
    if (transaction.getStatus() == TransactionStatus.COMMITTING
        || transaction.getStatus() == TransactionStatus.COMMITTED
        || transaction.getStatus() == TransactionStatus.PREPARE_UNKNOWN) {
      throw new SQLException(
          "Transaction requires recovery; rollback is forbidden for " + tid, "08007");
    }
    transaction.setStatus(TransactionStatus.ROLLING_BACK);
    transaction.setRollbackOnly(true);
    SQLException failure = null;
    for (Participant participant : participants(transaction)) {
      try {
        if (participant.getStatus() == TransactionStatus.ROLLBACK) continue;
        execute(
            participant,
            participant.getStatus() == TransactionStatus.PREPARED
                ? "ROLLBACK PREPARED '" + branchName(tid, participant) + "'"
                : "ROLLBACK");
        participant.setStatus(TransactionStatus.ROLLBACK);
      } catch (SQLException ex) {
        if (failure == null)
          failure =
              new SQLException("Distributed rollback outcome is in doubt: " + tid, "08007", ex);
        else failure.addSuppressed(ex);
      }
    }
    if (failure != null) throw failure;
    transaction.setStatus(TransactionStatus.ROLLBACK);
  }

  public void doAfterCommit(long tid, boolean committed) throws SQLException {
    Transaction transaction = getTransaction(tid);
    if (transaction.getStatus()
        != (committed ? TransactionStatus.COMMITTED : TransactionStatus.ROLLBACK)) {
      throw new SQLException("Database participants have not all finished", "08007");
    }
    for (Participant participant : participants(transaction)) {
      ValidationStatus status = participant.getValidationStatus();
      if (status == ValidationStatus.VALIDATION_STATUS_NUM) continue;
      if (participant.getInstanceID() == instanceID) {
        participant.doAfterCommit(identity(tid), committed);
      } else {
        try {
          FinalResponse response =
              (FinalResponse)
                  client(participant.getInstanceID())
                      .request(new FinalRequest(identity(tid), committed, List.of()));
          if (response.getException() != null) {
            throw new SQLException("Remote finalization failed", response.getException());
          }
        } catch (SQLException ex) {
          throw new SQLException(
              "Distributed transaction " + tid + " requires finalization recovery", "08007", ex);
        }
      }
    }
    removeTransaction(transaction);
  }

  private void execute(Participant participant, String sql) throws SQLException {
    if (participant.getConnection() == null)
      throw new SQLException("Missing participant connection", "08003");
    try (PreparedStatement statement = participant.getConnection().prepareStatement(sql)) {
      statement.execute();
    }
  }

  private TransactionIdentity identity(long sequence) {
    return new TransactionIdentity(instanceID, sequence);
  }

  private String branchName(long sequence, Participant participant) {
    return instanceID + "-" + sequence + "-" + participant.getInstanceID();
  }

  public void updateTransactionStatus(long tid, int id, ValidationStatus status) {
    Transaction transaction = transactions.get(tid);
    if (transaction != null)
      transaction.getParticipants().stream()
          .filter(participant -> participant.getInstanceID() == id)
          .forEach(p -> p.setValidationStatus(status));
  }

  public static TransactionManager getInstance() {
    return INSTANCE;
  }
}
