package org.dbiir.txnsails.execution.transaction;

import org.dbiir.txnsails.common.AsyncResultWrapper;
import org.dbiir.txnsails.common.TransactionStatus;
import org.dbiir.txnsails.common.ValidationStatus;
import org.dbiir.txnsails.common.messages.DoneRequest;
import org.dbiir.txnsails.common.messages.FinalRequest;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.worker.OnlineWorker;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

public class TransactionManager {
  private static final Logger logger = Logger.getLogger(TransactionManager.class.getName());
  private static final int TRANSACTION_HASH_SIZE = 128; // must be configured to 2^n
  private static final int SEND_THREAD_NUM = 8; // must be configured to 2^n
  private static final int REMOTE_PORT = 23333;
  private static final int LOCAL_PORT = 23333;
  private int instanceID;
  private static TransactionManager INSTANCE = new TransactionManager();
  private List<List<Transaction>> localCoordinateTransactionList = new ArrayList<>(TRANSACTION_HASH_SIZE);
  private TxnSailsRPCServer rpcServer;
  private List<List<TxnSailsRPCClient>> rpcClients;
  private int instanceNumber;
  private WorkloadConfiguration wrkld;
  private Thread rpcServerThread;
  private List<Thread> rpcClientThreads = new LinkedList<>();
  private boolean[] participantsFinish = new boolean[OnlineWorker.MAX_INSTANCE_NUM];
  private boolean[] participantsFinishAck = new boolean[OnlineWorker.MAX_INSTANCE_NUM];

  public TransactionManager() {
    for (int i = 0; i < TRANSACTION_HASH_SIZE; i++) {
      localCoordinateTransactionList.add(new LinkedList<>());
    }
    rpcClients = new LinkedList<>();
    for (int i = 0; i < OnlineWorker.MAX_INSTANCE_NUM; i++) {
      participantsFinish[i] = false;
      participantsFinishAck[i] = false;
    }
  }

  public void init(WorkloadConfiguration wrkld) {
    this.wrkld = wrkld;
    this.instanceID = wrkld.getInstanceID();
    this.rpcServer = new TxnSailsRPCServer(LOCAL_PORT, wrkld);

    // start rpc server
    rpcServerThread = new Thread(rpcServer);
    rpcServerThread.start();

    try {
      Thread.sleep(100);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // connect the clients to the remote
    instanceNumber = wrkld.getInstances().size();
    for (int i = 0; i < instanceNumber; i++) {
      rpcClients.add(new LinkedList<>());
      if (i == instanceID) {
        continue;
      }
      for (int j = 0; j < SEND_THREAD_NUM; j++) {
        TxnSailsRPCClient rpcClient = new TxnSailsRPCClient(wrkld.getInstances().get(i), REMOTE_PORT);
        rpcClients.get(i).add(rpcClient);
        Thread rpcClientThread = new Thread(rpcClient);
        rpcClientThreads.add(rpcClientThread);
        rpcClientThread.start();
      }
    }
  }

  public void close() {
    for (int i = 0; i < instanceNumber; i++) {
      if (i == instanceID) {
        continue;
      }
      // send done message
      rpcClients.get(i).get(0).addRequest(new DoneRequest(instanceID));
    }
    for (int i = 0; i < instanceNumber; i++) {
      if (i == instanceID) {
        continue;
      }

      // send finish request to all instances
      while (!participantsFinish[i]) {
        try {
          Thread.sleep(1000);
          System.out.println("Participant-" + i + " is not finished");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      // make sure all participants receive the final request
      while (!participantsFinishAck[i]) {
        try {
          Thread.sleep(1000);
          System.out.println("Participant-" + i + " is not acked");
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }
    for (Thread t : rpcClientThreads) {
      t.interrupt();
    }
    rpcServerThread.interrupt();
    rpcServer.closeServer();
    System.out.println("TransactionManager closed");
  }

  public void setParticipantsFinish(int instanceID) {
    participantsFinish[instanceID] = true;
  }

  public void setParticipantsFinishAck(int instanceID) {
    participantsFinishAck[instanceID] = true;
  }

  public void initTransaction(long tid) {
    int bucketNumber = (int)(tid & (TRANSACTION_HASH_SIZE - 1));
    localCoordinateTransactionList.get(bucketNumber).add(new Transaction(tid, new LinkedList<>()));
  }

  public void addTransaction(Transaction transaction) {
    int bucketNumber = (int)(transaction.getTid() & (TRANSACTION_HASH_SIZE - 1));
    localCoordinateTransactionList.get(bucketNumber).add(transaction);
  }

  public void removeTransaction(Transaction transaction) {
    int bucketNumber = (int)(transaction.getTid() & (TRANSACTION_HASH_SIZE - 1));
    localCoordinateTransactionList.get(bucketNumber).remove(transaction);
  }

  public void removeTransaction(long tid) {
    int bucketNumber = (int)(tid & (TRANSACTION_HASH_SIZE - 1));
    localCoordinateTransactionList.get(bucketNumber).remove(getTransaction(tid));
  }

  public void prepare(long tid) throws SQLException {
    Transaction transaction = getTransaction(tid);
    boolean distributed = transaction.getParticipants().size() > 1;

    // System.out.println(Thread.currentThread().getName() + " " + tid + " " + distributed);
    for (Participant p : transaction.getParticipants()) {
      if (p.getInstanceID() == instanceID) {
        p.validate(tid);
      } else {
        // generate the message and add to the client's message queue
        // ValidationRequest req = new ValidationRequest(tid, p.getValidationSet().getValidationItems());
        // int m = SEND_THREAD_NUM;
        // rpcClients.get(p.getInstanceID()).get((int)((tid % m) + m) % m).addRequest(req);
        // p.setValidationStatus(ValidationStatus.VALIDATING);
        p.setValidationStatus(ValidationStatus.VALIDATED);
      }

      if (distributed) {
        try {
          PreparedStatement prepare = p.getConnection().prepareStatement("PREPARE TRANSACTION '" + tid + "-" + p.getInstanceID() + "'");
          prepare.execute();
          p.setStatus(TransactionStatus.PREPARED);
        } catch (SQLException ex) {
          p.setStatus(TransactionStatus.PREPARE_FAILED);
          throw ex;
        }
      }
    }

    // wait for all validation finished
    // System.out.println("Transaction #" + tid + " Waiting for all participants validation");
    for (Participant p : transaction.getParticipants()) {
      while (p.getValidationStatus() != ValidationStatus.VALIDATED &&
              p.getValidationStatus() != ValidationStatus.FAILED) {
        try {
          Thread.sleep(0, 10000);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        // System.out.println("Transaction #" + tid + " Waiting for validation #" + tid + "-" + p.getInstanceID() + " " + p.getValidationStatus());
      }

      if (p.getValidationStatus() == ValidationStatus.FAILED) {
        throw new SQLException("Remote validation failed #" + tid + "-" + p.getInstanceID(), "500");
      }
    }
    // System.out.println("Prepare finish");
  }

  public void commit(long tid, AsyncResultWrapper[] results) {
    Transaction transaction = getTransaction(tid);

    for (Participant p : transaction.getParticipants()) {
      if (p.getValidationStatus() != ValidationStatus.VALIDATED) {
        logger.warning(Thread.currentThread().getName() + " The transaction is not validated !!!" + p.getValidationStatus());
        return;
      }
      if (p.getConnection() != null) {
        try {
          if (p.getStatus() == TransactionStatus.PREPARED) {
            PreparedStatement prepare = p.getConnection().prepareStatement("COMMIT PREPARED '" + tid + "-" + p.getInstanceID() + "'");
            prepare.execute();
          } else if (p.getStatus() == TransactionStatus.ACTIVE) {
            PreparedStatement prepare = p.getConnection().prepareStatement("COMMIT");
            prepare.execute();
          }
        } catch (SQLException ex) {
          logger.warning("Commit failed !!!" + ex);
          results[p.getInstanceID()].setException(ex);
        }
      } else {
        logger.warning("Cannot find the connection");
      }
    }
  }

  public void rollback(long tid, AsyncResultWrapper[] results) {
    // wait until the status become PREPARED or ABORTED
    Transaction transaction = getTransaction(tid);

    for (Participant p: transaction.getParticipants()) {
      if (p.getConnection() != null) {
        try {
          if (p.getStatus() == TransactionStatus.PREPARED) {
            PreparedStatement prepare = p.getConnection().prepareStatement("ROLLBACK PREPARED '" + tid + "-" + p.getInstanceID() + "'");
            prepare.execute();
          } else if (p.getStatus() == TransactionStatus.PREPARE_FAILED || p.getStatus() == TransactionStatus.ACTIVE) {
            PreparedStatement prepare = p.getConnection().prepareStatement("ROLLBACK");
            prepare.execute();
          } else {
            logger.info("The transaction status is " + p.getStatus() + "; Validation status is " + p.getValidationStatus());
          }
        } catch (SQLException ex) {
          results[p.getInstanceID()].setException(ex);
        }
      } else {
        logger.warning("Cannot find the connection");
      }
    }
  }

  public void doAfterCommit(long tid, boolean isCommit) {
    Transaction transaction = getTransaction(tid);

    for (Participant p : transaction.getParticipants()) {
      if (p.getValidationStatus() == ValidationStatus.FAILED) {
        // have released the validation locks
        continue;
      }
      if (p.getInstanceID() == instanceID) {
        p.doAfterCommit(tid, isCommit);
      } else {
        // generate the message and add to the client's message queue
        FinalRequest req = new FinalRequest(tid, isCommit, p.getValidationSet().getValidationItems());
        int m = SEND_THREAD_NUM;
        rpcClients.get(p.getInstanceID()).get((int)((tid % m) + m) % m).addRequest(req);
      }
    }
    // TODO:
    removeTransaction(transaction);
  }

  public void updateTransactionStatus(long tid, int instanceId, ValidationStatus status) {
    Transaction transaction = getTransaction(tid);
    for (Participant p : transaction.getParticipants()) {
      if (p.getInstanceID() == instanceId) {
        p.setValidationStatus(status);
        break;
      }
    }
  }

  private Transaction getTransaction(long tid) {
    int bucketNumber = (int)(tid & (TRANSACTION_HASH_SIZE - 1));
    for (Transaction transaction : localCoordinateTransactionList.get(bucketNumber)) {
      if (transaction.getTid() == tid) {
        return transaction;
      }
    }
    logger.warning("Cannot find the transaction!!!");
    return null;
  }

  public static TransactionManager getInstance() {
    return INSTANCE;
  }
}

/* TODO:
 * 1. modify the generation of tid in SmallbankWorker.java
 */
