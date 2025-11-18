package org.dbiir.txnsails.execution.transaction;

import lombok.Getter;
import org.dbiir.txnsails.common.messages.*;
import org.dbiir.txnsails.common.types.MessageType;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.validation.ValidationItem;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.sql.SQLException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class TxnSailsRPCServer implements Runnable {
  private static final Logger logger = Logger.getLogger(TxnSailsRPCServer.class.getName());
  @Getter
  public static final int DEFAULT_PORT = 23333;

  private final int port;
  // private final Connection connection;
  private final int instanceID;
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private ServerSocket serverSocket;

  public TxnSailsRPCServer(int port, WorkloadConfiguration wrkld) {
    this.port = port;
    this.instanceID = wrkld.getInstanceID();
    try {
      // this.connection = DriverManager.getConnection(wrkld.getUrl(), wrkld.getUsername(), wrkld.getPassword());
    } catch (Exception e) {
      throw new RuntimeException("Failed to connect to database", e);
    }
    try {
      this.serverSocket = new ServerSocket(port);
    } catch (IOException e) {
      System.out.println("Failed to start server");
    }
  }

  @Override
  public void run() {
    try {
      if (serverSocket == null) {
        return;
      }
      System.out.println("ValidationRpcServer started on port: " + port);
      while (!Thread.currentThread().isInterrupted()) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("Accepted connection from: " + clientSocket.getInetAddress().getHostName());
        executor.execute(new ClientHandler(clientSocket));
      }
    } catch (SocketException e) {
      if (!Thread.currentThread().isInterrupted()) {
        logger.warning("ServerSocket closed unexpectedly" + e);
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("ValidationRpcServer execution failed", e);
    } finally {
      executor.shutdown();
      System.out.println("ValidationRpcServer wait for all executor terminate");
      try {
        while (!executor.isTerminated()) {
          executor.awaitTermination(100, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
        Thread.currentThread().interrupt();
      }
      System.out.println("ValidationRpcServer stopped");
    }
  }

  public void closeServer() {
    try {
      if (serverSocket != null && !serverSocket.isClosed()) {
        serverSocket.close();
      }
    } catch (IOException e) {
      logger.warning("Error closing ServerSocket" + e);
    }
  }

  private class ClientHandler implements Runnable {
    private Socket clientSocket;
    private ObjectInputStream ois;
    private ObjectOutputStream oos;

    public ClientHandler(Socket socket) {
      this.clientSocket = socket;
      try {
        ois = new ObjectInputStream(clientSocket.getInputStream());
        oos = new ObjectOutputStream(clientSocket.getOutputStream());
      } catch (IOException ex) {
        ex.printStackTrace();
        throw new RuntimeException("Failed to create input/output streams", ex);
      }
    }

    @Override
    public void run() {
      try {
        while (!clientSocket.isClosed()) {
          int messageLength = ois.readInt();
          if (messageLength <= 0) {
            logger.warning("Invalid message length: " + messageLength);
            break;
          }
          byte[] messageData = new byte[messageLength];
          ois.readFully(messageData);  // 确保读取完整数据

          ByteArrayInputStream bais = new ByteArrayInputStream(messageData);
          ObjectInputStream oois = new ObjectInputStream(bais);
          Message msg = (Message) oois.readObject();
          // handle the requests
          // System.out.println("Receive message: " + msg.getType());
          switch (msg.getType()) {
            case MessageType.VALIDATION_REQ -> {
              handleValidationRequest((ValidationRequest) msg);
            }
            case MessageType.FINAL_REQ -> {
              handleFinalRequest((FinalRequest) msg);
            }
            case MessageType.DONE_REQ -> {
              handleDoneRequest((DoneRequest) msg);
            }
            default -> {
              logger.warning("Unknown message type: " + msg.getType());
            }
          }
        }
      } catch (EOFException e) {
        // close
        System.out.println("Client disconnected normally");
      } catch (SocketException e) {
        // network error
        System.out.println("Client disconnected unexpectedly: " + e.getMessage());
      } catch (IOException e) {
        logger.warning("I/O error while reading message" + e);
      } catch (Exception e) {
        throw new RuntimeException("Failed to handle client request", e);
      } finally {
        System.out.println("ClientHandler stopped");
      }
    }

    private String bytesToHex(byte[] bytes) {
      StringBuilder sb = new StringBuilder();
      for (byte b : bytes) {
        sb.append(String.format("%02x ", b));
      }
      return sb.toString();
    }

    private void handleValidationRequest(ValidationRequest req) {
      // debugSerialization(req);
      long tid = req.getTid();
      // logger.info("Received validation request for transaction #" + tid + " from " + clientSocket.getInetAddress().getHostName());
      ValidationResponse resp = new ValidationResponse(tid);
      int validationPhase = 0;

      try {
        for (ValidationItem item : req.getItems()) {
          item.validate(tid);
          validationPhase += 1;
        }
      } catch (SQLException ex) {
        releaseValidationLocks(tid, validationPhase, req.getItems());
        resp.setException(ex);
      }
      try {
        sendResponse(resp);
        // oos.writeObject(resp);
      } catch (IOException ex) {
        releaseValidationLocks(tid, validationPhase, req.getItems());
        ex.printStackTrace();
      }
    }

    private void handleFinalRequest(FinalRequest req) {
      long tid = req.getTid();
      logger.info("Received final request for transaction #" + tid + " from " + clientSocket.getInetAddress().getHostName());
      FinalResponse resp = new FinalResponse(tid);
      for (ValidationItem item : req.getItems()) {
        item.doAfterCommit(tid, req.isCommit());
      }
      try {
        sendResponse(resp);
        // oos.writeObject(resp);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }

    private void handleDoneRequest(DoneRequest req) {
      int finishedInstance = req.getInstanceID();
      logger.info("Received done request from " + finishedInstance + " " + clientSocket.getInetAddress().getHostName());
      DoneResponse resp = new DoneResponse(instanceID);
      TransactionManager.getInstance().setParticipantsFinish(finishedInstance);
      try {
        sendResponse(resp);
        // oos.writeObject(resp);
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }

    private void sendResponse(Message resp) throws IOException {
      byte[] data = packageMessage(resp);
      oos.writeInt(data.length);  // 先写长度
      oos.write(data);           // 再写数据
      oos.flush();
    }

    private byte[] packageMessage(Message msg) {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      try (ObjectOutputStream ooos = new ObjectOutputStream(baos)) {
        ooos.writeObject(msg);
      } catch (IOException e) {
        System.out.println("Package failed, message: " + e);
      }
      return baos.toByteArray();
    }

    private void releaseValidationLocks(long tid, int validationPhase, ValidationItem[] validationSet) {
      for (int i = validationPhase; i >= 0; i--) {
        validationSet[i].releaseValidationLock(tid);
      }
    }
  }
}
