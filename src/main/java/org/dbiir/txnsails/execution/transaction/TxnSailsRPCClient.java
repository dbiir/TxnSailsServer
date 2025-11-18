package org.dbiir.txnsails.execution.transaction;


import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.common.ValidationStatus;
import org.dbiir.txnsails.common.messages.DoneResponse;
import org.dbiir.txnsails.common.messages.FinalResponse;
import org.dbiir.txnsails.common.messages.Message;
import org.dbiir.txnsails.common.messages.ValidationResponse;
import org.dbiir.txnsails.common.types.MessageType;

import java.io.*;
import java.net.Socket;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;


public class TxnSailsRPCClient implements Runnable {
  private static final Logger logger = Logger.getLogger(TxnSailsRPCClient.class.getName());
  static final int MAX_REQUEST_QUEUE_SIZE = 256;
  static final int MAX_RETRY = 10;
  private final String host;
  private final int port;
  private final int instanceID;
  private final Queue<Message> requestQueue;
  private Socket socket;
  private ObjectOutputStream out;
  private ObjectInputStream in;

  public TxnSailsRPCClient(DBInstance dbInstance, int port) {
    this.host = dbInstance.getHost();
    this.port = port;
    this.instanceID = dbInstance.getId();
    this.requestQueue = new ConcurrentLinkedQueue<>();

    int retry = 0;
    do {
      try {
        socket = new Socket(host, port);
        out = new ObjectOutputStream(socket.getOutputStream());
        in = new ObjectInputStream(socket.getInputStream());
        break;
      } catch (IOException e) {
        System.out.println("Failed to connect to server, retrying...");
        retry++;
        if (retry >= MAX_RETRY) {
          e.printStackTrace();
        }
      }
      try {
        Thread.sleep(2000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (retry < MAX_RETRY);
  }

  public void addRequest(Message request) {
    this.requestQueue.offer(request);
    // System.out.println("Add request to queue: " + request.getType() + " requestQueue.size()" + requestQueue.size());
  }

  private String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x ", b));
    }
    return sb.toString();
  }

  private byte[] packageMessage(Message msg) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(msg);
    } catch (IOException e) {
      System.out.println("Package failed, message: " + e);
    }
    return baos.toByteArray();
  }

  @Override
  public void run() {
    Message request;
    while (!Thread.currentThread().isInterrupted()) {
      try {
        // get the next request from the queue
        request = requestQueue.poll();
        if (request == null) {
          Thread.sleep(0, 10);
          continue;
        }
        byte[] data = packageMessage(request);
        out.writeInt(data.length);  // 先写长度
        out.write(data);           // 再写数据
        // out.writeObject(request);
        out.flush();
//        if (request.getType() == MessageType.VALIDATION_REQ) {
          // System.out.println("Send request. Type: " + request.getType() + ", instanceID: " + instanceID);
          // System.out.println(((ValidationRequest) request).getTid() +  " send serializable data (Hex): " + bytesToHex(data));
//        }
        while (true) {
          if (in.available() < 4) {
            Thread.sleep(0, 100);
          } else {
            break;
          }
        }
        int messageLength = in.readInt();
        if (messageLength <= 0) {
          logger.warning("Client receive invalid message length: " + messageLength);
          break;
        }
        byte[] messageData = new byte[messageLength];
        in.readFully(messageData);

        ByteArrayInputStream bais = new ByteArrayInputStream(messageData);
        ObjectInputStream oois = new ObjectInputStream(bais);
        Message msg = (Message) oois.readObject();
        // handle the responses
        switch (msg.getType()) {
          case MessageType.VALIDATION_RESP -> {
            handleValidationResponse((ValidationResponse) msg);
          }
          case MessageType.FINAL_RESP -> {
            handleFinalResponse((FinalResponse) msg);
          }
          case MessageType.DONE_RESP -> {
            handleDoneResponse((DoneResponse) msg);
          }
          default -> {
            logger.warning("Unknown message type: " + msg.getType());
          }
        }
      } catch (IOException | ClassNotFoundException e) {
        e.printStackTrace();
        break;
      } catch (InterruptedException e) {
        break;
      }
    }

    close();
    System.out.println("Remote client " + instanceID + " closed");
  }

  private void handleValidationResponse(ValidationResponse response) {
    long tid = response.getTid();

    if (response.getException() != null) {
      logger.info("Remote distributed transaction #" + tid + "-" + instanceID + " validation failed!");
      TransactionManager.getInstance().updateTransactionStatus(tid, instanceID, ValidationStatus.FAILED);
    } else {
      logger.info("Remote distributed transaction #" + tid + "-" + instanceID + " validation failed!");
      TransactionManager.getInstance().updateTransactionStatus(tid, instanceID, ValidationStatus.VALIDATED);
    }
  }

  private void handleFinalResponse(FinalResponse response) {
    long tid = response.getTid();
    if (response.getException() != null) {
      logger.info("Final failed: " + response.getException().getMessage());
    } else {
      logger.info("Remote distributed transaction #" + tid + "-" + instanceID + " finished!");
    }
  }

  private void handleDoneResponse(DoneResponse response) {
    // pass, do nothing
    TransactionManager.getInstance().setParticipantsFinishAck(response.getInstanceID());
  }

  private void close() {
    try {
      socket.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
