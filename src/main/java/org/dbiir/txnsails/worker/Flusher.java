package org.dbiir.txnsails.worker;

import java.io.*;
import java.net.Socket;
import lombok.Setter;
import lombok.SneakyThrows;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.execution.validation.TransactionCollector;

public class Flusher implements Runnable {
  private static final String ip = "localhost";
  private static final int port = 7654;
  private static final CCType[] types =
      new CCType[] {CCType.SER, CCType.SI_TAILOR, CCType.RC_TAILOR};

  private final String workload;
  @Setter private String outputFilePrefix;
  private CCType ccType;
  private final boolean online;
  private final Socket socket;
  private boolean use = false;

  public Flusher(String workload, String prefix, CCType ccType, boolean online) {
    try {
      this.workload = workload;
      this.outputFilePrefix = prefix;
      this.ccType = ccType;
      this.online = online;
      if (use && online)
        this.socket = new Socket(ip, port);
      else
        this.socket = new Socket();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean needFlush(CCType type) {
    for (CCType t : types) {
      if (type == t) return true;
    }
    return false;
  }

  @SneakyThrows
  @Override
  public void run() {
    while (!Thread.currentThread().isInterrupted()) {
      if (TransactionCollector.getInstance().isNeedFlush() && needFlush(ccType)) {
        long timestamp = System.currentTimeMillis();
        String fileName = outputFilePrefix + "sample_" + timestamp;
        System.out.println(fileName);
        try (FileWriter fileWriter = new FileWriter(fileName, true)) {
          for (int i = 0; i < TransactionCollector.TRANSACTION_BATCH; i++) {
            fileWriter.write(i + ",");
            fileWriter.write(TransactionCollector.getInstance().getTransactionNodeFeature(i));
            fileWriter.write(TransactionCollector.getInstance().getTransactionEdgeFeature(i));
            fileWriter.write("\n");
          }
          fileWriter.close();
          System.out.println("Content appended to the file.");
        } catch (IOException ex) {
          System.out.println("An error occurred: " + ex.getMessage());
        }

        TransactionCollector.getInstance().refreshMetas();
        if (online) {
          PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
          BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
          out.println(workload + ",predict," + fileName);
          String data = in.readLine();
          System.out.println("Receive the prediction result: " + data);
          // TODO: change the
          Adapter.getInstance().setNextCCType(data);
        }
        System.out.println("Flush time cost: " + (System.currentTimeMillis() - timestamp) + " ms");
      }
      try {
        if (online) {
          Thread.sleep(1000);
        } else {
          Thread.sleep(10000);
        }
      } catch (InterruptedException ignored) {
        return;
      }
    }
  }
}
