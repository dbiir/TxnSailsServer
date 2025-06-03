package org.dbiir.txnsails;

import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.worker.MetaWorker;
import org.dbiir.txnsails.worker.OfflineWorker;
import org.dbiir.txnsails.worker.OnlineWorker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.List;

class ClientHandler implements Runnable {
  private final Socket clientSocket;
  private final WorkloadConfiguration configuration;
  private final int id;
  private final OnlineWorker worker;

  public ClientHandler(Socket clientSocket, WorkloadConfiguration configuration, int id) {
    this.clientSocket = clientSocket;
    this.configuration = configuration;
    this.id = id;
    this.worker = new OnlineWorker(configuration, id);
  }

  @Override
  public void run() {
    try (
            BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true)
    ) {
      String message;
      while ((message = in.readLine()) != null) {
        long start = System.currentTimeMillis();
        System.out.println(
                " id: "
                        + id
                        + " Received: "
                        + message
                        + " cost: "
                        + (System.currentTimeMillis() - start)
                        + "ms");
        String[] args = parseArgs(message.trim());
        String functionName = args[0].toLowerCase();
        String response;
        start = System.currentTimeMillis();
        try {
          switch (functionName) {
            case "execute" -> {
              if (args.length < 2) {
                throw new SQLException("Invalid number of arguments for execute command.");
              }
              response = worker.execute(args, 3);
              response = "OK#" + response;
            }
            case "commit" -> {
              worker.commit();
              response = "OK";
            }
            case  "rollback" -> {
              worker.rollback();
              response = "OK";
            }
            case "register" -> {
              if (args.length < 5) {
                response = "FAILED";
                break;
              }
              int idx = OfflineWorker.getINSTANCE().register(args);
              if (idx < 0) {
                response = "FAILED";
              } else {
                response = "OK#" + idx; // response with the unique sql index in server-side
              }
            }
            case "analysis" -> {
              response = "OK";
              OfflineWorker.getINSTANCE().register_end(args);
            }
            case "close" -> {
              clientSocket.close();
              TxnSailsServer.closeServer();
              return;
            }
            default -> {
              response = "Unknown function: " + functionName;
            }
          }

        } catch (SQLException ex) {
          response = MessageFormat.format(
                  MetaWorker.ERROR_FORMATTER, ex.getMessage().split("\n")[0], ex.getSQLState(), ex.getErrorCode());
        }

        System.out.println("Execution time: " + (System.currentTimeMillis() - start) + "ms");
        System.out.println(worker.toString() + " response: " + response);
        out.println(response);
      }
    } catch (IOException ex) {
      System.out.println("Client disconnected: " + id);
    } catch (Exception e) {
      System.out.println(List.of(e.getStackTrace()));
    } finally {
      try {
        clientSocket.close();
        worker.closeWorker();
      } catch (Exception e) {
        System.out.println(List.of(e.getStackTrace()));
      }
    }
  }

  private String[] parseArgs(String message) {
    String[] parts = message.split("#");
    for (int i = 0; i < parts.length; i++) {
      parts[i] = parts[i].trim();
    }
    return parts;
  }

  private void sendResponse(PrintWriter out, String response) {
    System.out.println("Sending: " + response);
    out.println(response);
  }
}