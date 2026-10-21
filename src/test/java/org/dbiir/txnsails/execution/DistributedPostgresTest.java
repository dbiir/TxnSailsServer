package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Real PostgreSQL and separate middleware JVMs; no shared validation metadata between owners. */
@EnabledIfEnvironmentVariable(named = "TXNSAILS_TEST_PG_PORT", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DistributedPostgresTest {
  private final List<Process> servers = new ArrayList<>();
  private final String[] databases = new String[2];
  private final int[] ports = new int[2];
  private Path directory;
  private Path distributedConfig;
  private Path[] configurations = new Path[2];
  private String prefix;
  private final String user = "txnsails_test";

  @BeforeAll
  void start() throws Exception {
    directory = Files.createTempDirectory("txnsails-postgres-test-");
    prefix = "jdbc:postgresql://127.0.0.1:" + System.getenv("TXNSAILS_TEST_PG_PORT") + "/";
    int[] rpcPorts = {freePort(), freePort()};
    String suffix = Long.toUnsignedString(System.nanoTime());
    for (int id = 0; id < 2; id++) {
      databases[id] = "txnsails_test_" + suffix + "_" + id;
      try (Connection connection = connect("postgres");
          Statement statement = connection.createStatement()) {
        statement.execute("CREATE DATABASE " + databases[id]);
      }
      try (Connection connection = connect(databases[id]);
          Statement statement = connection.createStatement()) {
        statement.execute(
            "CREATE TABLE usertable (ycsb_key INT PRIMARY KEY, vid BIGINT NOT NULL DEFAULT 0, field1 VARCHAR(100))");
        for (int key = id; key < 40; key += 2)
          statement.execute("INSERT INTO usertable VALUES (" + key + ",0,'0')");
      }
      ports[id] = freePort();
      configurations[id] = directory.resolve("owner" + id + ".xml");
      Files.writeString(
          configurations[id],
          """
          <parameters><type>POSTGRES</type><driver>org.postgresql.Driver</driver>
          <url>%s</url><username>%s</username><password></password><instanceId>%d</instanceId>
          <concurrencyControlType>SERIALIZABLE</concurrencyControlType><benchmark>ycsb</benchmark></parameters>
          """
              .formatted(prefix + databases[id], user, id));
    }
    String firstIsolation = System.getenv().getOrDefault("TXNSAILS_TEST_A_ISOLATION", "RC");
    distributedConfig = directory.resolve("distributed.xml");
    Files.writeString(
        distributedConfig,
        """
        <parameters><instances>
        <instance><id>0</id><url>%s</url><host>127.0.0.1</host><username>%s</username><password></password><isolation>%s</isolation><rpcPort>%d</rpcPort></instance>
        <instance><id>1</id><url>%s</url><host>127.0.0.1</host><username>%s</username><password></password><isolation>SI</isolation><rpcPort>%d</rpcPort></instance>
        </instances><tables><table><name>usertable</name><locations><location>0</location><location>1</location></locations></table></tables></parameters>
        """
            .formatted(
                prefix + databases[0],
                user,
                firstIsolation,
                rpcPorts[0],
                prefix + databases[1],
                user,
                rpcPorts[1]));
    for (int id = 0; id < 2; id++)
      servers.add(launch(id, ports[id], directory.resolve("server" + id + ".log")));
    for (int id = 0; id < 2; id++) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
      while (true) {
        try (Client client = new Client(ports[id])) {
          register(
              client,
              "read",
              "read",
              "usertable",
              "SELECT field1 FROM usertable WHERE ycsb_key = ?");
          register(
              client,
              "write",
              "write",
              "usertable",
              "UPDATE usertable SET field1 = ? WHERE ycsb_key = ?");
          register(
              client,
              "scan",
              "read",
              "usertable",
              "SELECT field1 FROM usertable WHERE ycsb_key > ?");
          register(
              client,
              "upper",
              "read",
              "USERTABLE",
              "SELECT field1 FROM USERTABLE WHERE YCSB_KEY = ?");
          break;
        } catch (ConnectException ex) {
          if (System.nanoTime() > deadline || !servers.get(id).isAlive()) {
            fail(
                "Owner failed to start: "
                    + Files.readString(directory.resolve("server" + id + ".log")),
                ex);
          }
          Thread.sleep(50);
        }
      }
    }
    System.out.println("PostgreSQL middleware test logs: " + directory);
  }

  private Process launch(int id, int port, Path log) throws Exception {
    return new ProcessBuilder(
            Path.of(System.getProperty("java.home"), "bin", "java").toString(),
            "-cp",
            System.getProperty(
                "txnsails.test.serverClasspath", System.getProperty("java.class.path")),
            "org.dbiir.txnsails.TxnSailsServer",
            "-c",
            configurations[id].toString(),
            "-g",
            distributedConfig.toString(),
            "-s",
            "config/ycsb.sql",
            "-d",
            directory.resolve("metas" + id).toString(),
            "-l",
            Integer.toString(port),
            "-r",
            directory.resolve("state").toString())
        .redirectErrorStream(true)
        .redirectOutput(log.toFile())
        .start();
  }

  @AfterAll
  void stop() throws Exception {
    for (Process server : servers) {
      server.destroy();
      if (!server.waitFor(3, TimeUnit.SECONDS)) server.destroyForcibly().waitFor();
    }
    if (prefix != null) {
      try (Connection connection = connect("postgres");
          Statement statement = connection.createStatement()) {
        for (String database : databases) {
          if (database != null)
            statement.execute("DROP DATABASE IF EXISTS " + database + " WITH (FORCE)");
        }
      }
    }
  }

  @Test
  void commitsBothParticipantsAndClosesPreparedTransactions() throws Exception {
    try (Client client = new Client(ports[0])) {
      ok(client.call("begin"));
      ok(client.call("execute#write#0#a#0"));
      ok(client.call("execute#write#0#b#1"));
      ok(client.call("commit"));
      assertEquals("a", value(0));
      assertEquals("b", value(1));
      try (Connection connection = connect("postgres");
          Statement statement = connection.createStatement();
          ResultSet rows =
              statement.executeQuery(
                  "SELECT count(*) FROM pg_prepared_xacts WHERE database LIKE 'txnsails_test_%'")) {
        assertTrue(rows.next());
        assertEquals(0, rows.getInt(1));
      }
    }
  }

  @Test
  void rollbackDiscardsBothBranches() throws Exception {
    try (Client client = new Client(ports[0])) {
      ok(client.call("begin"));
      ok(client.call("execute#write#0#a#2"));
      ok(client.call("execute#write#0#b#3"));
      ok(client.call("rollback"));
      assertEquals("0", value(2));
      assertEquals("0", value(3));
    }
  }

  @Test
  void staleRemoteReadAbortsAndReleasesItsLock() throws Exception {
    try (Client reader = new Client(ports[0]);
        Client writer = new Client(ports[1])) {
      ok(reader.call("begin"));
      assertEquals("0", decode(reader.call("execute#read#0#5")));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#new#5"));
      ok(writer.call("commit"));
      assertTrue(reader.call("commit").startsWith("ERROR#"));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#later#5"));
      ok(writer.call("commit"));
      assertEquals("later", value(5));
    }
  }

  @Test
  void fracturedSnapshotsWithOneRwEdgeAreRejected() throws Exception {
    try (Client reader = new Client(ports[0]);
        Client writer = new Client(ports[1])) {
      ok(reader.call("begin"));
      assertEquals("0", decode(reader.call("execute#read#0#6")));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#new#6"));
      ok(writer.call("execute#write#0#new#7"));
      ok(writer.call("commit"));
      assertEquals("new", decode(reader.call("execute#read#0#7")));
      assertTrue(reader.call("commit").startsWith("ERROR#"));
    }
  }

  @Test
  void readModifyWritePreservesExternalVersion() throws Exception {
    try (Client reader = new Client(ports[0]);
        Client writer = new Client(ports[1])) {
      ok(reader.call("begin"));
      ok(reader.call("execute#read#0#8"));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#other#8"));
      ok(writer.call("commit"));
      String update = reader.call("execute#write#0#stale#8");
      if (update.startsWith("OK")) {
        ok(reader.call("execute#write#0#stale#9"));
        assertTrue(reader.call("commit").startsWith("ERROR#"));
      } else {
        ok(reader.call("rollback"));
      }
      assertEquals("other", value(8));
      assertEquals("0", value(9));
    }
  }

  @Test
  void repeatedWritesAndReadYourWritesCommit() throws Exception {
    try (Client client = new Client(ports[0])) {
      ok(client.call("begin"));
      assertEquals("0", decode(client.call("execute#read#0#10")));
      ok(client.call("execute#write#0#first#10"));
      assertEquals("first", decode(client.call("execute#read#0#10")));
      ok(client.call("execute#write#0#second#10"));
      assertEquals("second", decode(client.call("execute#read#0#10")));
      ok(client.call("execute#write#0#remote#11"));
      ok(client.call("commit"));
      assertEquals("second", value(10));
    }
  }

  @Test
  void executionFailurePoisonsWholeTransaction() throws Exception {
    try (Client client = new Client(ports[0])) {
      ok(client.call("begin"));
      ok(client.call("execute#write#0#discard#12"));
      assertTrue(client.call("execute#read#0#999").startsWith("ERROR#"));
      assertTrue(client.call("commit").startsWith("ERROR#"));
      assertEquals("0", value(12));
      ok(client.call("begin"));
      ok(client.call("execute#read#0#12"));
      ok(client.call("commit"));
    }
  }

  @Test
  void rejectsUnsupportedPredicatesBeforeExecution() throws Exception {
    try (Client client = new Client(ports[0])) {
      ok(client.call("begin"));
      assertTrue(client.call("execute#scan#0#12").contains("0A000"));
      ok(client.call("rollback"));
    }
  }

  @Test
  void remoteOnlyParticipantCommits() throws Exception {
    try (Client client = new Client(ports[0])) {
      ok(client.call("begin"));
      ok(client.call("execute#write#0#remoteonly#15"));
      ok(client.call("commit"));
      assertEquals("remoteonly", value(15));
    }
  }

  @Test
  void caseVariantsUseTheSameLogicalTable() throws Exception {
    try (Client client = new Client(ports[0])) {
      ok(client.call("begin"));
      assertEquals("0", decode(client.call("execute#upper#0#16")));
      ok(client.call("commit"));
    }
  }

  @Test
  void fullCompositeKeysKeepDistinctOrderLinesSeparate() throws Exception {
    Path schema = directory.resolve("orderline.sql");
    String ddl =
        "CREATE TABLE order_line (ol_w_id INT, ol_d_id INT, ol_o_id INT, ol_number INT, ol_amount FLOAT, vid BIGINT NOT NULL, PRIMARY KEY(ol_w_id,ol_d_id,ol_o_id,ol_number))";
    Files.writeString(schema, ddl + ";");
    for (String database : databases) {
      try (Connection connection = connect(database);
          Statement statement = connection.createStatement()) {
        statement.execute(ddl);
        statement.execute("INSERT INTO order_line VALUES (1,1,1,1,0,0),(1,1,1,2,0,1)");
      }
    }
    int[] rpc = {freePort(), freePort()};
    int[] clientPorts = {freePort(), freePort()};
    Path placement = directory.resolve("orderline-distributed.xml");
    String xml =
        Files.readString(distributedConfig)
            .replace("<name>usertable</name>", "<name>order_line</name>");
    // Each deployment has independent validation owners and persistent identity state.
    xml = xml.replaceAll("<rpcPort>[0-9]+</rpcPort>", "<rpcPort>PORT</rpcPort>");
    xml =
        xml.replaceFirst("PORT", Integer.toString(rpc[0]))
            .replaceFirst("PORT", Integer.toString(rpc[1]));
    Files.writeString(placement, xml);
    List<Process> compositeServers = new ArrayList<>();
    try {
      for (int id = 0; id < 2; id++) {
        Path configuration = directory.resolve("orderline-owner" + id + ".xml");
        Files.writeString(
            configuration,
            Files.readString(configurations[id])
                .replace("<benchmark>ycsb</benchmark>", "<benchmark>tpcc</benchmark>"));
        Path log = directory.resolve("orderline-owner" + id + ".log");
        Process server =
            new ProcessBuilder(
                    Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                    "-cp",
                    System.getProperty(
                        "txnsails.test.serverClasspath", System.getProperty("java.class.path")),
                    "org.dbiir.txnsails.TxnSailsServer",
                    "-c",
                    configuration.toString(),
                    "-g",
                    placement.toString(),
                    "-s",
                    schema.toString(),
                    "-d",
                    directory.resolve("orderline-metas" + id).toString(),
                    "-l",
                    Integer.toString(clientPorts[id]),
                    "-r",
                    directory.resolve("orderline-state").toString())
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        compositeServers.add(server);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (true) {
          try (Client client = new Client(clientPorts[id])) {
            register(
                client,
                "line_read",
                "read",
                "order_line",
                "SELECT ol_amount FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=? AND ol_number=?");
            register(
                client,
                "line_write",
                "write",
                "order_line",
                "UPDATE order_line SET ol_amount=? WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=? AND ol_number=?");
            break;
          } catch (ConnectException ex) {
            if (!server.isAlive() || System.nanoTime() > deadline) fail(Files.readString(log));
            Thread.sleep(50);
          }
        }
      }
      try (Client reader = new Client(clientPorts[0]);
          Client writer = new Client(clientPorts[1])) {
        ok(reader.call("begin"));
        ok(reader.call("execute#line_write#0#7#1#1#1#1"));
        ok(reader.call("execute#line_read#0#1#1#1#2"));
        ok(writer.call("begin"));
        ok(writer.call("execute#line_write#0#9#1#1#1#2"));
        ok(writer.call("commit"));
        assertTrue(reader.call("commit").startsWith("ERROR#"));
      }
      try (Connection connection = connect(databases[0]);
          Statement statement = connection.createStatement();
          ResultSet rows =
              statement.executeQuery("SELECT ol_amount FROM order_line WHERE ol_number=1")) {
        assertTrue(rows.next());
        assertEquals(0, rows.getInt(1));
      }
    } finally {
      for (Process server : compositeServers) {
        server.destroy();
        if (!server.waitFor(3, TimeUnit.SECONDS)) server.destroyForcibly().waitFor();
      }
    }
  }

  @Test
  void restartRefusesUnresolvedPreparedTransactions() throws Exception {
    String gid = "test-restart-" + System.nanoTime();
    try (Connection connection = connect(databases[0]);
        Statement statement = connection.createStatement()) {
      statement.execute("BEGIN");
      statement.execute("SELECT 1");
      statement.execute("PREPARE TRANSACTION '" + gid + "'");
      try {
        Path log = directory.resolve("restart.log");
        Process restarted = launch(0, freePort(), log);
        try {
          assertTrue(restarted.waitFor(10, TimeUnit.SECONDS));
          assertNotEquals(0, restarted.exitValue());
          assertTrue(Files.readString(log).contains("Resolve prepared transactions"));
        } finally {
          restarted.destroyForcibly();
        }
      } finally {
        statement.execute("ROLLBACK PREPARED '" + gid + "'");
      }
    }
  }

  private Connection connect(String database) throws SQLException {
    return DriverManager.getConnection(prefix + database, user, "");
  }

  private String value(int key) throws SQLException {
    try (Connection connection = connect(databases[key % 2]);
        Statement statement = connection.createStatement();
        ResultSet rows =
            statement.executeQuery("SELECT field1 FROM usertable WHERE ycsb_key=" + key)) {
      assertTrue(rows.next());
      return rows.getString(1);
    }
  }

  private static void register(
      Client client, String name, String operation, String table, String sql) throws Exception {
    ok(client.call("register#" + name + "#" + operation + "#" + table + "#" + sql));
  }

  private static void ok(String response) {
    assertTrue(response != null && response.startsWith("OK"), response);
  }

  private static String decode(String response) {
    ok(response);
    String data = response.substring(3);
    int length = Integer.parseInt(data.substring(2, 6), 16);
    return data.substring(6, 6 + length);
  }

  private static int freePort() throws IOException {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    }
  }

  private static class Client implements AutoCloseable {
    private final Socket socket;
    private final BufferedReader input;
    private final PrintWriter output;

    Client(int port) throws IOException {
      socket = new Socket("127.0.0.1", port);
      socket.setSoTimeout(15000);
      input = new BufferedReader(new InputStreamReader(socket.getInputStream()));
      output = new PrintWriter(socket.getOutputStream(), true);
    }

    String call(String request) throws IOException {
      output.println(request);
      return input.readLine();
    }

    public void close() throws IOException {
      socket.close();
    }
  }
}
