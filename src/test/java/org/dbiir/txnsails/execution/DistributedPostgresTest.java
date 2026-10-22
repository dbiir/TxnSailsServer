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
            "CREATE TABLE usertable (ycsb_key INT PRIMARY KEY, vid BIGINT NOT NULL DEFAULT 0,"
                + " field1 VARCHAR(100))");
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
    List<String> command =
        new ArrayList<>(
            List.of(
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
                directory.resolve("state").toString()));
    if (fullValidation()) command.addAll(List.of("-v", "full"));
    return new ProcessBuilder(command)
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
      // Visiting a different owner makes this an actual global writer.
      ok(writer.call("execute#write#0#global#4"));
      ok(writer.call("commit"));
      assertTrue(reader.call("commit").startsWith("ERROR#"));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#later#5"));
      ok(writer.call("commit"));
      assertEquals("later", value(5));
    }
  }

  @Test
  void localSiReadOnlyTransactionMaySerializeBeforeLocalWriter() throws Exception {
    try (Client reader = new Client(ports[0]);
        Client writer = new Client(ports[1])) {
      ok(reader.call("begin"));
      assertEquals("0", decode(reader.call("execute#read#0#17")));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#new#17"));
      ok(writer.call("commit"));
      // The coordinator is remote, but both transactions visit only owner B.
      String result = reader.call("commit");
      if (fullValidation()) assertTrue(result.startsWith("ERROR#"), result);
      else ok(result);
      assertEquals("new", value(17));
    }
  }

  @Test
  void localReadOnlyCertificationUsesActualParticipantIsolation() throws Exception {
    try (Client reader = new Client(ports[1]);
        Client writer = new Client(ports[0])) {
      ok(reader.call("begin"));
      assertEquals("0", decode(reader.call("execute#read#0#36")));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#new#36"));
      ok(writer.call("commit"));
      String result = reader.call("commit");
      String isolation =
          System.getenv()
              .getOrDefault("TXNSAILS_TEST_A_ISOLATION", "RC")
              .toUpperCase(Locale.ROOT)
              .replace("TRANSACTION_", "");
      if (fullValidation() || Set.of("RC", "READ_COMMITTED").contains(isolation))
        assertTrue(result.startsWith("ERROR#"), result);
      else ok(result);
      assertEquals("new", value(36));
    }
  }

  @Test
  void localSiReadWriteTransactionStillValidatesItsExternalReads() throws Exception {
    try (Client reader = new Client(ports[0]);
        Client writer = new Client(ports[1])) {
      ok(reader.call("begin"));
      assertEquals("0", decode(reader.call("execute#read#0#19")));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#new#19"));
      ok(writer.call("commit"));
      // The write occurs after the stale read; classification cannot be fixed at BEGIN.
      ok(reader.call("execute#write#0#discard#21"));
      assertTrue(reader.call("commit").startsWith("ERROR#"));
      assertEquals("0", value(21));
    }
  }

  @Test
  void lateParticipantPromotionRevalidatesEarlierLocalReads() throws Exception {
    try (Client reader = new Client(ports[0]);
        Client writer = new Client(ports[1])) {
      ok(reader.call("begin"));
      assertEquals("0", decode(reader.call("execute#read#0#23")));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#new#23"));
      ok(writer.call("commit"));
      // Until this access the reader could use local SI read-only certification.
      assertEquals("0", decode(reader.call("execute#read#0#22")));
      assertTrue(reader.call("commit").startsWith("ERROR#"));
    }
  }

  @Test
  void interveningLocalOverwriteDoesNotHideCommittedGlobalWriter() throws Exception {
    try (Client reader = new Client(ports[0]);
        Client writer = new Client(ports[1])) {
      ok(reader.call("begin"));
      assertEquals("0", decode(reader.call("execute#read#0#25")));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#global#25"));
      ok(writer.call("execute#write#0#global#24"));
      ok(writer.call("commit"));
      ok(writer.call("begin"));
      ok(writer.call("execute#write#0#local#25"));
      ok(writer.call("commit"));
      assertTrue(reader.call("commit").startsWith("ERROR#"));
      assertEquals("local", value(25));
    }
  }

  @Test
  void businessBranchesExplicitlyOverrideDatabaseReadOnlyDefault() throws Exception {
    try (Connection connection = connect("postgres");
        Statement statement = connection.createStatement()) {
      statement.execute("ALTER DATABASE " + databases[0] + " SET default_transaction_read_only=on");
      try {
        try (Connection check = connect(databases[0]);
            Statement verify = check.createStatement();
            ResultSet rows = verify.executeQuery("SHOW default_transaction_read_only")) {
          assertTrue(rows.next());
          assertEquals("on", rows.getString(1));
        }
        try (Client client = new Client(ports[0])) {
          ok(client.call("begin"));
          ok(client.call("execute#write#0#readwrite#28"));
          ok(client.call("execute#write#0#readwrite#29"));
          ok(client.call("commit"));
        }
        assertEquals("readwrite", value(28));
        assertEquals("readwrite", value(29));
      } finally {
        statement.execute(
            "ALTER DATABASE " + databases[0] + " RESET default_transaction_read_only");
      }
    }
  }

  @Test
  void localDangerousStructureBreaksMixedNodeGlobalCycle() throws Exception {
    try (Client globalReader = new Client(ports[0]);
        Client pivot = new Client(ports[0]);
        Client outgoing = new Client(ports[1]);
        Client globalWriter = new Client(ports[1])) {
      // G1 --rw(x)--> L1 --rw(y)--> L2 --wr(z)--> G2 --wr(w)--> G1.
      // A supplies native SSI (SER) or local TxnSails validation (RC/SI).
      ok(globalReader.call("begin"));
      assertEquals("0", decode(globalReader.call("execute#read#0#30")));
      ok(pivot.call("begin"));
      assertEquals("0", decode(pivot.call("execute#read#0#32")));
      ok(pivot.call("execute#write#0#1#30"));
      ok(outgoing.call("begin"));
      ok(outgoing.call("execute#write#0#1#32"));
      ok(outgoing.call("execute#write#0#1#34"));
      ok(outgoing.call("commit"));
      ok(globalWriter.call("begin"));
      assertEquals("1", decode(globalWriter.call("execute#read#0#34")));
      ok(globalWriter.call("execute#write#0#1#31"));
      ok(globalWriter.call("commit"));
      assertEquals("1", decode(globalReader.call("execute#read#0#31")));
      // Both global transactions have SELECT-only branches on A and use 2PC.
      ok(globalReader.call("commit"));
      String rejected = pivot.call("commit");
      assertTrue(rejected.startsWith("ERROR#"), rejected);
      assertTrue(rejected.contains("40001"), rejected);
      // A definite native abort must release validation state and reset this worker.
      ok(pivot.call("begin"));
      assertEquals("0", decode(pivot.call("execute#read#0#30")));
      ok(pivot.call("commit"));
      assertEquals("0", value(30));
      assertEquals("1", value(32));
      assertEquals("1", value(34));
      assertEquals("1", value(31));
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
        "CREATE TABLE order_line (ol_w_id INT, ol_d_id INT, ol_o_id INT, ol_number INT, ol_amount"
            + " FLOAT, vid BIGINT NOT NULL, PRIMARY KEY(ol_w_id,ol_d_id,ol_o_id,ol_number))";
    Files.writeString(schema, ddl + ";");
    for (int id = 0; id < databases.length; id++) {
      try (Connection connection = connect(databases[id]);
          Statement statement = connection.createStatement()) {
        statement.execute(ddl);
        statement.execute("INSERT INTO order_line VALUES (1,1,1,1,0,0),(1,1,1,2,0,1)");
        // Placement uses order ID minus one: order 1 is on A, order 2 is on B.
        if (id == 1) statement.execute("INSERT INTO order_line VALUES (1,1,2,1,0,0)");
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
        List<String> command =
            new ArrayList<>(
                List.of(
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
                    directory.resolve("orderline-state").toString()));
        if (fullValidation()) command.addAll(List.of("-v", "full"));
        Process server =
            new ProcessBuilder(command)
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
                "SELECT ol_amount FROM order_line WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=? AND"
                    + " ol_number=?");
            register(
                client,
                "line_write",
                "write",
                "order_line",
                "UPDATE order_line SET ol_amount=? WHERE ol_w_id=? AND ol_d_id=? AND ol_o_id=? AND"
                    + " ol_number=?");
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
        ok(writer.call("execute#line_write#0#9#1#1#2#1"));
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
  void nativeReadWriteSelectOnlyTwoPhaseBranchRetainsSsiProtection() throws Exception {
    try (Connection connection = connect(databases[0]);
        Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE native_ssi_x (value INT NOT NULL)");
      statement.execute("CREATE TABLE native_ssi_y (value INT NOT NULL)");
      statement.execute("INSERT INTO native_ssi_x VALUES (0)");
      statement.execute("INSERT INTO native_ssi_y VALUES (0)");
    }
    // Ordinary COMMIT recognizes an implicit read-only transaction. The same
    // SELECT-only READ WRITE transaction completed through 2PC remains a writer
    // for SSI's conservative conflict checks, without an auxiliary write.
    nativeSsiDelayedDangerousStructure(false);
    nativeSsiDelayedDangerousStructure(true);
  }

  private void nativeSsiDelayedDangerousStructure(boolean prepared) throws Exception {
    String gid = "test-select-only-" + System.nanoTime();
    try (Connection incoming = connect(databases[0]);
        Connection pivot = connect(databases[0]);
        Connection outgoing = connect(databases[0]);
        Statement in = incoming.createStatement();
        Statement mid = pivot.createStatement();
        Statement out = outgoing.createStatement()) {
      out.execute("UPDATE native_ssi_x SET value=0");
      out.execute("UPDATE native_ssi_y SET value=0");
      in.execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE");
      try (ResultSet rows = in.executeQuery("SHOW transaction_read_only")) {
        assertTrue(rows.next());
        assertEquals("off", rows.getString(1));
      }
      try (ResultSet rows = in.executeQuery("SELECT value FROM native_ssi_x")) {
        assertTrue(rows.next());
        assertEquals(0, rows.getInt(1));
      }
      mid.execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE");
      // Acquire the pivot's snapshot now, but discover its outgoing RW later.
      mid.execute("SELECT pg_current_snapshot()");
      out.execute("BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE");
      out.execute("UPDATE native_ssi_y SET value=1");
      out.execute("COMMIT");
      if (prepared) {
        in.execute("PREPARE TRANSACTION '" + gid + "'");
        try {
          in.execute("COMMIT PREPARED '" + gid + "'");
        } finally {
          try (ResultSet rows =
              out.executeQuery("SELECT count(*) FROM pg_prepared_xacts WHERE gid='" + gid + "'")) {
            assertTrue(rows.next());
            if (rows.getInt(1) != 0) in.execute("ROLLBACK PREPARED '" + gid + "'");
          }
        }
      } else {
        in.execute("COMMIT");
      }
      try {
        try (ResultSet rows = mid.executeQuery("SELECT value FROM native_ssi_y")) {
          assertTrue(rows.next());
          assertEquals(0, rows.getInt(1));
        }
        if (prepared) {
          SQLException failure =
              assertThrows(
                  SQLException.class,
                  () -> {
                    mid.execute("UPDATE native_ssi_x SET value=1");
                    mid.execute("COMMIT");
                  });
          assertEquals("40001", failure.getSQLState());
        } else {
          mid.execute("UPDATE native_ssi_x SET value=1");
          mid.execute("COMMIT");
        }
      } finally {
        mid.execute("ROLLBACK");
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

  private static boolean fullValidation() {
    return "full".equalsIgnoreCase(System.getenv("TXNSAILS_TEST_VALIDATION_MODE"));
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
