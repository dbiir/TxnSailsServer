package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ConnectException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Native TOML startup, real PostgreSQL branches, and separate logical-owner JVMs. */
@EnabledIfEnvironmentVariable(named = "TXNSAILS_TEST_PG_PORT", matches = ".+")
class TomlDistributedPostgresTest {
  @Test
  void sharedDatabaseCommitsBothLogicalOwnersFromEitherCoordinator() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      for (int coordinator = 0; coordinator < 2; coordinator++) {
        int even = coordinator * 2;
        int odd = even + 1;
        String value = "coordinator" + coordinator;
        try (Client client = fixture.client(coordinator)) {
          ok(client.call("begin#pair"));
          // Slot order and repetition are independent of declaration order.
          ok(client.call("execute#pair#1#" + value + "#" + even));
          assertEquals(value, decode(client.call("execute#pair#0#" + even)));
          ok(client.call("execute#pair#1#" + value + "#" + odd));
          ok(client.call("execute#pair#1#final#" + even));
          assertEquals("final", decode(client.call("execute#pair#0#" + even)));
          ok(client.call("commit"));
        }
        assertEquals("final", fixture.value(even));
        assertEquals(value, fixture.value(odd));
        assertEquals(2, fixture.version(even));
        assertEquals(1, fixture.version(odd));
        assertEquals(0, fixture.preparedCount());
      }
    }
  }

  @Test
  void sharedDatabaseRollbackDiscardsBothBranchesAndAllowsReadOnlyReuse() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      for (int coordinator = 0; coordinator < 2; coordinator++) {
        int even = 4 + coordinator * 2;
        int odd = even + 1;
        try (Client client = fixture.client(coordinator)) {
          ok(client.call("begin#pair"));
          ok(client.call("execute#pair#1#discard#" + even));
          ok(client.call("execute#pair#1#discard#" + odd));
          ok(client.call("rollback"));
          assertEquals("0", fixture.value(even));
          assertEquals("0", fixture.value(odd));
          assertEquals(0, fixture.version(even));
          assertEquals(0, fixture.version(odd));
          ok(client.call("begin#pair"));
          assertEquals("0", decode(client.call("execute#pair#0#" + odd)));
          assertEquals("0", decode(client.call("execute#pair#0#" + even)));
          ok(client.call("commit"));
          assertEquals(0, fixture.preparedCount());
        }
      }
    }
  }

  @Test
  void sharedDatabaseStaleRemoteReadRejectsAfterGlobalWriterAndReleasesState() throws Exception {
    try (Fixture fixture = new Fixture(true);
        Client reader = fixture.client(0);
        Client writer = fixture.client(1)) {
      ok(reader.call("begin#pair"));
      // The read visits only remote logical owner 1 at SI, although both owners
      // connect to the same physical database. The writer actually visits both.
      assertEquals("0", decode(reader.call("execute#pair#0#9")));
      ok(writer.call("begin#pair"));
      ok(writer.call("execute#pair#1#new#9"));
      ok(writer.call("execute#pair#1#new#8"));
      ok(writer.call("commit"));
      errorState(reader.call("commit"), "40001");
      assertEquals("new", fixture.value(8));
      assertEquals("new", fixture.value(9));
      assertEquals(0, fixture.preparedCount());
      ok(reader.call("begin#pair"));
      assertEquals("new", decode(reader.call("execute#pair#0#8")));
      assertEquals("new", decode(reader.call("execute#pair#0#9")));
      ok(reader.call("commit"));
      assertEquals(0, fixture.preparedCount());
    }
  }

  @Test
  void separateDatabaseProfileUsesTheSameNativeTomlInterface() throws Exception {
    try (Fixture fixture = new Fixture(false);
        Client client = fixture.client(1)) {
      ok(client.call("begin#pair"));
      ok(client.call("execute#pair#1#left#0"));
      ok(client.call("execute#pair#1#right#1"));
      ok(client.call("commit"));
      assertEquals("left", fixture.value(0));
      assertEquals("right", fixture.value(1));
      assertEquals(1, fixture.version(0));
      assertEquals(1, fixture.version(1));
      assertEquals(0, fixture.preparedCount());
    }
  }

  @Test
  void configuredIsolationAppliesToEachActualNativeBusinessBranch() throws Exception {
    try (Fixture fixture = new Fixture(true)) {
      for (int owner = 0; owner < 2; owner++) {
        int key = 10 + owner;
        try (Client reader = fixture.client(1 - owner);
            Client writer = fixture.client(owner)) {
          ok(reader.call("begin#pair"));
          assertEquals("0", decode(reader.call("execute#pair#0#" + key)));
          // Only this branch is active. Native SERIALIZABLE READ WRITE takes
          // SIREAD locks; RC and SI do not, even on the same physical database.
          fixture.assertSerializableTracking(owner);
          ok(writer.call("begin#pair"));
          ok(writer.call("execute#pair#1#new#" + key));
          ok(writer.call("commit"));
          String repeatedRead = reader.call("execute#pair#0#" + key);
          if (fixture.isolation(owner) == Connection.TRANSACTION_READ_COMMITTED) {
            // RC sees the new version, which the middleware rejects as a
            // nonrepeatable read. SI/SER keep the original native snapshot.
            errorState(repeatedRead, "40001");
          } else {
            assertEquals("0", decode(repeatedRead));
          }
          ok(reader.call("rollback"));
          assertEquals("new", fixture.value(key));
          assertEquals(0, fixture.preparedCount());
        }
      }
    }
  }

  private static final class Fixture implements AutoCloseable {
    private static final String USER = "txnsails_test";
    private static final String DDL =
        "CREATE TABLE usertable (ycsb_key INT PRIMARY KEY, vid BIGINT NOT NULL DEFAULT 0,"
            + " field1 VARCHAR(100))";
    private final List<Process> servers = new ArrayList<>();
    private final String[] databases = new String[2];
    private final int[] clientPorts = new int[2];
    private final String[] isolationNames = {
      System.getenv().getOrDefault("TXNSAILS_TEST_A_ISOLATION", "SER"),
      System.getenv().getOrDefault("TXNSAILS_TEST_B_ISOLATION", "SI")
    };
    private final Path directory;
    private final String prefix;

    Fixture(boolean sharedDatabase) throws Exception {
      directory =
          Files.createTempDirectory("txnsails-toml-" + (sharedDatabase ? "shared-" : "separate-"));
      prefix = "jdbc:postgresql://127.0.0.1:" + System.getenv("TXNSAILS_TEST_PG_PORT") + "/";
      try {
        String suffix = Long.toUnsignedString(System.nanoTime());
        databases[0] = "txnsails_toml_" + suffix + "_0";
        databases[1] = sharedDatabase ? databases[0] : "txnsails_toml_" + suffix + "_1";
        for (int owner = 0; owner < (sharedDatabase ? 1 : 2); owner++) {
          try (Connection connection = connect("postgres");
              Statement sql = connection.createStatement()) {
            sql.execute("CREATE DATABASE " + databases[owner]);
          }
          try (Connection connection = connect(databases[owner]);
              Statement sql = connection.createStatement()) {
            sql.execute(DDL);
            for (int key = sharedDatabase ? 0 : owner; key < 20; key += sharedDatabase ? 1 : 2) {
              sql.execute("INSERT INTO usertable VALUES (" + key + ",0,'0')");
            }
          }
        }
        Files.writeString(directory.resolve("schema.sql"), DDL + ";");
        Files.writeString(
            directory.resolve("workload.toml"),
            """
            version = 1
            closed_world = true

            [[programs]]
            name = 'pair'

            [[programs.statements]]
            id = 0
            operation = 'read'
            relation = 'usertable'
            sql = 'SELECT field1 FROM usertable WHERE ycsb_key = ?'

            [[programs.statements]]
            id = 1
            operation = 'write'
            relation = 'usertable'
            sql = 'UPDATE usertable SET field1 = ? WHERE ycsb_key = ?'
            """);
        String profile = sharedDatabase ? "shared" : "separate";
        int[] rpcPorts = new int[2];
        Set<Integer> allocatedPorts = new LinkedHashSet<>();
        for (int owner = 0; owner < 2; owner++) {
          clientPorts[owner] = freePort(allocatedPorts);
          rpcPorts[owner] = freePort(allocatedPorts);
        }
        Path deployment = directory.resolve("deployment.toml");
        StringBuilder toml =
            new StringBuilder(
                """
                version = 1
                benchmark = 'ycsb'
                schema = 'schema.sql'
                workload = 'workload.toml'
                validation = 'optimized'
                state_directory = 'state'

                [profiles.%s.tables]
                usertable = [0, 1]
                """
                    .formatted(profile));
        for (int owner = 0; owner < 2; owner++) {
          toml.append(
              """

[[profiles.%s.instances]]
id = %d
url = '%s'
host = '127.0.0.1'
username = '%s'
password = ''
isolation = '%s'
client_port = %d
rpc_port = %d
"""
                  .formatted(
                      profile,
                      owner,
                      prefix + databases[owner],
                      USER,
                      isolationNames[owner],
                      clientPorts[owner],
                      rpcPorts[owner]));
        }
        Files.writeString(deployment, toml);
        for (int owner = 0; owner < 2; owner++) {
          servers.add(
              new ProcessBuilder(
                      Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                      "-cp",
                      System.getProperty(
                          "txnsails.test.serverClasspath", System.getProperty("java.class.path")),
                      "org.dbiir.txnsails.TxnSailsServer",
                      "-c",
                      deployment.toString(),
                      "--profile",
                      profile,
                      "--instance-id",
                      Integer.toString(owner))
                  .redirectErrorStream(true)
                  .redirectOutput(directory.resolve("owner" + owner + ".log").toFile())
                  .start());
        }
        for (int owner = 0; owner < 2; owner++) awaitOwner(owner);
        System.out.println("Native TOML PostgreSQL fixture logs: " + directory);
      } catch (Exception | AssertionError failure) {
        try {
          close();
        } catch (Exception cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    private void awaitOwner(int owner) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (true) {
        try (Client client = client(owner)) {
          ok(client.call("rollback"));
          return;
        } catch (IOException failure) {
          if (!(failure instanceof ConnectException)
              || !servers.get(owner).isAlive()
              || System.nanoTime() > deadline) {
            fail(
                "TOML owner failed to start: "
                    + Files.readString(directory.resolve("owner" + owner + ".log")),
                failure);
          }
          Thread.sleep(40);
        }
      }
    }

    Client client(int owner) throws IOException {
      return new Client(clientPorts[owner]);
    }

    Connection connect(String database) throws SQLException {
      return DriverManager.getConnection(prefix + database, USER, "");
    }

    int isolation(int owner) {
      return switch (isolationNames[owner]
          .toUpperCase(java.util.Locale.ROOT)
          .replace("TRANSACTION_", "")) {
        case "RC", "RC_TAILOR", "READ_COMMITTED" -> Connection.TRANSACTION_READ_COMMITTED;
        case "SI", "SI_TAILOR", "REPEATABLE_READ" -> Connection.TRANSACTION_REPEATABLE_READ;
        case "SER", "SERIALIZABLE", "DYNAMIC" -> Connection.TRANSACTION_SERIALIZABLE;
        default ->
            throw new IllegalArgumentException("Unknown test isolation " + isolationNames[owner]);
      };
    }

    void assertSerializableTracking(int owner) throws SQLException {
      try (Connection connection = connect("postgres");
          PreparedStatement sql =
              connection.prepareStatement(
                  "SELECT a.pid, count(l.pid) FILTER (WHERE l.mode='SIReadLock')"
                      + " FROM pg_stat_activity a LEFT JOIN pg_locks l ON l.pid=a.pid"
                      + " WHERE a.datname=? AND a.state='idle in transaction' GROUP BY a.pid")) {
        sql.setString(1, databases[owner]);
        try (ResultSet rows = sql.executeQuery()) {
          assertTrue(rows.next(), "Expected the one active native business branch");
          long predicateLocks = rows.getLong(2);
          if (isolation(owner) == Connection.TRANSACTION_SERIALIZABLE) {
            assertTrue(predicateLocks > 0, "SER branch must retain native SSI read tracking");
          } else {
            assertEquals(0, predicateLocks, "RC/SI branch must not use native SSI read tracking");
          }
          assertFalse(rows.next(), "Only one native business branch should be active");
        }
      }
    }

    String value(int key) throws SQLException {
      try (Connection connection = connect(databases[key % 2]);
          Statement sql = connection.createStatement();
          ResultSet rows = sql.executeQuery("SELECT field1 FROM usertable WHERE ycsb_key=" + key)) {
        assertTrue(rows.next());
        return rows.getString(1);
      }
    }

    long version(int key) throws SQLException {
      try (Connection connection = connect(databases[key % 2]);
          Statement sql = connection.createStatement();
          ResultSet rows = sql.executeQuery("SELECT vid FROM usertable WHERE ycsb_key=" + key)) {
        assertTrue(rows.next());
        return rows.getLong(1);
      }
    }

    int preparedCount() throws SQLException {
      try (Connection connection = connect("postgres");
          PreparedStatement sql =
              connection.prepareStatement(
                  "SELECT count(*) FROM pg_prepared_xacts WHERE database IN (?,?)")) {
        sql.setString(1, databases[0]);
        sql.setString(2, databases[1]);
        try (ResultSet rows = sql.executeQuery()) {
          assertTrue(rows.next());
          return rows.getInt(1);
        }
      }
    }

    @Override
    public void close() throws Exception {
      for (Process server : servers) {
        server.destroy();
        if (!server.waitFor(3, TimeUnit.SECONDS)) server.destroyForcibly().waitFor();
      }
      // Deduplicate the shared physical database, including partial-startup cleanup.
      for (String database : new LinkedHashSet<>(Arrays.asList(databases))) {
        if (database == null) continue;
        try (Connection connection = connect(database);
            Statement sql = connection.createStatement()) {
          List<String> pending = new ArrayList<>();
          try (ResultSet rows =
              sql.executeQuery(
                  "SELECT gid FROM pg_prepared_xacts WHERE database=current_database()")) {
            while (rows.next()) pending.add(rows.getString(1));
          }
          for (String gid : pending)
            sql.execute("ROLLBACK PREPARED '" + gid.replace("'", "''") + "'");
        }
        try (Connection connection = connect("postgres");
            Statement sql = connection.createStatement()) {
          sql.execute("DROP DATABASE IF EXISTS " + database + " WITH (FORCE)");
        }
      }
    }
  }

  private static int freePort(Set<Integer> allocated) throws IOException {
    while (true) {
      try (ServerSocket socket = new ServerSocket(0)) {
        if (allocated.add(socket.getLocalPort())) return socket.getLocalPort();
      }
    }
  }

  private static void ok(String response) {
    assertTrue(response != null && response.startsWith("OK"), response);
  }

  private static void errorState(String response, String state) {
    assertTrue(response != null && response.startsWith("ERROR#"), response);
    assertTrue(response.contains("#" + state + "#"), response);
  }

  private static String decode(String response) {
    ok(response);
    String data = response.substring(3);
    int length = Integer.parseInt(data.substring(2, 6), 16);
    return data.substring(6, 6 + length);
  }

  private static final class Client implements AutoCloseable {
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

    @Override
    public void close() throws IOException {
      socket.close();
    }
  }
}
