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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/** Real PostgreSQL execution through each mode of the same native version-2 deployment. */
@EnabledIfEnvironmentVariable(named = "TXNSAILS_TEST_PG_PORT", matches = ".+")
class UnifiedDeploymentPostgresTest {
  private static final List<Integer> DISTRIBUTED_IDS = List.of(0, 256);
  private static final List<Integer> MOCK_IDS =
      List.of(0, 7, 42, 256, 65536, 1000000, 16777216, 1073741824, Integer.MAX_VALUE);

  @Test
  void singleModeUsesDynamicRegistrationWithoutTheDistributedCatalogue() throws Exception {
    try (Fixture fixture = new Fixture("single");
        Client client = fixture.client(0)) {
      assertEquals(
          "OK#0",
          client.call(
              "register#dynamic#read#usertable#SELECT field1 FROM usertable WHERE ycsb_key = ?"));
      assertEquals(
          "OK#1",
          client.call(
              "register#dynamic#write#usertable#UPDATE usertable SET field1 = ? WHERE ycsb_key ="
                  + " ?"));
      ok(client.call("analysis"));
      assertEquals("0", decode(client.call("execute#dynamic#0#0")));
      ok(client.call("execute#dynamic#1#committed#0"));
      ok(client.call("commit"));
      assertEquals("committed", fixture.value(0));
      assertEquals(1, fixture.version(0));
      ok(client.call("execute#dynamic#1#discard#0"));
      ok(client.call("rollback"));
      assertEquals("committed", decode(client.call("execute#dynamic#0#0")));
      ok(client.call("commit"));
      assertEquals("committed", fixture.value(0));
      assertEquals(1, fixture.version(0));
      assertEquals(0, fixture.preparedCount());
    }
  }

  @Test
  void distributedModeUsesSeparateDatasourcesWithSparseCoordinators() throws Exception {
    try (Fixture fixture = new Fixture("distributed")) {
      for (int coordinator : DISTRIBUTED_IDS) {
        int even = coordinator == 0 ? 0 : 2;
        try (Client client = fixture.client(coordinator)) {
          ok(client.call("begin#pair"));
          ok(client.call("execute#pair#1#left#" + even));
          ok(client.call("execute#pair#1#right#" + (even + 1)));
          assertEquals(2, fixture.activeBusinessBranches());
          ok(client.call("commit"));
          assertEquals("left", fixture.value(even));
          assertEquals("right", fixture.value(even + 1));
          assertEquals(1, fixture.version(even));
          assertEquals(1, fixture.version(even + 1));
          assertEquals(0, fixture.preparedCount());

          ok(client.call("begin#pair"));
          ok(client.call("execute#pair#1#discard#" + even));
          ok(client.call("execute#pair#1#discard#" + (even + 1)));
          ok(client.call("rollback"));
          ok(client.call("begin#pair"));
          assertEquals("right", decode(client.call("execute#pair#0#" + (even + 1))));
          assertEquals("left", decode(client.call("execute#pair#0#" + even)));
          ok(client.call("commit"));
          assertEquals(1, fixture.version(even));
          assertEquals(1, fixture.version(even + 1));
          assertEquals(0, fixture.preparedCount());
        }
      }
    }
  }

  @Test
  void mockModeCommitsAndRollsBackNineSparseLogicalOwnersInOneDatabase() throws Exception {
    try (Fixture fixture = new Fixture("mock")) {
      int committedVersion = 0;
      for (int coordinator : List.of(0, 256)) {
        String value = "coordinator" + coordinator;
        try (Client client = fixture.client(coordinator)) {
          ok(client.call("begin#pair"));
          // Repeating one declared slot reaches nine independently managed native branches.
          for (int key = 0; key < MOCK_IDS.size(); key++)
            ok(client.call("execute#pair#1#" + value + "#" + key));
          assertEquals(MOCK_IDS.size(), fixture.activeBusinessBranches());
          ok(client.call("commit"));
          committedVersion++;
          for (int key = 0; key < MOCK_IDS.size(); key++) {
            assertEquals(value, fixture.value(key));
            assertEquals(committedVersion, fixture.version(key));
          }
          assertEquals(0, fixture.preparedCount());

          ok(client.call("begin#pair"));
          for (int key = MOCK_IDS.size() - 1; key >= 0; key--)
            ok(client.call("execute#pair#1#discard#" + key));
          assertEquals(MOCK_IDS.size(), fixture.activeBusinessBranches());
          ok(client.call("rollback"));
          // Reuse the same worker for a read-only transaction over all nine owners.
          ok(client.call("begin#pair"));
          for (int key = 0; key < MOCK_IDS.size(); key++)
            assertEquals(value, decode(client.call("execute#pair#0#" + key)));
          assertEquals(MOCK_IDS.size(), fixture.activeBusinessBranches());
          ok(client.call("commit"));
          for (int key = 0; key < MOCK_IDS.size(); key++) {
            assertEquals(value, fixture.value(key));
            assertEquals(committedVersion, fixture.version(key));
          }
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
    private final String mode;
    private final Path directory;
    private final String prefix;
    private final Map<Integer, Process> servers = new LinkedHashMap<>();
    private final Map<Integer, Integer> clientPorts = new LinkedHashMap<>();
    private final Map<String, String> databases = new LinkedHashMap<>();
    private final Set<String> createdDatabases = new LinkedHashSet<>();

    Fixture(String mode) throws Exception {
      this.mode = mode;
      directory = Files.createTempDirectory("txnsails-unified-" + mode + "-");
      prefix = "jdbc:postgresql://127.0.0.1:" + System.getenv("TXNSAILS_TEST_PG_PORT") + "/";
      try {
        String suffix = Long.toUnsignedString(System.nanoTime());
        for (String datasource : List.of("shared", "shard0", "shard1")) {
          String database = "txnsails_unified_" + suffix + "_" + datasource;
          databases.put(datasource, database);
          try (Connection connection = connect("postgres");
              Statement sql = connection.createStatement()) {
            sql.execute("CREATE DATABASE " + database);
            createdDatabases.add(database);
          }
          try (Connection connection = connect(database);
              Statement sql = connection.createStatement()) {
            sql.execute(DDL);
            for (int key = 0; key < 18; key++) {
              if (datasource.equals("shared")
                  || Math.floorMod(key, 2) == (datasource.equals("shard0") ? 0 : 1))
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
        Path deployment = directory.resolve("unified.toml");
        Set<Integer> allocated = new LinkedHashSet<>();
        int singlePort = freePort(allocated);
        Map<Integer, Integer> distributedPorts = allocatePorts(DISTRIBUTED_IDS, allocated);
        Map<Integer, Integer> mockPorts = allocatePorts(MOCK_IDS, allocated);
        StringBuilder toml =
            new StringBuilder(
                """
                version = 2
                mode = 'single'
                benchmark = 'ycsb'
                schema = 'schema.sql'
                validation = 'optimized'
                state_directory = 'state'

                [runtime]
                auxiliary_connections = 1
                """);
        for (Map.Entry<String, String> datasource : databases.entrySet()) {
          toml.append(
              """

[datasources.%s]
url = '%s'
username = '%s'
password = ''
"""
                  .formatted(datasource.getKey(), prefix + datasource.getValue(), USER));
        }
        toml.append(
            """

[single]
datasource = 'shared'
client_port = %d
concurrency_control = 'SERIALIZABLE'

[distributed]
workload = 'workload.toml'

[distributed.tables]
usertable = %s
"""
                .formatted(singlePort, DISTRIBUTED_IDS));
        for (int index = 0; index < DISTRIBUTED_IDS.size(); index++) {
          int owner = DISTRIBUTED_IDS.get(index);
          appendNode(
              toml,
              "distributed",
              owner,
              distributedPorts.get(owner),
              freePort(allocated),
              index == 0 ? "SER" : "SI",
              "shard" + index);
        }
        toml.append(
            """

[mock]
datasource = 'shared'
workload = 'workload.toml'

[mock.tables]
usertable = %s
"""
                .formatted(MOCK_IDS));
        for (int index = 0; index < MOCK_IDS.size(); index++) {
          int owner = MOCK_IDS.get(index);
          appendNode(
              toml,
              "mock",
              owner,
              mockPorts.get(owner),
              freePort(allocated),
              index % 2 == 0 ? "SER" : "SI",
              null);
        }
        Files.writeString(deployment, toml);
        if (mode.equals("single")) clientPorts.put(0, singlePort);
        else clientPorts.putAll(mode.equals("distributed") ? distributedPorts : mockPorts);
        for (int owner : clientPorts.keySet()) {
          List<String> command = new ArrayList<>();
          command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
          command.add("-Xmx128m");
          command.add("-cp");
          command.add(
              System.getProperty(
                  "txnsails.test.serverClasspath", System.getProperty("java.class.path")));
          command.addAll(
              List.of(
                  "org.dbiir.txnsails.TxnSailsServer",
                  "-c",
                  deployment.toString(),
                  "--mode",
                  mode));
          if (!mode.equals("single"))
            command.addAll(List.of("--instance-id", Integer.toString(owner)));
          servers.put(
              owner,
              new ProcessBuilder(command)
                  .directory(directory.toFile())
                  .redirectErrorStream(true)
                  .redirectOutput(ownerLog(owner).toFile())
                  .start());
        }
        for (int owner : clientPorts.keySet()) awaitOwner(owner);
        System.out.println("Unified " + mode + " PostgreSQL fixture logs: " + directory);
      } catch (Exception | AssertionError failure) {
        try {
          close();
        } catch (Exception cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    private static Map<Integer, Integer> allocatePorts(List<Integer> owners, Set<Integer> allocated)
        throws IOException {
      Map<Integer, Integer> ports = new LinkedHashMap<>();
      for (int owner : owners) ports.put(owner, freePort(allocated));
      return ports;
    }

    private static void appendNode(
        StringBuilder toml,
        String mode,
        int owner,
        int clientPort,
        int rpcPort,
        String isolation,
        String datasource) {
      toml.append(
          """

[[%s.nodes]]
id = %d
host = '127.0.0.1'
client_port = %d
rpc_port = %d
isolation = '%s'
"""
              .formatted(mode, owner, clientPort, rpcPort, isolation));
      if (datasource != null) toml.append("datasource = '" + datasource + "'\n");
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
              || System.nanoTime() > deadline)
            fail(
                "Unified " + mode + " owner " + owner + ": " + Files.readString(ownerLog(owner)),
                failure);
          Thread.sleep(40);
        }
      }
    }

    private Path ownerLog(int owner) {
      return directory.resolve("owner" + owner + ".log");
    }

    Client client(int owner) throws IOException {
      return new Client(clientPorts.get(owner));
    }

    private Connection connect(String database) throws SQLException {
      return DriverManager.getConnection(prefix + database, USER, "");
    }

    private String databaseForKey(int key) {
      return databases.get(mode.equals("distributed") ? "shard" + Math.floorMod(key, 2) : "shared");
    }

    String value(int key) throws SQLException {
      try (Connection connection = connect(databaseForKey(key));
          Statement sql = connection.createStatement();
          ResultSet rows = sql.executeQuery("SELECT field1 FROM usertable WHERE ycsb_key=" + key)) {
        assertTrue(rows.next());
        return rows.getString(1);
      }
    }

    long version(int key) throws SQLException {
      try (Connection connection = connect(databaseForKey(key));
          Statement sql = connection.createStatement();
          ResultSet rows = sql.executeQuery("SELECT vid FROM usertable WHERE ycsb_key=" + key)) {
        assertTrue(rows.next());
        return rows.getLong(1);
      }
    }

    int activeBusinessBranches() throws SQLException {
      return count(
          "SELECT count(*) FROM pg_stat_activity WHERE state='idle in transaction' AND datname");
    }

    int preparedCount() throws SQLException {
      return count("SELECT count(*) FROM pg_prepared_xacts WHERE database");
    }

    private int count(String query) throws SQLException {
      String placeholders =
          createdDatabases.stream().map(ignored -> "?").collect(Collectors.joining(","));
      try (Connection connection = connect("postgres");
          PreparedStatement sql =
              connection.prepareStatement(query + " IN (" + placeholders + ")")) {
        int parameter = 1;
        for (String database : createdDatabases) sql.setString(parameter++, database);
        try (ResultSet rows = sql.executeQuery()) {
          assertTrue(rows.next());
          return rows.getInt(1);
        }
      }
    }

    @Override
    public void close() throws Exception {
      for (Process server : servers.values()) {
        server.destroy();
        if (!server.waitFor(3, TimeUnit.SECONDS)) server.destroyForcibly().waitFor();
      }
      for (String database : createdDatabases) {
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
