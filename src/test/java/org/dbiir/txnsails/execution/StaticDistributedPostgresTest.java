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

/** Closed catalogues, actual PostgreSQL branches, and independent owner JVMs. */
@EnabledIfEnvironmentVariable(named = "TXNSAILS_TEST_PG_PORT", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StaticDistributedPostgresTest {
  private final Map<String, Fixture> fixtures = new LinkedHashMap<>();

  @AfterAll
  void stopOwners() throws Exception {
    Exception failure = null;
    for (Fixture fixture : fixtures.values()) {
      try {
        fixture.close();
      } catch (Exception ex) {
        if (failure == null) failure = ex;
        else failure.addSuppressed(ex);
      }
    }
    fixtures.clear();
    if (failure != null) throw failure;
  }

  private Fixture fixture(String profile) throws Exception {
    Fixture fixture = fixtures.get(profile);
    if (fixture == null) {
      // A pair of owners keeps 32 auxiliary connections; do not accumulate
      // every profile on test servers configured with 100 connection slots.
      stopOwners();
      fixture = new Fixture(profile);
      fixtures.put(profile, fixture);
    }
    return fixture;
  }

  @Test
  void prunedGlobalReadCanSerializeBeforeSingleOwnerWriter() throws Exception {
    Fixture fixture = fixture("prunable");
    try (Client reader = fixture.client(0);
        Client writer = fixture.client(1)) {
      ok(reader.call("begin#read_pair"));
      assertEquals("0", decode(reader.call("execute#read_pair#0#0")));
      ok(writer.call("begin#write_a"));
      ok(writer.call("execute#write_a#0#new#0"));
      ok(writer.call("commit"));
      assertEquals("0", decode(reader.call("execute#read_pair#1#1")));
      // Its only conflicting dependencies are on SER owner A. Static analysis
      // may omit both read items even though this transaction visits two owners.
      ok(reader.call("commit"));
      assertEquals("new", fixture.value(0));
      assertEquals(1, fixture.version(0));
      assertEquals(0, fixture.preparedCount());
      ok(reader.call("begin#read_pair"));
      assertEquals("new", decode(reader.call("execute#read_pair#0#0")));
      ok(reader.call("execute#read_pair#1#1"));
      ok(reader.call("commit"));
    }
  }

  @Test
  void fullValidationRetainsTheSameOtherwisePrunableGlobalRead() throws Exception {
    Fixture fixture = fixture("full");
    try (Client reader = fixture.client(0);
        Client writer = fixture.client(1)) {
      ok(reader.call("begin#read_pair"));
      ok(reader.call("execute#read_pair#0#0"));
      ok(writer.call("begin#write_a"));
      ok(writer.call("execute#write_a#0#new#0"));
      ok(writer.call("commit"));
      ok(reader.call("execute#read_pair#1#1"));
      errorState(reader.call("commit"), "40001");
      assertEquals("new", fixture.value(0));
      assertEquals(0, fixture.preparedCount());
    }
  }

  @Test
  void retainedMultiOwnerCycleRejectsFracturedSnapshot() throws Exception {
    Fixture fixture = fixture("cyclic");
    try (Client reader = fixture.client(0);
        Client writer = fixture.client(1)) {
      ok(reader.call("begin#read_pair"));
      assertEquals("0", decode(reader.call("execute#read_pair#0#0")));
      ok(writer.call("begin#write_pair"));
      ok(writer.call("execute#write_pair#0#new#0"));
      ok(writer.call("execute#write_pair#1#new#1"));
      ok(writer.call("commit"));
      assertEquals("new", decode(reader.call("execute#read_pair#1#1")));
      errorState(reader.call("commit"), "40001");
      assertEquals(0, fixture.preparedCount());
      // The failed transaction is fully cleaned up before this client is reused.
      ok(reader.call("begin#read_pair"));
      assertEquals("new", decode(reader.call("execute#read_pair#0#0")));
      assertEquals("new", decode(reader.call("execute#read_pair#1#1")));
      ok(reader.call("commit"));
    }
  }

  @Test
  void retainedSiPivotReadRejectsStaleLocalReadWriteTransaction() throws Exception {
    Fixture fixture = fixture("cyclic");
    try (Client pivot = fixture.client(0);
        Client writer = fixture.client(1)) {
      ok(pivot.call("begin#si_pivot"));
      assertEquals("0", decode(pivot.call("execute#si_pivot#0#13")));
      ok(writer.call("begin#write_b"));
      ok(writer.call("execute#write_b#0#new#13"));
      ok(writer.call("commit"));
      ok(pivot.call("execute#si_pivot#1#discard#15"));
      errorState(pivot.call("commit"), "40001");
      assertEquals("new", fixture.value(13));
      assertEquals("0", fixture.value(15));
    }
  }

  @Test
  void repeatedAndOmittedSlotsRemainValidAndReadOnlyGlobalCommitUsesTwoPhaseCommit()
      throws Exception {
    Fixture fixture = fixture("prunable");
    try (Client client = fixture.client(1)) {
      ok(client.call("begin#read_pair"));
      assertEquals("0", decode(client.call("execute#read_pair#0#2")));
      assertEquals("0", decode(client.call("execute#read_pair#0#2")));
      assertEquals("0", decode(client.call("execute#read_pair#1#3")));
      assertEquals("0", decode(client.call("execute#read_pair#1#3")));
      ok(client.call("commit"));
      assertEquals(0, fixture.preparedCount());
      ok(client.call("begin#read_pair"));
      // Omitting the other slot makes this an actual local transaction.
      ok(client.call("execute#read_pair#1#5"));
      ok(client.call("commit"));
    }
  }

  @Test
  void beginRequiresADeclaredProgramAndFailedBeginDoesNotPoisonTheClient() throws Exception {
    Fixture fixture = fixture("prunable");
    try (Client client = fixture.client(0)) {
      error(client.call("begin"));
      error(client.call("begin#unknown_program"));
      ok(client.call("begin#read_pair"));
      ok(client.call("execute#read_pair#0#6"));
      ok(client.call("execute#read_pair#1#7"));
      ok(client.call("commit"));
    }
  }

  @Test
  void mixedProgramExecutionIsRejectedBeforeSqlAndRollsBackEarlierWrites() throws Exception {
    Fixture fixture = fixture("prunable");
    try (Client client = fixture.client(0)) {
      ok(client.call("begin#write_a"));
      ok(client.call("execute#write_a#0#discard#8"));
      error(client.call("execute#read_pair#0#8"));
      error(client.call("execute#write_a#0#also_discard#10"));
      error(client.call("commit"));
      assertEquals("0", fixture.value(8));
      assertEquals("0", fixture.value(10));
      ok(client.call("begin#write_a"));
      ok(client.call("execute#write_a#0#kept#10"));
      ok(client.call("commit"));
      assertEquals("kept", fixture.value(10));
    }
  }

  @Test
  void unknownSlotRejectsBeforeSqlAndMakesTheWholeTransactionRollbackOnly() throws Exception {
    Fixture fixture = fixture("prunable");
    try (Client client = fixture.client(0)) {
      ok(client.call("begin#write_a"));
      ok(client.call("execute#write_a#0#discard#12"));
      error(client.call("execute#write_a#1#never#14"));
      error(client.call("commit"));
      assertEquals("0", fixture.value(12));
      assertEquals("0", fixture.value(14));
      ok(client.call("begin#read_pair"));
      ok(client.call("execute#read_pair#0#12"));
      ok(client.call("commit"));
    }
  }

  @Test
  void malformedExecuteMakesTheWholeTransactionRollbackOnly() throws Exception {
    Fixture fixture = fixture("prunable");
    try (Client client = fixture.client(0)) {
      ok(client.call("begin#write_a"));
      ok(client.call("execute#write_a#0#discard#22"));
      error(client.call("execute"));
      error(client.call("commit"));
      assertEquals("0", fixture.value(22));
      ok(client.call("begin#read_pair"));
      ok(client.call("execute#read_pair#0#22"));
      ok(client.call("commit"));
    }
  }

  @Test
  void ownerDomainIsCheckedBeforeSqlAndEarlierBranchIsRolledBack() throws Exception {
    Fixture fixture = fixture("prunable");
    try (Client client = fixture.client(0)) {
      ok(client.call("begin#write_a"));
      ok(client.call("execute#write_a#0#discard#16"));
      // Odd key17 is physically present on B, outside this statement's A-only domain.
      error(client.call("execute#write_a#0#never#17"));
      error(client.call("commit"));
      assertEquals("0", fixture.value(16));
      assertEquals("0", fixture.value(17));
      assertEquals(0, fixture.version(17));
    }
  }

  @Test
  void closedCatalogueRejectsDynamicRegistrationAndAnalysis() throws Exception {
    Fixture fixture = fixture("prunable");
    try (Client client = fixture.client(0)) {
      error(
          client.call(
              "register#rogue#write#usertable#UPDATE usertable SET field1=? WHERE ycsb_key=?"));
      error(client.call("analysis"));
      ok(client.call("begin#read_pair"));
      ok(client.call("execute#read_pair#0#18"));
      ok(client.call("execute#read_pair#1#19"));
      ok(client.call("commit"));
    }
  }

  @Test
  void mismatchedOwnerCataloguesRejectBeginOnBothCoordinatorsBeforeBusinessSql() throws Exception {
    Fixture fixture = fixture("mismatch");
    for (int owner = 0; owner < 2; owner++) {
      try (Client client = fixture.client(owner)) {
        error(client.call("begin#write_a"));
        error(client.call("execute#write_a#0#never#20"));
        error(client.call("begin#read_pair"));
      }
    }
    assertEquals("0", fixture.value(20));
    assertEquals(0, fixture.version(20));
    assertEquals(0, fixture.preparedCount());
  }

  private static String statement(int slot, String operation, int owner) {
    String sql =
        operation.equals("read")
            ? "SELECT field1 FROM usertable WHERE ycsb_key = ?"
            : "UPDATE usertable SET field1 = ? WHERE ycsb_key = ?";
    return """
           <statement id="%d" operation="%s" relation="usertable">
             <sql>%s</sql><owners><owner>%d</owner></owners>
           </statement>
           """
        .formatted(slot, operation, sql, owner);
  }

  private static String program(String name, String... statements) {
    return "<program name=\"" + name + "\">" + String.join("", statements) + "</program>";
  }

  private static String catalogue(boolean cyclic, boolean mismatch) {
    String programs =
        program("read_pair", statement(0, "read", 0), statement(1, "read", 1))
            + program("write_a", statement(0, "write", 0));
    if (cyclic) {
      programs +=
          program("write_pair", statement(0, "write", 0), statement(1, "write", 1))
              + program("write_b", statement(0, "write", 1))
              + program("si_pivot", statement(0, "read", 1), statement(1, "write", 1));
    }
    if (mismatch) programs += program("extra_read_b", statement(0, "read", 1));
    return "<workload version=\"1\" closedWorld=\"true\">" + programs + "</workload>";
  }

  private static final class Fixture implements AutoCloseable {
    private final List<Process> servers = new ArrayList<>();
    private final String[] databases = new String[2];
    private final int[] ports = new int[2];
    private final Path directory;
    private final String prefix;
    private static final String USER = "txnsails_test";

    Fixture(String profile) throws Exception {
      directory = Files.createTempDirectory("txnsails-static-" + profile + "-");
      prefix = "jdbc:postgresql://127.0.0.1:" + System.getenv("TXNSAILS_TEST_PG_PORT") + "/";
      try {
        String suffix = Long.toUnsignedString(System.nanoTime());
        int[] rpcPorts = {freePort(), freePort()};
        Path[] configurations = new Path[2];
        Path[] catalogues = new Path[2];
        for (int owner = 0; owner < 2; owner++) {
          databases[owner] = "txnsails_static_" + suffix + "_" + owner;
          try (Connection connection = connect("postgres");
              Statement sql = connection.createStatement()) {
            sql.execute("CREATE DATABASE " + databases[owner]);
          }
          try (Connection connection = connect(databases[owner]);
              Statement sql = connection.createStatement()) {
            sql.execute(
                "CREATE TABLE usertable (ycsb_key INT PRIMARY KEY, vid BIGINT NOT NULL DEFAULT 0,"
                    + " field1 VARCHAR(100))");
            for (int key = owner; key < 40; key += 2)
              sql.execute("INSERT INTO usertable VALUES (" + key + ",0,'0')");
          }
          ports[owner] = freePort();
          configurations[owner] = directory.resolve("owner" + owner + ".xml");
          Files.writeString(
              configurations[owner],
              """
<parameters><type>POSTGRES</type><driver>org.postgresql.Driver</driver>
<url>%s</url><username>%s</username><password></password><instanceId>%d</instanceId>
<concurrencyControlType>SERIALIZABLE</concurrencyControlType><benchmark>ycsb</benchmark></parameters>
"""
                  .formatted(prefix + databases[owner], USER, owner));
          catalogues[owner] = directory.resolve("catalogue" + owner + ".xml");
          Files.writeString(
              catalogues[owner],
              catalogue(profile.equals("cyclic"), profile.equals("mismatch") && owner == 1));
        }
        Path distributed = directory.resolve("distributed.xml");
        Files.writeString(
            distributed,
            """
<parameters><instances>
  <instance><id>0</id><url>%s</url><host>127.0.0.1</host><username>%s</username><password></password><isolation>SER</isolation><rpcPort>%d</rpcPort></instance>
  <instance><id>1</id><url>%s</url><host>127.0.0.1</host><username>%s</username><password></password><isolation>SI</isolation><rpcPort>%d</rpcPort></instance>
</instances><tables><table><name>usertable</name><locations><location>0</location><location>1</location></locations></table></tables></parameters>
"""
                .formatted(
                    prefix + databases[0],
                    USER,
                    rpcPorts[0],
                    prefix + databases[1],
                    USER,
                    rpcPorts[1]));
        for (int owner = 0; owner < 2; owner++) {
          List<String> command =
              new ArrayList<>(
                  List.of(
                      Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                      "-cp",
                      System.getProperty(
                          "txnsails.test.serverClasspath", System.getProperty("java.class.path")),
                      "org.dbiir.txnsails.TxnSailsServer",
                      "-c",
                      configurations[owner].toString(),
                      "-g",
                      distributed.toString(),
                      "-s",
                      "config/ycsb.sql",
                      "-w",
                      catalogues[owner].toString(),
                      "-l",
                      Integer.toString(ports[owner]),
                      "-r",
                      directory.resolve("state").toString()));
          if (profile.equals("full")) command.addAll(List.of("-v", "full"));
          servers.add(
              new ProcessBuilder(command)
                  .redirectErrorStream(true)
                  .redirectOutput(directory.resolve("owner" + owner + ".log").toFile())
                  .start());
        }
        for (int owner = 0; owner < 2; owner++) awaitOwner(owner);
        System.out.println("Closed PostgreSQL fixture logs: " + directory);
      } catch (Exception | AssertionError ex) {
        try {
          close();
        } catch (Exception cleanup) {
          ex.addSuppressed(cleanup);
        }
        throw ex;
      }
    }

    private void awaitOwner(int owner) throws Exception {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
      while (true) {
        try (Client ignored = client(owner)) {
          return;
        } catch (ConnectException ex) {
          if (!servers.get(owner).isAlive() || System.nanoTime() > deadline) {
            fail(
                "Owner failed to start: "
                    + Files.readString(directory.resolve("owner" + owner + ".log")),
                ex);
          }
          Thread.sleep(40);
        }
      }
    }

    Client client(int owner) throws IOException {
      return new Client(ports[owner]);
    }

    Connection connect(String database) throws SQLException {
      return DriverManager.getConnection(prefix + database, USER, "");
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
      for (String database : databases) {
        if (database == null) continue;
        // Failed assertions must not strand prepared branches in the shared test server.
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

  private static void ok(String response) {
    assertTrue(response != null && response.startsWith("OK"), response);
  }

  private static void error(String response) {
    assertTrue(response != null && response.startsWith("ERROR#"), response);
  }

  private static void errorState(String response, String state) {
    error(response);
    assertTrue(response.contains("#" + state + "#"), response);
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
