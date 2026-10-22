package org.dbiir.txnsails.common;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.List;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.DeploymentMode;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TomlDeploymentTest {
  @TempDir Path directory;

  @Test
  void selectsLocalOwnerAndResolvesPathsFromDeploymentFile() throws Exception {
    var loaded = load(config(), "shared", 1);
    var configuration = loaded.configuration();
    assertEquals(1, configuration.getInstanceID());
    assertEquals("jdbc:postgresql://127.0.0.1:5432/shared", configuration.getUrl());
    assertEquals(9877, loaded.clientPort());
    assertEquals(directory.resolve("schema.sql"), loaded.schema());
    assertEquals(directory.resolve("workload.toml"), loaded.workload());
    assertEquals(directory.resolve("state"), loaded.stateDirectory());
    assertEquals(List.of(0, 1), configuration.getTableName2DbList().get("usertable"));
    assertEquals(
        Connection.TRANSACTION_SERIALIZABLE,
        configuration
            .getInstances()
            .get(0)
            .jdbcIsolation(configuration.getConcurrencyControlType()));
    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        configuration
            .getInstances()
            .get(1)
            .jdbcIsolation(configuration.getConcurrencyControlType()));
    assertNull(configuration.getXmlConfig());
    assertTrue(configuration.isOptimizedDistributedValidation());
  }

  @Test
  void acceptsFullValidationAndOpenWorkloads() throws Exception {
    var loaded =
        load(
            config()
                .replace("workload = \"workload.toml\"", "")
                .replace("validation = \"optimized\"", "validation = \"full\""),
            "shared",
            0);
    assertNull(loaded.workload());
    assertFalse(loaded.configuration().isOptimizedDistributedValidation());
  }

  @Test
  void rejectsUnknownProfileAndMissingOwner() {
    assertThrows(IllegalArgumentException.class, () -> load(config(), "typo", 0));
    assertThrows(IllegalArgumentException.class, () -> load(config(), "shared", 2));
  }

  @Test
  void rejectsMisspelledConfigurationFields() {
    assertThrows(
        com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException.class,
        () -> load(config().replace("rpc_port", "rpc_prt"), "shared", 0));
  }

  @Test
  void rejectsDuplicateOwnerIdsAndUnknownPlacementOwners() {
    assertThrows(
        IllegalArgumentException.class,
        () -> load(config().replace("id = 1", "id = 0"), "shared", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> load(config().replace("[0, 1]", "[0, 2]"), "shared", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> load(config().replace("[0, 1]", "[0, 0]"), "shared", 0));
  }

  @Test
  void rejectsPortCollisionsIncludingRpcAndClientPorts() {
    assertThrows(
        IllegalArgumentException.class, () -> load(config().replace("9877", "9876"), "shared", 0));
    assertThrows(
        IllegalArgumentException.class, () -> load(config().replace("23334", "9876"), "shared", 0));
  }

  @Test
  void rejectsUnsupportedIsolationAndValidationModes() {
    assertThrows(
        IllegalArgumentException.class,
        () -> load(config().replace("isolation = \"SI\"", "isolation = \"RU\""), "shared", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> load(config().replace("\"optimized\"", "\"skip\""), "shared", 0));
  }

  @Test
  void rejectsUnsupportedVersionAndMissingSchema() {
    assertThrows(
        IllegalArgumentException.class,
        () -> load(config().replace("version = 1", "version = 2"), "shared", 0));
    assertThrows(
        IllegalArgumentException.class,
        () -> load(config().replace("schema = \"schema.sql\"", ""), "shared", 0));
  }

  @Test
  void rejectsUnsupportedBenchmarkBeforeOpeningDatabaseConnections() {
    assertThrows(
        IllegalArgumentException.class,
        () -> load(config().replace("benchmark = \"ycsb\"", "benchmark = \"typo\""), "shared", 0));
  }

  @Test
  void unifiedSingleSelectsOneNamedDatasourceAndMigratesRuntimeSettings() throws Exception {
    var loaded = loadMode(unified(), null, null);
    var configuration = loaded.configuration();
    assertEquals(DeploymentMode.SINGLE, configuration.getDeploymentMode());
    assertFalse(configuration.isDistributedMode());
    assertEquals("jdbc:postgresql://localhost/primary", configuration.getUrl());
    assertEquals("alice", configuration.getUsername());
    assertEquals("org.postgresql.Driver", configuration.getDriverClass());
    assertEquals(CCType.RC_TAILOR, configuration.getConcurrencyControlType());
    assertEquals(700, configuration.getInstanceID());
    assertEquals(11000, loaded.clientPort());
    assertNull(loaded.workload());
    assertEquals(5, configuration.getAuxiliaryConnections());
    assertEquals(17, configuration.getBatchSize());
    assertEquals(7, configuration.getMaxRetries());
    assertEquals(24, configuration.getTerminals());
    assertEquals(42, configuration.getRandomSeed());
    assertEquals(2.5, configuration.getScaleFactor());
    assertTrue(configuration.getNewConnectionPerTxn());
    assertFalse(configuration.getReconnectOnConnectionFailure());
    assertTrue(configuration.isParallelExecution());
  }

  @Test
  void modeOverrideSelectsDistributedDatasourceAndLocalSparseIdentity() throws Exception {
    var loaded = loadMode(unified(), "distributed", 9000);
    var configuration = loaded.configuration();
    assertEquals(DeploymentMode.DISTRIBUTED, configuration.getDeploymentMode());
    assertTrue(configuration.isDistributedMode());
    assertEquals(9000, configuration.getInstanceID());
    assertEquals("jdbc:postgresql://localhost/secondary", configuration.getUrl());
    assertEquals("bob", configuration.getUsername());
    assertEquals(11002, loaded.clientPort());
    assertEquals(List.of(300, 9000), configuration.getTableName2DbList().get("usertable"));
    assertEquals(directory.resolve("distributed.toml"), loaded.workload());
    assertEquals(2, configuration.getInstances().size());
    assertEquals(
        "jdbc:postgresql://localhost/primary", configuration.getInstances().get(0).getUrl());
    assertEquals(300, loadMode(unified(), "distributed", null).configuration().getInstanceID());
  }

  @Test
  void mockUsesOneSharedDatasourceAndRejectsPerNodeDatasourceOverrides() throws Exception {
    var loaded = loadMode(unified(), "mock", 9000);
    assertEquals(DeploymentMode.MOCK, loaded.configuration().getDeploymentMode());
    assertTrue(loaded.configuration().isDistributedMode());
    assertEquals(directory.resolve("mock.toml"), loaded.workload());
    assertTrue(
        loaded.configuration().getInstances().stream()
            .allMatch(instance -> instance.getUrl().equals("jdbc:postgresql://localhost/primary")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            loadMode(
                unified()
                    .replace(
                        "id = 9000\nhost = 'localhost'\nclient_port = 21002",
                        "id = 9000\n"
                            + "datasource = 'secondary'\n"
                            + "host = 'localhost'\n"
                            + "client_port = 21002"),
                "mock",
                9000));
  }

  @Test
  void unifiedDefaultsToSingleAndKeepsExplicitDistributedSingleNode() throws Exception {
    var single = loadMode(minimalSingle(), null, null);
    assertEquals(DeploymentMode.SINGLE, single.configuration().getDeploymentMode());
    assertEquals(9876, single.clientPort());
    assertEquals(0, single.configuration().getInstanceID());
    assertEquals(CCType.SER, single.configuration().getConcurrencyControlType());
    assertEquals(16, single.configuration().getAuxiliaryConnections());
    var distributed = loadMode(manyNodes(1, false), "distributed", 17);
    assertTrue(distributed.configuration().isDistributedMode());
    assertEquals(1, distributed.configuration().getInstances().size());
  }

  @Test
  void unifiedAndLegacyProfilesAcceptManySparseOwnersWithoutAnEightNodeLimit() throws Exception {
    var unified = loadMode(manyNodes(12, false), "distributed", 17 + 11 * 256);
    assertEquals(12, unified.configuration().getInstances().size());
    assertEquals(17 + 11 * 256, unified.configuration().getInstanceID());
    var legacy = load(manyNodes(12, true), "shared", 17 + 11 * 256);
    assertEquals(12, legacy.configuration().getInstances().size());
  }

  @Test
  void rejectsUnknownModesMissingReferencesAndInvalidSingleIdentity() {
    assertThrows(IllegalArgumentException.class, () -> loadMode(unified(), "typo", null));
    assertThrows(IllegalArgumentException.class, () -> loadMode(unified(), "single", 701));
    assertThrows(IllegalArgumentException.class, () -> loadMode(unified(), "distributed", -1));
    assertThrows(IllegalArgumentException.class, () -> loadMode(unified(), "distributed", 301));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            loadMode(
                unified().replace("datasource = 'secondary'", "datasource = 'missing'"),
                "distributed",
                9000));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            loadMode(
                minimalSingle().replace("datasource = 'primary'", "datasource = 'missing'"),
                null,
                null));
  }

  @Test
  void unifiedRejectsDuplicateOwnersEndpointsAndUnknownPlacementOwners() {
    assertThrows(
        IllegalArgumentException.class,
        () -> loadMode(unified().replace("id = 9000", "id = 300"), "distributed", 300));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            loadMode(
                unified().replace("client_port = 11002", "client_port = 11001"),
                "distributed",
                300));
    assertThrows(
        IllegalArgumentException.class,
        () -> loadMode(unified().replace("[300, 9000]", "[300, 301]"), "distributed", 300));
    assertThrows(
        IllegalArgumentException.class,
        () -> loadMode(unified().replace("[300, 9000]", "[]"), "distributed", 300));
  }

  @Test
  void unifiedRejectsInvalidRuntimeSettingsAndConcurrencyModes() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            loadMode(
                unified().replace("auxiliary_connections = 5", "auxiliary_connections = 0"),
                null,
                null));
    assertThrows(
        IllegalArgumentException.class,
        () -> loadMode(unified().replace("scale_factor = 2.5", "scale_factor = -1"), null, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            loadMode(
                unified()
                    .replace("concurrency_control = 'rc_tailor'", "concurrency_control = 'NUM_CC'"),
                null,
                null));
    assertThrows(
        com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException.class,
        () ->
            loadMode(
                unified().replace("auxiliary_connections", "auxiliary_connection"), null, null));
  }

  @Test
  void concurrencyAliasesNormalizeAndUnknownValuesFailExplicitly() {
    WorkloadConfiguration configuration = new WorkloadConfiguration();
    configuration.setConcurrencyControlType(" ser ");
    assertEquals(CCType.SER, configuration.getConcurrencyControlType());
    configuration.setConcurrencyControlType("rc_tailor");
    assertEquals(CCType.RC_TAILOR, configuration.getConcurrencyControlType());
    assertThrows(
        IllegalArgumentException.class, () -> configuration.setConcurrencyControlType("typo"));
    assertThrows(
        IllegalArgumentException.class, () -> configuration.setConcurrencyControlType("NUM_CC"));
  }

  @Test
  void legacyConfigurationStillInfersModeWhenNoModeIsExplicit() {
    WorkloadConfiguration configuration = new WorkloadConfiguration();
    assertNull(configuration.getDeploymentMode());
    assertFalse(configuration.isDistributedMode());
    configuration.setInstances(List.of(new DBInstance(), new DBInstance()));
    assertTrue(configuration.isDistributedMode());
    configuration.setDeploymentMode(DeploymentMode.SINGLE);
    assertFalse(configuration.isDistributedMode());
    configuration.setInstances(List.of());
    configuration.setDeploymentMode(DeploymentMode.MOCK);
    assertTrue(configuration.isDistributedMode());
  }

  private TomlDeployment.Loaded loadMode(String content, String mode, Integer owner)
      throws Exception {
    Path path = directory.resolve("deployment.toml");
    Files.writeString(path, content);
    return TomlDeployment.loadMode(path, mode, owner);
  }

  private String minimalSingle() {
    return """
           version = 2
           benchmark = 'ycsb'
           schema = 'schema.sql'
           [datasources.primary]
           url = 'jdbc:postgresql://localhost/primary'
           [single]
           datasource = 'primary'
           """;
  }

  private String manyNodes(int count, boolean legacy) {
    String prefix = legacy ? "profiles.shared" : "distributed";
    StringBuilder text =
        new StringBuilder(
            legacy
                ? "version = 1\nbenchmark = 'ycsb'\nschema = 'schema.sql'\n"
                : "version = 2\n"
                    + "benchmark = 'ycsb'\n"
                    + "schema = 'schema.sql'\n"
                    + "[datasources.primary]\n"
                    + "url = 'jdbc:postgresql://localhost/primary'\n");
    text.append('[').append(prefix).append(".tables]\nusertable = [");
    for (int i = 0; i < count; i++) {
      if (i > 0) text.append(", ");
      text.append(17 + i * 256);
    }
    text.append("]\n");
    for (int i = 0; i < count; i++) {
      text.append("[[")
          .append(prefix)
          .append(legacy ? ".instances]]\n" : ".nodes]]\n")
          .append("id = ")
          .append(17 + i * 256)
          .append("\nhost = 'localhost'\nclient_port = ")
          .append(30000 + i * 2)
          .append("\nrpc_port = ")
          .append(30001 + i * 2)
          .append("\nisolation = 'SI'\n")
          .append(
              legacy
                  ? "url = 'jdbc:postgresql://localhost/primary'\n"
                  : "datasource = 'primary'\n");
    }
    return text.toString();
  }

  private String unified() {
    return """
           version = 2
           mode = 'single'
           benchmark = 'ycsb'
           schema = 'schema.sql'
           validation = 'optimized'
           state_directory = 'state'
           [runtime]
           auxiliary_connections = 5
           batch_size = 17
           max_retries = 7
           terminals = 24
           random_seed = 42
           scale_factor = 2.5
           new_connection_per_txn = true
           reconnect_on_connection_failure = false
           parallel_execution = true
           [datasources.primary]
           url = 'jdbc:postgresql://localhost/primary'
           username = 'alice'
           password = 'primary-secret'
           [datasources.secondary]
           url = 'jdbc:postgresql://localhost/secondary'
           username = 'bob'
           password = 'secondary-secret'
           [single]
           datasource = 'primary'
           client_port = 11000
           concurrency_control = 'rc_tailor'
           node_id = 700
           [distributed]
           workload = 'distributed.toml'
           [distributed.tables]
           usertable = [300, 9000]
           [[distributed.nodes]]
           id = 300
           host = 'localhost'
           client_port = 11001
           rpc_port = 12001
           isolation = 'SI'
           datasource = 'primary'
           [[distributed.nodes]]
           id = 9000
           host = 'localhost'
           client_port = 11002
           rpc_port = 12002
           isolation = 'SER'
           datasource = 'secondary'
           [mock]
           datasource = 'primary'
           workload = 'mock.toml'
           [mock.tables]
           usertable = [300, 9000]
           [[mock.nodes]]
           id = 300
           host = 'localhost'
           client_port = 21001
           rpc_port = 22001
           isolation = 'SI'
           [[mock.nodes]]
           id = 9000
           host = 'localhost'
           client_port = 21002
           rpc_port = 22002
           isolation = 'SER'
           """;
  }

  private TomlDeployment.Loaded load(String content, String profile, int owner) throws Exception {
    Path path = directory.resolve("deployment.toml");
    Files.writeString(path, content);
    return TomlDeployment.load(path, profile, owner);
  }

  private String config() {
    return """
           version = 1
           benchmark = "ycsb"
           schema = "schema.sql"
           workload = "workload.toml"
           validation = "optimized"
           state_directory = "state"

           [profiles.shared.tables]
           usertable = [0, 1]

           [[profiles.shared.instances]]
           id = 0
           host = "127.0.0.1"
           url = "jdbc:postgresql://127.0.0.1:5432/shared"
           client_port = 9876
           rpc_port = 23333
           isolation = "SER"

           [[profiles.shared.instances]]
           id = 1
           host = "127.0.0.1"
           url = "jdbc:postgresql://127.0.0.1:5432/shared"
           client_port = 9877
           rpc_port = 23334
           isolation = "SI"
           """;
  }
}
