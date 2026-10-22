package org.dbiir.txnsails.common;

import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.DatabaseType;
import org.dbiir.txnsails.common.types.DeploymentMode;
import org.dbiir.txnsails.execution.WorkloadConfiguration;

/** Native deployment settings; version 2 selects its protocol explicitly, even for one node. */
public final class TomlDeployment {
  private TomlDeployment() {}

  public record Loaded(
      WorkloadConfiguration configuration,
      Path schema,
      Path workload,
      Path stateDirectory,
      int clientPort) {}

  public static Loaded loadMode(Path path, String modeOverride, Integer instanceId)
      throws IOException {
    ModeDocument document = mapper().readValue(path.toFile(), ModeDocument.class);
    if (!Integer.valueOf(2).equals(document.version)) throw invalid("version must be 2");
    DeploymentMode mode = DeploymentMode.parse(modeOverride == null ? document.mode : modeOverride);
    WorkloadConfiguration configuration =
        baseConfiguration(document.benchmark, document.validation);
    configuration.setDeploymentMode(mode);
    applyRuntime(configuration, document.runtime);
    if (instanceId != null && instanceId < 0) throw invalid("owner IDs must be nonnegative");
    Path base = path.toAbsolutePath().normalize().getParent();
    Path schema = base.resolve(text(document.schema, "schema")).normalize();
    Path state = base.resolve(text(document.stateDirectory, "state_directory")).normalize();
    if (mode == DeploymentMode.SINGLE) {
      Single single = document.single;
      if (single == null) throw invalid("single mode requires a [single] section");
      if (single.nodeId == null || single.nodeId < 0)
        throw invalid("single.node_id must be nonnegative");
      if (instanceId != null && !instanceId.equals(single.nodeId)) {
        throw invalid("single mode has only configured node " + single.nodeId);
      }
      var datasource = datasource(document.datasources, single.datasource);
      applyDatasource(configuration, datasource);
      configuration.setInstanceID(single.nodeId);
      configuration.setConcurrencyControlType(
          text(single.concurrencyControl, "single.concurrency_control"));
      configuration.setIsolationMode(nativeIsolation(configuration.getConcurrencyControlType()));
      checkPort("127.0.0.1", single.clientPort, new HashSet<>());
      return new Loaded(configuration, schema, null, state, single.clientPort);
    }

    NodeGroup selected = mode == DeploymentMode.MOCK ? document.mock : document.distributed;
    if (selected == null || selected.nodes == null || selected.nodes.isEmpty()) {
      throw invalid(mode.name().toLowerCase(Locale.ROOT) + " mode requires at least one node");
    }
    if (mode == DeploymentMode.DISTRIBUTED && selected.datasource != null) {
      throw invalid("distributed nodes must each declare their datasource");
    }
    ResolvedDatasource shared =
        mode == DeploymentMode.MOCK ? datasource(document.datasources, selected.datasource) : null;
    Set<Integer> ids = new HashSet<>();
    Set<String> endpoints = new HashSet<>();
    List<DBInstance> instances = new ArrayList<>();
    Integer localId = instanceId;
    Integer clientPort = null;
    for (Node node : selected.nodes) {
      if (node == null || node.id == null || node.id < 0 || !ids.add(node.id)) {
        throw invalid("invalid or duplicate owner ID");
      }
      if (mode == DeploymentMode.MOCK && node.datasource != null) {
        throw invalid("mock nodes use only the mock section's shared datasource");
      }
      ResolvedDatasource source =
          shared != null ? shared : datasource(document.datasources, node.datasource);
      if (source.type() != DatabaseType.POSTGRES) {
        throw invalid("distributed and mock nodes require PostgreSQL datasources");
      }
      String host = text(node.host, "owner host");
      checkPort(host, node.clientPort, endpoints);
      checkPort(host, node.rpcPort, endpoints);
      DBInstance instance =
          new DBInstance(node.id, source.url(), host, source.username(), source.password());
      instance.setRpcPort(node.rpcPort);
      instance.setIsolation(text(node.isolation, "owner isolation"));
      try {
        instance.jdbcIsolation(configuration.getConcurrencyControlType());
      } catch (SQLException exception) {
        throw invalid(exception.getMessage());
      }
      instances.add(instance);
      if (localId == null) localId = node.id;
      if (localId.equals(node.id)) {
        configuration.setInstanceID(node.id);
        applyDatasource(configuration, source);
        clientPort = node.clientPort;
      }
    }
    if (clientPort == null) throw invalid("missing local owner " + localId);
    configuration.setInstances(List.copyOf(instances));
    configuration.setTableName2DbList(placement(selected.tables, ids));
    return new Loaded(
        configuration,
        schema,
        selected.workload == null
            ? null
            : base.resolve(text(selected.workload, "workload")).normalize(),
        state,
        clientPort);
  }

  public static Loaded load(Path path, String profile, int instanceId) throws IOException {
    Document document = mapper().readValue(path.toFile(), Document.class);
    if (!Integer.valueOf(1).equals(document.version)) throw invalid("version must be 1");
    if (document.profiles == null || !document.profiles.containsKey(profile))
      throw invalid("unknown profile: " + profile);
    Profile selected = document.profiles.get(profile);
    if (selected == null || selected.instances == null || selected.instances.size() < 2)
      throw invalid("a distributed profile requires at least two owners");
    String validation = text(document.validation, "validation");
    if (!validation.equals("optimized") && !validation.equals("full"))
      throw invalid("validation must be optimized or full");

    WorkloadConfiguration configuration = new WorkloadConfiguration();
    configuration.setBenchmarkName(text(document.benchmark, "benchmark").toLowerCase(Locale.ROOT));
    if (!Set.of("ycsb", "smallbank", "tpcc").contains(configuration.getBenchmarkName()))
      throw invalid("benchmark must be ycsb, smallbank or tpcc");
    configuration.setDatabaseType(DatabaseType.POSTGRES);
    configuration.setDriverClass("org.postgresql.Driver");
    configuration.setInstanceID(instanceId);
    configuration.setOptimizedDistributedValidation(validation.equals("optimized"));
    Set<Integer> ids = new HashSet<>();
    Set<String> endpoints = new HashSet<>();
    List<DBInstance> instances = new ArrayList<>();
    Integer clientPort = null;
    for (Instance declared : selected.instances) {
      if (declared == null || declared.id == null || declared.id < 0 || !ids.add(declared.id))
        throw invalid("invalid or duplicate owner ID");
      String host = text(declared.host, "owner host");
      checkPort(host, declared.clientPort, endpoints);
      checkPort(host, declared.rpcPort, endpoints);
      String url = text(declared.url, "owner URL");
      if (!url.startsWith("jdbc:postgresql:")) throw invalid("owners require PostgreSQL JDBC URLs");
      DBInstance instance =
          new DBInstance(declared.id, url, host, declared.username, declared.password);
      instance.setRpcPort(declared.rpcPort);
      instance.setIsolation(text(declared.isolation, "owner isolation"));
      try {
        instance.jdbcIsolation(configuration.getConcurrencyControlType());
      } catch (SQLException e) {
        throw invalid(e.getMessage());
      }
      instances.add(instance);
      if (declared.id == instanceId) {
        configuration.setUrl(url);
        configuration.setUsername(declared.username);
        configuration.setPassword(declared.password);
        clientPort = declared.clientPort;
      }
    }
    if (clientPort == null) throw invalid("missing local owner " + instanceId);
    configuration.setInstances(List.copyOf(instances));
    if (selected.tables == null || selected.tables.isEmpty())
      throw invalid("table placement is required");
    Map<String, List<Integer>> placement = new LinkedHashMap<>();
    for (var entry : selected.tables.entrySet()) {
      String table = text(entry.getKey(), "table").toLowerCase(Locale.ROOT);
      List<Integer> owners = entry.getValue();
      if (owners == null
          || owners.isEmpty()
          || new HashSet<>(owners).size() != owners.size()
          || !ids.containsAll(owners)
          || placement.putIfAbsent(table, List.copyOf(owners)) != null)
        throw invalid("invalid placement for " + table);
    }
    configuration.setTableName2DbList(Map.copyOf(placement));
    Path base = path.toAbsolutePath().normalize().getParent();
    return new Loaded(
        configuration,
        base.resolve(text(document.schema, "schema")).normalize(),
        document.workload == null
            ? null
            : base.resolve(text(document.workload, "workload")).normalize(),
        base.resolve(text(document.stateDirectory, "state_directory")).normalize(),
        clientPort);
  }

  private static TomlMapper mapper() {
    TomlMapper mapper = new TomlMapper();
    mapper.setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    return mapper;
  }

  private static WorkloadConfiguration baseConfiguration(String benchmark, String validation) {
    WorkloadConfiguration configuration = new WorkloadConfiguration();
    String name = text(benchmark, "benchmark").toLowerCase(Locale.ROOT);
    if (!Set.of("ycsb", "smallbank", "tpcc").contains(name)) {
      throw invalid("benchmark must be ycsb, smallbank or tpcc");
    }
    configuration.setBenchmarkName(name);
    String policy = text(validation, "validation").toLowerCase(Locale.ROOT);
    if (!policy.equals("optimized") && !policy.equals("full")) {
      throw invalid("validation must be optimized or full");
    }
    configuration.setOptimizedDistributedValidation(policy.equals("optimized"));
    return configuration;
  }

  private static void applyRuntime(WorkloadConfiguration configuration, RuntimeSettings declared) {
    RuntimeSettings runtime = declared == null ? new RuntimeSettings() : declared;
    if (runtime.auxiliaryConnections < 1
        || runtime.batchSize < 1
        || runtime.maxRetries < 0
        || runtime.terminals < 1
        || !Double.isFinite(runtime.scaleFactor)
        || runtime.scaleFactor <= 0) {
      throw invalid("invalid runtime connection, batch, retry, terminal or scale settings");
    }
    configuration.setAuxiliaryConnections(runtime.auxiliaryConnections);
    configuration.setBatchSize(runtime.batchSize);
    configuration.setMaxRetries(runtime.maxRetries);
    configuration.setTerminals(runtime.terminals);
    configuration.setRandomSeed(runtime.randomSeed);
    configuration.setScaleFactor(runtime.scaleFactor);
    configuration.setNewConnectionPerTxn(runtime.newConnectionPerTxn);
    configuration.setReconnectOnConnectionFailure(runtime.reconnectOnConnectionFailure);
    configuration.setParallelExecution(runtime.parallelExecution);
  }

  private record ResolvedDatasource(
      String url, String username, String password, String driver, DatabaseType type) {}

  private static ResolvedDatasource datasource(Map<String, Datasource> sources, String reference) {
    String name = text(reference, "datasource reference");
    Datasource source = sources == null ? null : sources.get(name);
    if (source == null) throw invalid("unknown datasource: " + name);
    String typeName = text(source.type, "datasource type").toUpperCase(Locale.ROOT);
    DatabaseType type = DatabaseType.get(typeName.equals("POSTGRESQL") ? "POSTGRES" : typeName);
    if (type == null) throw invalid("unsupported datasource type: " + source.type);
    String url = text(source.url, "datasource URL");
    if (type == DatabaseType.POSTGRES && !url.startsWith("jdbc:postgresql:")) {
      throw invalid("PostgreSQL datasources require PostgreSQL JDBC URLs");
    }
    return new ResolvedDatasource(
        url, source.username, source.password, text(source.driver, "datasource driver"), type);
  }

  private static void applyDatasource(
      WorkloadConfiguration configuration, ResolvedDatasource source) {
    configuration.setDatabaseType(source.type());
    configuration.setDriverClass(source.driver());
    configuration.setUrl(source.url());
    configuration.setUsername(source.username());
    configuration.setPassword(source.password());
  }

  private static int nativeIsolation(CCType type) {
    return switch (type) {
      case RC, RC_TAILOR -> Connection.TRANSACTION_READ_COMMITTED;
      case SI, SI_TAILOR -> Connection.TRANSACTION_REPEATABLE_READ;
      default -> Connection.TRANSACTION_SERIALIZABLE;
    };
  }

  private static Map<String, List<Integer>> placement(
      Map<String, List<Integer>> tables, Set<Integer> ids) {
    if (tables == null || tables.isEmpty()) throw invalid("table placement is required");
    Map<String, List<Integer>> placement = new LinkedHashMap<>();
    for (var entry : tables.entrySet()) {
      String table = text(entry.getKey(), "table").toLowerCase(Locale.ROOT);
      List<Integer> owners = entry.getValue();
      if (owners == null
          || owners.isEmpty()
          || new HashSet<>(owners).size() != owners.size()
          || !ids.containsAll(owners)
          || placement.putIfAbsent(table, List.copyOf(owners)) != null) {
        throw invalid("invalid placement for " + table);
      }
    }
    return Map.copyOf(placement);
  }

  private static void checkPort(String host, Integer port, Set<String> endpoints) {
    if (port == null || port < 1 || port > 65535)
      throw invalid("ports must be between 1 and 65535");
    String normalized = host.toLowerCase(Locale.ROOT);
    if (normalized.equals("localhost") || normalized.equals("::1")) normalized = "127.0.0.1";
    if (!endpoints.add(normalized + ":" + port)) throw invalid("duplicate client/RPC endpoint");
  }

  private static String text(String value, String field) {
    if (value == null || value.isBlank()) throw invalid(field + " is required");
    return value.trim();
  }

  private static IllegalArgumentException invalid(String reason) {
    return new IllegalArgumentException("Invalid TOML deployment: " + reason);
  }

  public static final class Document {
    public Integer version;
    public String benchmark;
    public String schema;
    public String workload;
    public String validation = "optimized";
    public String stateDirectory = ".txnsails";
    public Map<String, Profile> profiles;
  }

  public static final class ModeDocument {
    public Integer version;
    public String mode = "single";
    public String benchmark;
    public String schema;
    public String validation = "optimized";
    public String stateDirectory = ".txnsails";
    public RuntimeSettings runtime = new RuntimeSettings();
    public Map<String, Datasource> datasources;
    public Single single;
    public NodeGroup distributed;
    public NodeGroup mock;
  }

  public static final class RuntimeSettings {
    public int auxiliaryConnections = 16;
    public int batchSize = 128;
    public int maxRetries = 3;
    public int terminals = 128;
    public int randomSeed = -1;
    public double scaleFactor = 1.0;
    public boolean newConnectionPerTxn;
    public boolean reconnectOnConnectionFailure;
    public boolean parallelExecution;
  }

  public static final class Datasource {
    public String url;
    public String username = "";
    public String password = "";
    public String driver = "org.postgresql.Driver";
    public String type = "POSTGRES";
  }

  public static final class Single {
    public String datasource;
    public Integer clientPort = 9876;
    public String concurrencyControl = "SERIALIZABLE";
    public Integer nodeId = 0;
  }

  public static final class NodeGroup {
    public String datasource;
    public String workload;
    public List<Node> nodes;
    public Map<String, List<Integer>> tables;
  }

  public static final class Node {
    public Integer id;
    public String host = "127.0.0.1";
    public Integer clientPort;
    public Integer rpcPort;
    public String isolation;
    public String datasource;
  }

  public static final class Profile {
    public List<Instance> instances;
    public Map<String, List<Integer>> tables;
  }

  public static final class Instance {
    public Integer id;
    public String url;
    public String host = "127.0.0.1";
    public String username = "";
    public String password = "";
    public String isolation;
    public Integer clientPort;
    public Integer rpcPort;
  }
}
