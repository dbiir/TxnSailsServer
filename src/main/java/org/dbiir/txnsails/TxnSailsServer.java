package org.dbiir.txnsails;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.dbiir.txnsails.analysis.ClosedWorkload;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.DistributedConfig;
import org.dbiir.txnsails.common.DistributedTable;
import org.dbiir.txnsails.common.JacksonXmlConfiguration;
import org.dbiir.txnsails.common.TomlDeployment;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.DatabaseType;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.transaction.DistributionInfo;
import org.dbiir.txnsails.execution.transaction.TransactionManager;
import org.dbiir.txnsails.execution.utils.FileUtil;
import org.dbiir.txnsails.execution.validation.ValidationMetaTable;
import org.dbiir.txnsails.worker.Flusher;
import org.dbiir.txnsails.worker.MetaWorker;

public class TxnSailsServer {
  private static Thread flushThread;
  private static ServerSocket serverSocket;
  private static ExecutorService threadPool;
  static boolean running = true;

  public static void main(String[] args)
      throws InterruptedException, ParseException, SQLException, IOException {
    CommandLineParser parser = new DefaultParser();
    Options options =
        new Options()
            .addOption("c", "config", true, "[required] Workload configuration file")
            .addOption("n", "profile", true, "Deployment profile in a TOML configuration")
            .addOption("m", "mode", true, "Execution mode: single, distributed or mock")
            .addOption("i", "instance-id", true, "Local owner ID in a TOML configuration")
            .addOption("d", "directory", true, "Base directory for the meta files")
            .addOption("s", "schema", true, "Base directory for the schema sql files")
            .addOption("p", "phase", true, "Online predict or offline training")
            .addOption("g", "distributed-config", true, "Distributed database configuration")
            .addOption("w", "closed-workload", true, "Immutable distributed workload catalogue")
            .addOption("l", "listen-port", true, "Client port (default 9876)")
            .addOption(
                "v", "validation", true, "Distributed validation: optimized (default) or full")
            .addOption(
                "r",
                "state-directory",
                true,
                "Durable transaction ID directory (default .txnsails)");

    CommandLine argsLine = parser.parse(options, args);

    String configFile = argsLine.getOptionValue("c", "config/txnsails.toml").trim();
    final WorkloadConfiguration workloadConfiguration;
    String schemaFile;
    String closedWorkloadFile = null;
    String stateDirectory = ".txnsails";
    int listenPort = 9876;
    if (configFile.toLowerCase(Locale.ROOT).endsWith(".toml")) {
      if (argsLine.hasOption("g") || argsLine.hasOption("s") || argsLine.hasOption("w"))
        throw new IllegalArgumentException(
            "Set schema, workload and owners in the TOML deployment");
      if (argsLine.hasOption("profile")
          && (!argsLine.hasOption("instance-id") || argsLine.hasOption("mode")))
        throw new IllegalArgumentException("Legacy --profile requires --instance-id and no --mode");
      var deployment =
          argsLine.hasOption("profile")
              ? TomlDeployment.load(
                  java.nio.file.Path.of(configFile),
                  argsLine.getOptionValue("profile"),
                  Integer.parseInt(argsLine.getOptionValue("instance-id")))
              : TomlDeployment.loadMode(
                  java.nio.file.Path.of(configFile),
                  argsLine.getOptionValue("mode"),
                  argsLine.hasOption("instance-id")
                      ? Integer.valueOf(argsLine.getOptionValue("instance-id"))
                      : null);
      workloadConfiguration = deployment.configuration();
      schemaFile = deployment.schema().toString();
      closedWorkloadFile = deployment.workload() == null ? null : deployment.workload().toString();
      stateDirectory = deployment.stateDirectory().toString();
      listenPort = deployment.clientPort();
      DistributionInfo.setTableName2DbList(workloadConfiguration.getTableName2DbList());
    } else {
      if (argsLine.hasOption("profile")
          || argsLine.hasOption("instance-id")
          || argsLine.hasOption("mode"))
        throw new IllegalArgumentException("--mode, --profile and --instance-id require TOML");
      if (!argsLine.hasOption("s")) throw new IllegalArgumentException("--schema is required");
      schemaFile = argsLine.getOptionValue("s").trim();
      workloadConfiguration = loadConfiguration(buildConfiguration(configFile));
      if (argsLine.hasOption("g"))
        loadDatabaseInfo(
            workloadConfiguration, buildDistributedConfig(argsLine.getOptionValue("g").trim()));
      if (argsLine.hasOption("w")) closedWorkloadFile = argsLine.getOptionValue("w").trim();
    }
    MetaWorker.getINSTANCE().setSchema(new SchemaInfo(schemaFile));

    // get the number of available cores
    int availableProcessors = Runtime.getRuntime().availableProcessors();
    MetaWorker.MAX_AVAILABLE_CORES = (int) Math.ceil(availableProcessors * 0.75);

    AtomicInteger genWorkerId = new AtomicInteger(0);
    if (argsLine.hasOption("v")) {
      String validationMode = argsLine.getOptionValue("v");
      if (!validationMode.equals("optimized") && !validationMode.equals("full"))
        throw new IllegalArgumentException("Validation mode must be optimized or full");
      workloadConfiguration.setOptimizedDistributedValidation(validationMode.equals("optimized"));
    }
    boolean distributed = workloadConfiguration.isDistributedMode();
    if (closedWorkloadFile != null) {
      if (!distributed)
        throw new IllegalArgumentException("A closed workload requires distributed mode");
      ClosedWorkload catalogue =
          ClosedWorkload.load(
              java.nio.file.Path.of(closedWorkloadFile),
              MetaWorker.getINSTANCE().getSchema(),
              workloadConfiguration);
      workloadConfiguration.setClosedWorkload(catalogue);
      MetaWorker.getINSTANCE().installClosedWorkload(catalogue);
      long retained =
          catalogue.readMasks().keySet().stream()
              .filter(
                  site ->
                      !workloadConfiguration.isOptimizedDistributedValidation()
                          || catalogue.retainRead(site.program(), site.slot(), site.owner()))
              .count();
      System.out.println(
          "Closed workload "
              + catalogue.fingerprint()
              + ": retained read-site owners="
              + retained
              + "/"
              + catalogue.readMasks().size());
    }
    List<Connection> auxiliaryConnectionList = makeAuxiliaryConnections(workloadConfiguration);
    if (distributed) {
      for (var instance : workloadConfiguration.getInstances()) {
        try (Connection connection =
                StringUtils.isEmpty(instance.getUsername())
                    ? DriverManager.getConnection(instance.getUrl())
                    : DriverManager.getConnection(
                        instance.getUrl(), instance.getUsername(), instance.getPassword());
            var statement = connection.createStatement();
            var prepared =
                statement.executeQuery(
                    "SELECT gid FROM pg_prepared_xacts WHERE database = current_database()")) {
          if (prepared.next())
            throw new SQLException(
                "Resolve prepared transactions before restarting TxnSails: "
                    + prepared.getString(1),
                "08007");
          if (workloadConfiguration.isOptimizedDistributedValidation()
              && instance.jdbcIsolation(workloadConfiguration.getConcurrencyControlType())
                  == Connection.TRANSACTION_SERIALIZABLE) {
            int major = connection.getMetaData().getDatabaseMajorVersion();
            if (major < 15 || major > 18)
              throw new SQLException(
                  "Optimized SER certification requires PostgreSQL 15–18; use -v full", "0A000");
          }
        }
      }
      org.dbiir.txnsails.execution.utils.TransactionIdGenerator.configure(
          java.nio.file.Path.of(argsLine.getOptionValue("r", stateDirectory))
              .resolve("instance-" + workloadConfiguration.getInstanceID() + ".ids"),
          workloadConfiguration.getInstanceID());
      ValidationMetaTable.getInstance()
          .initDistributed(workloadConfiguration.getBenchmarkName(), auxiliaryConnectionList);
      TransactionManager.getInstance().init(workloadConfiguration);
    } else if (workloadConfiguration.getConcurrencyControlType() != CCType.SER) {
      // Fixed native SERIALIZABLE execution does not use middleware hotspot validation.
      ValidationMetaTable.getInstance()
          .initHotspot(workloadConfiguration.getBenchmarkName(), auxiliaryConnectionList);
    } else {
      ValidationMetaTable.getInstance()
          .initSingleSerializable(
              workloadConfiguration.getBenchmarkName(), auxiliaryConnectionList);
    }

    threadPool = Executors.newFixedThreadPool(128);
    try {
      if (!distributed) {
        createFlushThread(
            argsLine,
            workloadConfiguration.getBenchmarkName(),
            workloadConfiguration.getConcurrencyControlType());
      } else {
        org.dbiir.txnsails.worker.Adapter.getInstance().setUsed(false);
      }
      serverSocket =
          new ServerSocket(
              Integer.parseInt(argsLine.getOptionValue("l", Integer.toString(listenPort))));
      System.out.println(
          "TxnSails owner "
              + workloadConfiguration.getInstanceID()
              + " listening for clients on "
              + serverSocket.getLocalPort());
      while (running) {
        try {
          Socket clientSocket = serverSocket.accept();
          threadPool.submit(
              new ClientHandler(clientSocket, workloadConfiguration, genWorkerId.addAndGet(1)));
        } catch (IOException e) {
          if (running) {
            System.out.println(List.of(e.getStackTrace()));
          }
        }
      }
    } finally {
      finishFlushThread();
      threadPool.shutdown();
      if (distributed) TransactionManager.getInstance().close();
      for (Connection connection : auxiliaryConnectionList) connection.close();
      System.out.println("Server stopped.");
    }
  }

  public static JacksonXmlConfiguration buildConfiguration(String filename) throws IOException {
    //    String currentPath = System.getProperty("user.dir");
    //    System.out.println("Current working directory: " + currentPath);
    //    System.out.println("Loading configuration from " + filename);

    XmlMapper xmlMapper = new XmlMapper();
    return xmlMapper.readValue(new File(filename), JacksonXmlConfiguration.class);
  }

  private static DistributedConfig buildDistributedConfig(String filename) throws IOException {
    XmlMapper xmlMapper = new XmlMapper();
    return xmlMapper.readValue(new File(filename), DistributedConfig.class);
  }

  private static WorkloadConfiguration loadConfiguration(JacksonXmlConfiguration xmlConfig) {
    // ----------------------------------------------------------------
    // BEGIN LOADING BENCHMARK CONFIGURATION
    // Simplify the evaluation, the config file is provided by application, TxnSails would provide
    // an api in future
    // ----------------------------------------------------------------

    WorkloadConfiguration wrkld = new WorkloadConfiguration();
    wrkld.setXmlConfig(xmlConfig);

    // Pull in database configuration
    wrkld.setDatabaseType(DatabaseType.get(xmlConfig.getType()));
    wrkld.setDriverClass(xmlConfig.getDriver());
    wrkld.setUrl(xmlConfig.getUrl());
    wrkld.setUsername(xmlConfig.getUsername());
    wrkld.setPassword(xmlConfig.getPassword());
    wrkld.setRandomSeed(xmlConfig.getRandomSeed());
    wrkld.setBatchSize(xmlConfig.getBatchsize());
    wrkld.setMaxRetries(xmlConfig.getMaxRetries());
    wrkld.setNewConnectionPerTxn(xmlConfig.isNewConnectionPerTxn());
    wrkld.setReconnectOnConnectionFailure(xmlConfig.isReconnectOnConnectionFailure());
    wrkld.setBenchmarkName(xmlConfig.getBenchmark().toLowerCase());

    int terminals = xmlConfig.getTerminals();
    wrkld.setTerminals(terminals);

    wrkld.setScaleFactor(xmlConfig.getScalefactor());
    String type = xmlConfig.getConcurrencyControlType();
    wrkld.setConcurrencyControlType(type);

    wrkld.setParallelExecution(xmlConfig.isParallelExecution());
    wrkld.setInstanceID(xmlConfig.getInstanceID());

    return wrkld;
  }

  private static void loadDatabaseInfo(WorkloadConfiguration wrkld, DistributedConfig xmlConfig) {
    wrkld.setInstances(xmlConfig.getInstances().getInstanceList());
    Set<Integer> ids = new HashSet<>();
    for (var instance : wrkld.getInstances()) {
      if (instance.getId() < 0 || !ids.add(instance.getId()))
        throw new IllegalArgumentException("Invalid or duplicate database ID");
    }
    var local =
        wrkld.getInstances().stream()
            .filter(instance -> instance.getId() == wrkld.getInstanceID())
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Missing local database instance"));
    wrkld.setUrl(local.getUrl());
    wrkld.setUsername(local.getUsername());
    wrkld.setPassword(local.getPassword());

    Map<String, List<Integer>> tableName2DbList = new HashMap<>();
    for (DistributedTable table : xmlConfig.getTables().getTableList()) {
      List<Integer> locations = table.getLocations().getLocationList();
      if (locations == null || locations.isEmpty() || !ids.containsAll(locations)) {
        throw new IllegalArgumentException("Invalid placement for " + table.getName());
      }
      tableName2DbList.put(table.getName().toLowerCase(Locale.ROOT), locations);
    }
    wrkld.setTableName2DbList(tableName2DbList);
    DistributionInfo.setTableName2DbList(tableName2DbList);
  }

  private static List<Connection> makeAuxiliaryConnections(WorkloadConfiguration workConf)
      throws SQLException {
    if (StringUtils.isBlank(workConf.getUrl()))
      throw new IllegalArgumentException(
          "Datasource URL is missing; configure datasource settings in config/txnsails.toml");
    List<Connection> connectionList = new ArrayList<>(workConf.getAuxiliaryConnections());
    for (int i = 0; i < workConf.getAuxiliaryConnections(); i++) {
      if (StringUtils.isEmpty(workConf.getUsername())) {
        connectionList.add(DriverManager.getConnection(workConf.getUrl()));
      } else {
        connectionList.add(
            DriverManager.getConnection(
                workConf.getUrl(), workConf.getUsername(), workConf.getPassword()));
      }
    }
    return connectionList;
  }

  private static void createFlushThread(CommandLine argsLine, String benchmark, CCType ccType) {
    String metaDirectory = "metas";
    boolean onlinePredict = false;
    if (argsLine.hasOption("d")) {
      metaDirectory = argsLine.getOptionValue("d").trim();
      if (metaDirectory.contains("results")) {
        metaDirectory = metaDirectory.replace("results", "metas");
        int lastIndex = metaDirectory.lastIndexOf("_");

        if (lastIndex != -1) {
          metaDirectory = metaDirectory.substring(0, lastIndex - 1);
        }
        metaDirectory += "/";
      }
    }

    if (argsLine.hasOption("p")) {
      if (argsLine.getOptionValue("p").contains("online")) {
        onlinePredict = true;
      }
    }

    FileUtil.makeDirIfNotExists(metaDirectory);
    flushThread = new Thread(new Flusher(benchmark, metaDirectory, ccType, onlinePredict));
    flushThread.start();
  }

  private static void finishFlushThread() {
    if (flushThread != null) flushThread.interrupt();
  }

  public static void closeServer() throws IOException {
    running = false;
    if (serverSocket != null && !serverSocket.isClosed()) {
      serverSocket.close();
    }
  }
}
