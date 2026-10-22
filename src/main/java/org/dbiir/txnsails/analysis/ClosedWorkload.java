package org.dbiir.txnsails.analysis;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.dataformat.toml.TomlMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.dbiir.txnsails.analysis.DistributedDependencyAnalysis.AccessSite;
import org.dbiir.txnsails.analysis.DistributedDependencyAnalysis.Masks;
import org.dbiir.txnsails.analysis.DistributedDependencyAnalysis.SiteOwner;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.common.TransactionTemplate;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.transaction.DistributedSQL;

/**
 * Immutable startup declaration; negative analysis results require every owner to use this hash.
 */
public final class ClosedWorkload {
  private static final String ANALYSIS_VERSION = "txnsails-closed-scc-v1";
  private static final int MAX_STATEMENTS = 15;
  private final String fingerprint;
  private final Map<String, TransactionTemplate> templates;
  private final Map<String, List<Statement>> programs;
  private final Map<SiteOwner, Boolean> retainedReads;
  private final Map<SiteOwner, Masks> readMasks;

  private record Statement(int slot, String table, boolean write, String sql, Set<Integer> owners) {
    Statement {
      owners = Set.copyOf(owners);
    }
  }

  private ClosedWorkload(
      String fingerprint,
      Map<String, TransactionTemplate> templates,
      Map<String, List<Statement>> programs,
      Map<SiteOwner, Boolean> retainedReads,
      Map<SiteOwner, Masks> readMasks) {
    this.fingerprint = fingerprint;
    this.templates = Map.copyOf(templates);
    this.programs = Map.copyOf(programs);
    this.retainedReads = Map.copyOf(retainedReads);
    this.readMasks = Map.copyOf(readMasks);
  }

  public static ClosedWorkload load(
      Path path, SchemaInfo schema, WorkloadConfiguration configuration)
      throws IOException, SQLException {
    Document document =
        path.toString().toLowerCase(Locale.ROOT).endsWith(".toml")
            ? readToml(path)
            : new XmlMapper().readValue(path.toFile(), Document.class);
    if (!Integer.valueOf(1).equals(document.version)
        || !Boolean.TRUE.equals(document.closedWorld)) {
      throw invalid("version=1 and closedWorld=true are required");
    }
    if (document.programs == null || document.programs.isEmpty()) {
      throw invalid("at least one program is required");
    }
    Map<Integer, DBInstance> instances = new TreeMap<>();
    Map<Integer, Integer> isolations = new TreeMap<>();
    for (DBInstance instance : configuration.getInstances()) {
      if (instance == null
          || instance.getId() < 0
          || instances.putIfAbsent(instance.getId(), instance) != null) {
        throw invalid("invalid or duplicate database owner");
      }
      isolations.put(
          instance.getId(), instance.jdbcIsolation(configuration.getConcurrencyControlType()));
    }
    if (instances.isEmpty()) throw invalid("database owners are required");
    Map<String, List<Integer>> placement = new TreeMap<>();
    for (var entry : configuration.getTableName2DbList().entrySet()) {
      String table = text(entry.getKey(), "placement table").toLowerCase(Locale.ROOT);
      List<Integer> owners = entry.getValue();
      if (owners == null
          || owners.isEmpty()
          || !instances.keySet().containsAll(owners)
          || placement.putIfAbsent(table, List.copyOf(owners)) != null) {
        throw invalid("invalid placement for " + table);
      }
    }

    Map<String, List<Statement>> programs = new TreeMap<>();
    Map<String, TransactionTemplate> templates = new TreeMap<>();
    List<AccessSite> sites = new ArrayList<>();
    for (Program declared : document.programs) {
      String name = text(declared.name, "program name");
      if (name.indexOf('#') >= 0 || programs.containsKey(name)) {
        throw invalid("invalid or duplicate program " + name);
      }
      if (declared.statements == null
          || declared.statements.isEmpty()
          || declared.statements.size() > MAX_STATEMENTS) {
        throw invalid("programs require between 1 and " + MAX_STATEMENTS + " statements");
      }
      Map<Integer, Statement> slots = new TreeMap<>();
      for (SQL declaredSQL : declared.statements) {
        if (declaredSQL.id == null || declaredSQL.id < 0) throw invalid("statement id is required");
        String operation = text(declaredSQL.operation, "operation").toLowerCase(Locale.ROOT);
        if (!operation.equals("read") && !operation.equals("write")) {
          throw invalid("operation must be read or write");
        }
        String table = text(declaredSQL.relation, "relation").toLowerCase(Locale.ROOT);
        List<Integer> locations = placement.get(table);
        if (locations == null || schema.getColumnInfoByTableName(table) == null) {
          throw invalid("missing schema or placement for " + table);
        }
        Set<Integer> owners = new HashSet<>(locations);
        if (declaredSQL.owners != null) {
          List<Integer> explicit = declaredSQL.owners.values;
          if (explicit == null
              || explicit.isEmpty()
              || new HashSet<>(explicit).size() != explicit.size()
              || !owners.containsAll(explicit)) {
            throw invalid("invalid owner domain for " + name + ":" + declaredSQL.id);
          }
          owners = new HashSet<>(explicit);
        }
        Statement statement =
            new Statement(
                declaredSQL.id,
                table,
                operation.equals("write"),
                text(declaredSQL.sql, "SQL"),
                owners);
        if (slots.putIfAbsent(statement.slot(), statement) != null) {
          throw invalid("duplicate statement id in " + name);
        }
      }
      TransactionTemplate template = new TransactionTemplate(name);
      List<Statement> statements = new ArrayList<>(slots.size());
      for (int slot = 0; slot < slots.size(); slot++) {
        Statement statement = slots.get(slot);
        if (statement == null) throw invalid("statement ids must be contiguous from zero");
        template.addTemplateSQL(statement.write() ? 1 : 0, statement.table(), statement.sql());
        DistributedSQL.plan(template.getSQLTemplateByIndex(slot), schema);
        statements.add(statement);
        sites.add(
            new AccessSite(name, slot, statement.table(), statement.write(), statement.owners()));
      }
      programs.put(name, List.copyOf(statements));
      templates.put(name, template);
    }
    Map<SiteOwner, Boolean> retainedReads = new HashMap<>();
    Map<SiteOwner, Masks> readMasks = DistributedDependencyAnalysis.analyze(sites);
    readMasks.forEach(
        (site, masks) -> retainedReads.put(site, masks.required(isolations.get(site.owner()))));
    return new ClosedWorkload(
        fingerprint(schema, instances, isolations, placement, programs),
        templates,
        programs,
        retainedReads,
        readMasks);
  }

  public String fingerprint() {
    return fingerprint;
  }

  /**
   * Reviewable flags for every declared read site and allowed owner, including all-false results.
   */
  public Map<SiteOwner, Masks> readMasks() {
    return readMasks;
  }

  /**
   * Every caller receives its own mutable execution templates. The catalogue keeps its originals.
   */
  public Map<String, TransactionTemplate> templates() {
    Map<String, TransactionTemplate> result = new LinkedHashMap<>();
    templates.forEach((name, template) -> result.put(name, template.clone()));
    return Map.copyOf(result);
  }

  public void requireProgram(String name) throws SQLException {
    if (name == null || !programs.containsKey(name)) {
      throw new SQLException("Closed workload requires a declared program", "0A000");
    }
  }

  public void requireAccess(String program, int slot, int owner) throws SQLException {
    requireProgram(program);
    List<Statement> statements = programs.get(program);
    if (slot < 0 || slot >= statements.size() || !statements.get(slot).owners().contains(owner)) {
      throw new SQLException("Access is outside the declared program or owner domain", "0A000");
    }
  }

  public boolean retainRead(String program, int slot, int owner) {
    // Invalid/undeclared descriptors never provide a negative optimization result.
    return retainedReads.getOrDefault(new SiteOwner(program, slot, owner), true);
  }

  private static String fingerprint(
      SchemaInfo schema,
      Map<Integer, DBInstance> instances,
      Map<Integer, Integer> isolations,
      Map<String, List<Integer>> placement,
      Map<String, List<Statement>> programs)
      throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream output = new DataOutputStream(bytes)) {
      write(output, ANALYSIS_VERSION);
      output.writeBoolean(true);
      output.writeInt(instances.size());
      for (var entry : instances.entrySet()) {
        output.writeInt(entry.getKey());
        write(output, entry.getValue().getUrl());
        output.writeInt(isolations.get(entry.getKey()));
      }
      List<TableInfo> tables =
          schema.getTables().stream()
              .sorted(Comparator.comparing(table -> table.getTableName().toLowerCase(Locale.ROOT)))
              .toList();
      output.writeInt(tables.size());
      for (TableInfo table : tables) {
        write(output, table.getTableName().toLowerCase(Locale.ROOT));
        List<ColumnInfo> columns =
            table.getColumns().values().stream()
                .sorted(Comparator.comparing(column -> column.getName().toLowerCase(Locale.ROOT)))
                .toList();
        output.writeInt(columns.size());
        for (ColumnInfo column : columns) {
          write(output, column.getName().toLowerCase(Locale.ROOT));
          write(output, column.getType().name());
          output.writeBoolean(column.isPrimaryKey());
        }
      }
      output.writeInt(placement.size());
      for (var entry : placement.entrySet()) {
        write(output, entry.getKey());
        output.writeInt(entry.getValue().size());
        for (int owner : entry.getValue()) output.writeInt(owner);
      }
      output.writeInt(programs.size());
      for (var entry : programs.entrySet()) {
        write(output, entry.getKey());
        output.writeInt(entry.getValue().size());
        for (Statement statement : entry.getValue()) {
          output.writeInt(statement.slot());
          write(output, statement.table());
          output.writeBoolean(statement.write());
          write(output, statement.sql());
          output.writeInt(statement.owners().size());
          for (int owner : statement.owners().stream().sorted().toList()) output.writeInt(owner);
        }
      }
    }
    try {
      return HexFormat.of()
          .formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
    } catch (NoSuchAlgorithmException exception) {
      throw new IllegalStateException("SHA-256 is required by the Java runtime", exception);
    }
  }

  private static void write(DataOutputStream output, String value) throws IOException {
    if (value == null) {
      output.writeInt(-1);
      return;
    }
    byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
    output.writeInt(bytes.length);
    output.write(bytes);
  }

  private static String text(String value, String field) throws SQLException {
    if (value == null || value.isBlank()) throw invalid(field + " is required");
    return value.trim();
  }

  private static SQLException invalid(String message) {
    return new SQLException("Invalid closed workload: " + message, "22023");
  }

  /** Normalize TOML arrays into the existing XML declaration model before any semantic analysis. */
  private static Document readToml(Path path) throws IOException {
    TomlDocument input =
        new TomlMapper()
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .readValue(path.toFile(), TomlDocument.class);
    Document document = new Document();
    document.version = input.version;
    document.closedWorld = input.closedWorld;
    if (input.programs != null) {
      document.programs = new ArrayList<>(input.programs.size());
      for (TomlProgram declared : input.programs) {
        Program program = new Program();
        program.name = declared.name;
        if (declared.statements != null) {
          program.statements = new ArrayList<>(declared.statements.size());
          for (TomlSQL declaredSQL : declared.statements) {
            SQL statement = new SQL();
            statement.id = declaredSQL.id;
            statement.operation = declaredSQL.operation;
            statement.relation = declaredSQL.relation;
            statement.sql = declaredSQL.sql;
            if (declaredSQL.owners != null) {
              statement.owners = new Owners();
              statement.owners.values = declaredSQL.owners;
            }
            program.statements.add(statement);
          }
        }
        document.programs.add(program);
      }
    }
    return document;
  }

  public static class TomlDocument {
    public Integer version;

    @JsonProperty("closed_world")
    public Boolean closedWorld;

    public List<TomlProgram> programs;
  }

  public static class TomlProgram {
    public String name;
    public List<TomlSQL> statements;
  }

  public static class TomlSQL {
    public Integer id;
    public String operation;
    public String relation;
    public String sql;
    public List<Integer> owners;
  }

  @JacksonXmlRootElement(localName = "workload")
  public static class Document {
    @JacksonXmlProperty(isAttribute = true)
    public Integer version;

    @JacksonXmlProperty(isAttribute = true)
    public Boolean closedWorld;

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "program")
    public List<Program> programs;
  }

  public static class Program {
    @JacksonXmlProperty(isAttribute = true)
    public String name;

    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "statement")
    public List<SQL> statements;
  }

  public static class SQL {
    @JacksonXmlProperty(isAttribute = true)
    public Integer id;

    @JacksonXmlProperty(isAttribute = true)
    public String operation;

    @JacksonXmlProperty(isAttribute = true)
    public String relation;

    public String sql;
    public Owners owners;
  }

  public static class Owners {
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "owner")
    public List<Integer> values;
  }
}
