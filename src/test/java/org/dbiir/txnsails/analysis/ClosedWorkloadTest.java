package org.dbiir.txnsails.analysis;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.dbiir.txnsails.common.DBInstance;
import org.dbiir.txnsails.common.TransactionTemplate;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.worker.MetaWorker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ClosedWorkloadTest {
  @TempDir Path directory;

  @Test
  void loadsImmutableProgramsAndEnforcesSlotsAndOwnerDomains() throws Exception {
    ClosedWorkload catalogue =
        load(
            program("read", 0, "read", "<owners><owner>0</owner></owners>"),
            configuration("RC", "SI"));
    catalogue.requireProgram("read");
    catalogue.requireAccess("read", 0, 0);
    assertThrows(SQLException.class, () -> catalogue.requireProgram("unknown"));
    assertThrows(SQLException.class, () -> catalogue.requireAccess("read", -1, 0));
    assertThrows(SQLException.class, () -> catalogue.requireAccess("read", 1, 0));
    assertThrows(SQLException.class, () -> catalogue.requireAccess("read", 0, 1));
    Map<String, TransactionTemplate> first = catalogue.templates();
    assertThrows(UnsupportedOperationException.class, () -> first.clear());
    first.get("read").setSkipIndex(0, true);
    first.get("read").getSQLTemplateByIndex(0).getColumnList().clear();
    TransactionTemplate second = catalogue.templates().get("read");
    assertFalse(second.shouldSkipIndex(0));
    assertFalse(second.getSQLTemplateByIndex(0).getColumnList().isEmpty());
    assertThrows(UnsupportedOperationException.class, () -> catalogue.readMasks().clear());
  }

  @Test
  void resolvesLocalIsolationAndStaticReadMasksAtLoadTime() throws Exception {
    String owner0 = "<owners><owner>0</owner></owners>";
    String programs = program("reader", 0, "read", owner0) + program("writer", 0, "write", owner0);
    WorkloadConfiguration rc = configuration("RC", "SI");
    ClosedWorkload local = load(programs, rc);
    assertEquals(
        new DistributedDependencyAnalysis.Masks(false, true, false),
        local.readMasks().get(new DistributedDependencyAnalysis.SiteOwner("reader", 0, 0)));
    assertTrue(local.retainRead("reader", 0, 0));
    rc.getInstances().get(0).setIsolation("SER");
    assertTrue(local.retainRead("reader", 0, 0));
    assertFalse(load(programs, configuration("SER", "SI")).retainRead("reader", 0, 0));
    assertFalse(
        load(program("reader", 0, "read", owner0), configuration("RC", "SI"))
            .retainRead("reader", 0, 0));
    ClosedWorkload global =
        load(
            program("reader", 0, "read", "") + program("writer", 0, "write", ""),
            configuration("SER", "SI"));
    assertTrue(global.retainRead("reader", 0, 0));
    global.requireAccess("reader", 0, 1);
  }

  @Test
  void fingerprintIsCanonicalAndExcludesCredentialsAndLocalCoordinator() throws Exception {
    String read = program("reader", 0, "read", "");
    String write = program("writer", 0, "write", "");
    WorkloadConfiguration first = configuration("RC", "SI");
    String expected = load(read + write, first).fingerprint();
    WorkloadConfiguration reordered = configuration("RC", "SI");
    reordered.setInstanceID(1);
    reordered.getInstances().get(0).setUsername("other");
    reordered.getInstances().get(0).setPassword("secret");
    java.util.Collections.reverse(reordered.getInstances());
    assertEquals(expected, load(write + read, reordered).fingerprint());
    assertEquals(64, expected.length());
    first.getTableName2DbList().put("usertable", List.of(1, 0));
    assertNotEquals(expected, load(read + write, first).fingerprint());
    assertNotEquals(expected, load(read + write, configuration("SER", "SI")).fingerprint());
    WorkloadConfiguration differentDatabase = configuration("RC", "SI");
    differentDatabase.getInstances().get(1).setUrl("jdbc:postgresql://localhost/other");
    assertNotEquals(expected, load(read + write, differentDatabase).fingerprint());
    assertNotEquals(expected, load(read, configuration("RC", "SI")).fingerprint());
  }

  @Test
  void fingerprintIncludesUnreferencedSchemaTablesAndExactColumnTypes() throws Exception {
    String programs = program("reader", 0, "read", "");
    WorkloadConfiguration configuration = configuration("RC", "SI");
    String original = load(programs, configuration).fingerprint();
    SchemaInfo extraTable = schema("CREATE TABLE unused (id INT PRIMARY KEY, value TEXT);");
    assertNotEquals(original, load(programs, extraTable, configuration).fingerprint());
    SchemaInfo differentType = schema("", "TEXT");
    assertNotEquals(original, load(programs, differentType, configuration).fingerprint());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "<owners/>",
        "<owners><owner>2</owner></owners>",
        "<owners><owner>0</owner><owner>0</owner></owners>"
      })
  void rejectsMissingProgramsAndInvalidExplicitOwnerLists(String owners) throws Exception {
    String programs = owners.isEmpty() ? "" : program("reader", 0, "read", owners);
    assertThrows(SQLException.class, () -> load(programs, configuration("RC", "SI")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"scan", "delete", "", "WRITE READ"})
  void rejectsUnknownOperationNames(String operation) throws Exception {
    assertThrows(
        SQLException.class, () -> load(program("p", 0, operation, ""), configuration("RC", "SI")));
  }

  @Test
  void rejectsDuplicateProgramsAndNoncontiguousOrDuplicateSlots() throws Exception {
    String declared = program("reader", 0, "read", "");
    assertThrows(SQLException.class, () -> load(declared + declared, configuration("RC", "SI")));
    assertThrows(
        SQLException.class,
        () -> load(program("reader", 1, "read", ""), configuration("RC", "SI")));
    String statement =
        "<statement id=\"0\" operation=\"read\" relation=\"usertable\"><sql>SELECT field1 FROM"
            + " usertable WHERE ycsb_key = ?</sql></statement>";
    assertThrows(
        SQLException.class,
        () ->
            load(
                "<program name=\"reader\">" + statement + statement + "</program>",
                configuration("RC", "SI")));
  }

  @Test
  void rejectsNonclosedAndUnsupportedVersionDeclarations() throws Exception {
    String programs = program("reader", 0, "read", "");
    assertThrows(
        SQLException.class,
        () ->
            loadXml(
                "<workload version=\"1\" closedWorld=\"false\">" + programs + "</workload>",
                schema(""),
                configuration("RC", "SI")));
    assertThrows(
        SQLException.class,
        () ->
            loadXml(
                "<workload version=\"2\" closedWorld=\"true\">" + programs + "</workload>",
                schema(""),
                configuration("RC", "SI")));
  }

  @Test
  void rejectsSqlOutsideTheSupportedPointProtocol() throws Exception {
    String programs = program("reader", 0, "read", "").replace("ycsb_key = ?", "ycsb_key > ?");
    assertEquals(
        "0A000",
        assertThrows(SQLException.class, () -> load(programs, configuration("RC", "SI")))
            .getSQLState());
  }

  @Test
  void closedInstallationPreventsRegistrationAndLegacyReanalysis() throws Exception {
    ClosedWorkload catalogue = load(program("reader", 0, "read", ""), configuration("RC", "SI"));
    MetaWorker worker = new MetaWorker();
    worker.installClosedWorkload(catalogue);
    HashMap<String, TransactionTemplate> templates = new HashMap<>();
    worker.cloneTemplatesAfterAnalysis(templates);
    assertEquals(1, templates.size());
    assertThrows(IllegalStateException.class, () -> worker.registerTemplateName("extra"));
    assertThrows(
        IllegalStateException.class,
        () ->
            worker.registerTemplateSQL(
                "reader", 0, "usertable", "SELECT field1 FROM usertable WHERE ycsb_key = ?"));
    assertThrows(IllegalStateException.class, worker::analysisWorkload);
  }

  @Test
  void equivalentXmlAndTomlShareFingerprintMasksAndOwnerContracts() throws Exception {
    SchemaInfo schema = schema("");
    WorkloadConfiguration configuration = configuration("RC", "SI");
    ClosedWorkload xml =
        load(
            program("reader", 0, "read", "<owners><owner>0</owner></owners>")
                + program("writer", 0, "write", ""),
            schema,
            configuration);
    ClosedWorkload toml =
        loadToml(
            toml("reader", "read", "owners = [0]")
                + """

[[programs]]
name = 'writer'
[[programs.statements]]
id = 0
operation = 'write'
relation = 'usertable'
sql = 'UPDATE usertable SET field1 = ? WHERE ycsb_key = ?'
""",
            schema,
            configuration);
    assertEquals(xml.fingerprint(), toml.fingerprint());
    assertEquals(xml.readMasks(), toml.readMasks());
    assertEquals(
        xml.templates().get("writer").getSQLTemplateByIndex(0).getOriginSQL(),
        toml.templates().get("writer").getSQLTemplateByIndex(0).getOriginSQL());
    toml.requireAccess("reader", 0, 0);
    assertThrows(SQLException.class, () -> toml.requireAccess("reader", 0, 1));
    toml.requireAccess("writer", 0, 1);
  }

  @Test
  void omittedAndReorderedTomlOwnerArraysNormalizeLikeXml() throws Exception {
    SchemaInfo schema = schema("");
    WorkloadConfiguration configuration = configuration("RC", "SI");
    ClosedWorkload xml = load(program("reader", 0, "read", ""), schema, configuration);
    ClosedWorkload omitted = loadToml(toml("reader", "read", ""), schema, configuration);
    ClosedWorkload reordered =
        loadToml(toml("reader", "read", "owners = [1, 0]"), schema, configuration);
    assertEquals(xml.fingerprint(), omitted.fingerprint());
    assertEquals(xml.fingerprint(), reordered.fingerprint());
    omitted.requireAccess("reader", 0, 0);
    omitted.requireAccess("reader", 0, 1);
  }

  @Test
  void tomlSuffixIsCaseInsensitiveAndSupportsMultilineSql() throws Exception {
    SchemaInfo schema = schema("");
    WorkloadConfiguration configuration = configuration("RC", "SI");
    String input =
        toml("reader", "read", "")
            .replace(
                "sql = 'SELECT field1 FROM usertable WHERE ycsb_key = ?'",
                "sql = '''\nSELECT field1 FROM usertable WHERE ycsb_key = ?\n'''");
    Path file = directory.resolve("catalogue.TOML");
    Files.writeString(file, input);
    assertEquals(
        load(program("reader", 0, "read", ""), schema, configuration).fingerprint(),
        ClosedWorkload.load(file, schema, configuration).fingerprint());
  }

  @ParameterizedTest
  @ValueSource(strings = {"root", "program", "statement"})
  void tomlRejectsUnknownFieldsAtEveryDeclarationLevel(String level) throws Exception {
    String input = toml("reader", "read", "");
    input =
        switch (level) {
          case "root" -> input.replace("closed_world = true", "closed_world = true\nunknown = 1");
          case "program" -> input.replace("name = 'reader'", "name = 'reader'\nunknown = 1");
          default -> input + "\nunknown = 1\n";
        };
    String invalid = input;
    assertThrows(IOException.class, () -> loadToml(invalid, schema(""), configuration("RC", "SI")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "[0, 0]", "[2]"})
  void tomlUsesTheSameOwnerDomainValidationAsXml(String owners) throws Exception {
    assertEquals(
        "22023",
        assertThrows(
                SQLException.class,
                () ->
                    loadToml(
                        toml("reader", "read", "owners = " + owners),
                        schema(""),
                        configuration("RC", "SI")))
            .getSQLState());
  }

  @Test
  void tomlRequiresTheClosedWorldVersionContract() throws Exception {
    String declaration = toml("reader", "read", "");
    for (String invalid :
        List.of(
            declaration.replace("closed_world = true", "closed_world = false"),
            declaration.replace("version = 1", "version = 2"),
            declaration.replace("version = 1", ""))) {
      assertEquals(
          "22023",
          assertThrows(
                  SQLException.class,
                  () -> loadToml(invalid, schema(""), configuration("RC", "SI")))
              .getSQLState());
    }
  }

  private String toml(String name, String operation, String extraStatementFields) {
    String sql =
        operation.equals("write")
            ? "UPDATE usertable SET field1 = ? WHERE ycsb_key = ?"
            : "SELECT field1 FROM usertable WHERE ycsb_key = ?";
    return """
           version = 1
           closed_world = true
           [[programs]]
           name = '%s'
           [[programs.statements]]
           id = 0
           operation = '%s'
           relation = 'usertable'
           sql = '%s'
           %s
           """
        .formatted(name, operation, sql, extraStatementFields);
  }

  private ClosedWorkload loadToml(
      String toml, SchemaInfo schema, WorkloadConfiguration configuration) throws Exception {
    Path file = Files.createTempFile(directory, "workload", ".toml");
    Files.writeString(file, toml);
    return ClosedWorkload.load(file, schema, configuration);
  }

  private ClosedWorkload load(String programs, WorkloadConfiguration configuration)
      throws Exception {
    return load(programs, schema(""), configuration);
  }

  private ClosedWorkload load(
      String programs, SchemaInfo schema, WorkloadConfiguration configuration) throws Exception {
    return loadXml(
        "<workload version=\"1\" closedWorld=\"true\">" + programs + "</workload>",
        schema,
        configuration);
  }

  private ClosedWorkload loadXml(String xml, SchemaInfo schema, WorkloadConfiguration configuration)
      throws Exception {
    Path file = Files.createTempFile(directory, "workload", ".xml");
    Files.writeString(file, xml);
    return ClosedWorkload.load(file, schema, configuration);
  }

  private String program(String name, int slot, String operation, String owners) {
    String sql =
        operation.equals("write")
            ? "UPDATE usertable SET field1 = ? WHERE ycsb_key = ?"
            : "SELECT field1 FROM usertable WHERE ycsb_key = ?";
    return "<program name=\""
        + name
        + "\"><statement id=\""
        + slot
        + "\" operation=\""
        + operation
        + "\" relation=\"usertable\"><sql>"
        + sql
        + "</sql>"
        + owners
        + "</statement></program>";
  }

  private SchemaInfo schema(String extra) throws Exception {
    return schema(extra, "VARCHAR(100)");
  }

  private SchemaInfo schema(String extra, String fieldType) throws Exception {
    Path file = Files.createTempFile(directory, "schema", ".sql");
    Files.writeString(
        file,
        "CREATE TABLE usertable (ycsb_key INT PRIMARY KEY, vid BIGINT, field1 "
            + fieldType
            + ");"
            + extra);
    SchemaInfo schema = new SchemaInfo(file.toString());
    MetaWorker.getINSTANCE().setSchema(schema);
    return schema;
  }

  private WorkloadConfiguration configuration(String firstIsolation, String secondIsolation) {
    WorkloadConfiguration result = new WorkloadConfiguration();
    DBInstance first = new DBInstance(0, "jdbc:postgresql://localhost/a", "localhost", "", "");
    first.setIsolation(firstIsolation);
    DBInstance second = new DBInstance(1, "jdbc:postgresql://localhost/b", "localhost", "", "");
    second.setIsolation(secondIsolation);
    result.setInstances(new ArrayList<>(List.of(first, second)));
    result.setTableName2DbList(new HashMap<>(Map.of("usertable", List.of(0, 1))));
    return result;
  }
}
