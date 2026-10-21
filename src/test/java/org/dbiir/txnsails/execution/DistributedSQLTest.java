package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.TemplateSQL;
import org.dbiir.txnsails.execution.transaction.DistributedSQL;
import org.dbiir.txnsails.worker.MetaWorker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class DistributedSQLTest {
  private static SchemaInfo schema;

  @BeforeAll
  static void schema() {
    schema = new SchemaInfo("config/ycsb.sql");
    MetaWorker.getINSTANCE().setSchema(schema);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "SELECT field1 FROM usertable WHERE ycsb_key = ? ORDER BY (SELECT field1 FROM usertable WHERE ycsb_key = 1)",
        "SELECT field1 FROM other_schema.usertable WHERE ycsb_key = ?",
        "SELECT field1 FROM usertable WHERE ycsb_key = ? AND field1 = 'x'",
        "SELECT field1 FROM usertable WHERE ycsb_key = ? OR ycsb_key = ?",
        "SELECT count(*) FROM usertable WHERE ycsb_key = ?"
      })
  void rejectsUnmodeledReadDependencies(String sql) {
    assertEquals(
        "0A000",
        assertThrows(
                SQLException.class,
                () -> DistributedSQL.validate(new TemplateSQL(0, "usertable", sql), schema))
            .getSQLState());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "UPDATE usertable SET ycsb_key = ? WHERE ycsb_key = ?",
        "UPDATE usertable SET vid = ? WHERE ycsb_key = ?",
        "UPDATE usertable SET field1 = (SELECT field1 FROM usertable WHERE ycsb_key = 1) WHERE ycsb_key = ?"
      })
  void rejectsUntrackedWrites(String sql) {
    assertThrows(
        SQLException.class,
        () -> DistributedSQL.validate(new TemplateSQL(1, "usertable", sql), schema));
  }

  @Test
  void acceptsPointUpdate() throws Exception {
    DistributedSQL.validate(
        new TemplateSQL(1, "usertable", "UPDATE usertable SET field1 = ? WHERE ycsb_key = ?"),
        schema);
  }
}
