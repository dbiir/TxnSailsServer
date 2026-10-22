package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.List;
import net.sf.jsqlparser.schema.Column;
import org.dbiir.txnsails.analysis.ColumnInfo;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.TemplateSQL;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.transaction.DistributedSQL;
import org.dbiir.txnsails.worker.MetaWorker;
import org.junit.jupiter.api.Test;

class DistributedPlanTest {
  @Test
  void eligibilityAndRewriteAreSharedByTemplateClones() throws Exception {
    CountingSchema schema = new CountingSchema(true);
    TemplateSQL template = template(schema, "SELECT value FROM items WHERE b = ? AND a = ?");
    TemplateSQL clone = template.clone();
    DistributedSQL.validate(template, schema);
    DistributedSQL.validate(clone, schema);
    assertEquals(1, schema.lookups);
    assertTrue(clone.getSQL().contains("vid"));
    assertSame(template.getSQL(), clone.getSQL());
    assertEquals(
        List.of("value", "vid"),
        clone.getColumnList().stream().map(Column::getColumnName).toList());
  }

  @Test
  void rejectedEligibilityIsCachedWithoutSharingMutableExceptions() {
    CountingSchema schema = new CountingSchema(true);
    TemplateSQL template = template(schema, "SELECT value FROM items WHERE a = ?");
    SQLException first =
        assertThrows(SQLException.class, () -> DistributedSQL.validate(template, schema));
    SQLException second =
        assertThrows(SQLException.class, () -> DistributedSQL.validate(template.clone(), schema));
    assertEquals("0A000", second.getSQLState());
    assertNotSame(first, second);
    assertEquals(1, schema.lookups);
  }

  @Test
  void cachedEligibilityIsInvalidatedForDifferentSchemaIdentity() throws Exception {
    CountingSchema original = new CountingSchema(true);
    TemplateSQL template = template(original, "SELECT value FROM items WHERE b = ? AND a = ?");
    DistributedSQL.validate(template, original);
    CountingSchema changed = new CountingSchema(false);
    assertEquals(
        "0A000",
        assertThrows(SQLException.class, () -> DistributedSQL.validate(template.clone(), changed))
            .getSQLState());
    assertEquals(1, original.lookups);
    assertEquals(1, changed.lookups);
  }

  @Test
  void compiledPrimaryKeyUsesCanonicalOrderAndOriginalParameterPositions() throws Exception {
    CountingSchema schema = new CountingSchema(true);
    MetaWorker.getINSTANCE().setSchema(schema);
    TemplateSQL template =
        new TemplateSQL(1, "items", "UPDATE items SET value = ? WHERE b = ? AND a = ?");
    var plan = DistributedSQL.plan(template, schema);
    assertEquals(List.of("a", "b"), plan.primaryKeyColumns());
    assertEquals(List.of(2, 1), plan.parameterIndexes());
    assertEquals("SELECT vid FROM items WHERE a = ? AND b = ?", plan.versionQuery());
    var item = plan.item(7, LockType.EX, 4, new String[] {"skip", "new value", "20", "10"}, 1);
    assertEquals("{a=10, b=20}", item.getIdentity());
    assertEquals(7, item.getKey());
    assertEquals(4, item.getWriteVersion());
    assertSame(plan, DistributedSQL.plan(template.clone(), schema));
    assertThrows(UnsupportedOperationException.class, () -> plan.primaryKeyColumns().add("other"));
  }

  private TemplateSQL template(CountingSchema schema, String sql) {
    MetaWorker.getINSTANCE().setSchema(schema);
    return new TemplateSQL(0, "items", sql);
  }

  private static class CountingSchema extends SchemaInfo {
    private final List<ColumnInfo> columns;
    private int lookups;

    CountingSchema(boolean compositeKey) {
      columns =
          List.of(
              new ColumnInfo("b", "BIGINT", compositeKey),
              new ColumnInfo("a", "BIGINT", true),
              new ColumnInfo("value", "TEXT", false),
              new ColumnInfo("vid", "BIGINT", false));
    }

    @Override
    public List<ColumnInfo> getColumnInfoByTableName(String table) {
      lookups++;
      return columns;
    }

    @Override
    public boolean isPrimaryKey(String table, Column column) {
      return columns.stream()
          .anyMatch(info -> info.isPrimaryKey() && info.getName().equals(column.getColumnName()));
    }
  }
}
