package org.dbiir.txnsails.execution.transaction;

import java.sql.SQLException;
import java.util.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.statement.update.Update;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.TemplateSQL;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.validation.ValidationItem;

/** Rejects statements whose complete dependency set cannot be represented by one row key. */
public final class DistributedSQL {
  private DistributedSQL() {}

  public static void validate(TemplateSQL template, SchemaInfo schema) throws SQLException {
    plan(template, schema);
  }

  public static Plan plan(TemplateSQL template, SchemaInfo schema) throws SQLException {
    Plan plan = template.getDistributedPlanCache().get(template, schema);
    template.applyDistributedRewrite(plan.executionSQL(), plan.resultColumns());
    return plan;
  }

  /** A template-owned one-entry cache shared by its clones; schema replacement invalidates it. */
  public static final class PlanCache {
    private volatile CachedPlan cached;

    private Plan get(TemplateSQL template, SchemaInfo schema) throws SQLException {
      CachedPlan current = cached;
      if (current == null || current.schema() != schema) {
        synchronized (this) {
          current = cached;
          if (current == null || current.schema() != schema) {
            try {
              current = new CachedPlan(schema, compile(template, schema), null, null);
            } catch (SQLException failure) {
              current = new CachedPlan(schema, null, failure.getMessage(), failure.getSQLState());
            }
            cached = current;
          }
        }
      }
      if (current.plan() == null)
        throw new SQLException(current.failureMessage(), current.failureState());
      return current.plan();
    }
  }

  private record CachedPlan(
      SchemaInfo schema, Plan plan, String failureMessage, String failureState) {}

  public record Plan(
      String table,
      List<String> primaryKeyColumns,
      List<Integer> parameterIndexes,
      String versionQuery,
      String executionSQL,
      List<String> resultColumns) {
    public Plan {
      primaryKeyColumns = List.copyOf(primaryKeyColumns);
      parameterIndexes = List.copyOf(parameterIndexes);
      resultColumns = List.copyOf(resultColumns);
    }

    public ValidationItem item(
        long placementKey, LockType type, long version, String[] args, int offset) {
      List<Long> values = new ArrayList<>(primaryKeyColumns.size());
      StringBuilder identity = new StringBuilder("{");
      for (int i = 0; i < primaryKeyColumns.size(); i++) {
        long value = Long.parseLong(args[offset + parameterIndexes.get(i)]);
        values.add(value);
        if (i > 0) identity.append(", ");
        identity.append(primaryKeyColumns.get(i)).append('=').append(value);
      }
      identity.append('}');
      return new ValidationItem(table, placementKey, type, version)
          .withPrimaryKey(identity.toString(), versionQuery, values);
    }
  }

  private static Plan compile(TemplateSQL template, SchemaInfo schema) throws SQLException {
    try {
      var statement = CCJSqlParserUtil.parse(template.getOriginSQL());
      Set<String> primaryKey = new HashSet<>();
      var columns = schema.getColumnInfoByTableName(template.getTable());
      if (columns == null) throw unsupported();
      columns.stream()
          .filter(column -> column.isPrimaryKey())
          .forEach(column -> primaryKey.add(column.getName().toUpperCase(Locale.ROOT)));
      if (primaryKey.isEmpty()) throw unsupported();
      Expression where;
      Table table;
      if (template.getOp() == 0
          && statement instanceof Select select
          && select.getSelectBody() instanceof PlainSelect body) {
        if (select.getWithItemsList() != null
            || body.getJoins() != null
            || body.getDistinct() != null
            || body.getGroupBy() != null
            || body.getHaving() != null
            || body.getLimit() != null
            || body.getOffset() != null
            || body.getFetch() != null
            || body.getTop() != null
            || body.getIntoTables() != null
            || body.getOrderByElements() != null
            || body.isForUpdate()
            || !(body.getFromItem() instanceof Table)) throw unsupported();
        table = (Table) body.getFromItem();
        checkColumns(body.getSelectItems());
        where = body.getWhere();
      } else if (template.getOp() == 1 && statement instanceof Update update) {
        if (update.getWithItemsList() != null
            || update.getFromItem() != null
            || update.getJoins() != null
            || (update.getStartJoins() != null && !update.getStartJoins().isEmpty())
            || update.getOrderByElements() != null
            || update.getSelect() != null
            || update.getLimit() != null) throw unsupported();
        table = update.getTable();
        for (var assignment : update.getUpdateSets()) {
          for (Column column : assignment.getColumns()) {
            if (primaryKey.contains(column.getColumnName().toUpperCase(Locale.ROOT))
                || column.getColumnName().equalsIgnoreCase("vid")) throw unsupported();
          }
          for (Expression expression : assignment.getExpressions()) checkValue(expression);
        }
        checkColumns(update.getReturningExpressionList());
        where = update.getWhere();
      } else throw unsupported();
      if (!table.getName().equalsIgnoreCase(template.getTable())
          || table.getSchemaName() != null
          || (table.getDatabase() != null && table.getDatabase().getDatabaseName() != null))
        throw unsupported();
      Set<String> predicates = new HashSet<>();
      checkPredicate(where, predicates);
      if (!predicates.equals(primaryKey)) throw unsupported();
      Map<String, Integer> indexes = new TreeMap<>();
      for (var condition : template.getWherePlaceholders()) {
        indexes.put(
            condition.getColumnName().toLowerCase(Locale.ROOT), condition.getPlaceholderIndex());
      }
      if (indexes.size() != primaryKey.size()) throw unsupported();
      String versionQuery =
          "SELECT vid FROM "
              + template.getTable()
              + " WHERE "
              + indexes.keySet().stream()
                  .map(column -> column + " = ?")
                  .collect(java.util.stream.Collectors.joining(" AND "));
      template.setNeedRewriteUnderRC();
      return new Plan(
          template.getTable(),
          new ArrayList<>(indexes.keySet()),
          new ArrayList<>(indexes.values()),
          versionQuery,
          template.getSQL(),
          template.getColumnList().stream().map(Column::getColumnName).toList());
    } catch (SQLException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new SQLException("Unsupported distributed SQL", "0A000", ex);
    }
  }

  private static void checkColumns(List<SelectItem> items) throws SQLException {
    if (items == null) return;
    for (SelectItem item : items) {
      if (!(item instanceof AllColumns)
          && !(item instanceof SelectExpressionItem expression
              && expression.getExpression() instanceof Column
              && expression.getAlias() == null)) {
        throw unsupported();
      }
    }
  }

  private static void checkPredicate(Expression expression, Set<String> columns)
      throws SQLException {
    if (expression instanceof Parenthesis parenthesis) {
      checkPredicate(parenthesis.getExpression(), columns);
    } else if (expression instanceof AndExpression and) {
      checkPredicate(and.getLeftExpression(), columns);
      checkPredicate(and.getRightExpression(), columns);
    } else if (expression instanceof EqualsTo equal) {
      Column column;
      if (equal.getLeftExpression() instanceof Column left
          && equal.getRightExpression() instanceof JdbcParameter) column = left;
      else if (equal.getRightExpression() instanceof Column right
          && equal.getLeftExpression() instanceof JdbcParameter) column = right;
      else throw unsupported();
      if (!columns.add(column.getColumnName().toUpperCase(Locale.ROOT))) throw unsupported();
    } else throw unsupported();
  }

  private static void checkValue(Expression expression) throws SQLException {
    if (expression instanceof Column
        || expression instanceof JdbcParameter
        || expression instanceof LongValue
        || expression instanceof DoubleValue
        || expression instanceof StringValue
        || expression instanceof NullValue) return;
    if (expression instanceof Parenthesis parenthesis) {
      checkValue(parenthesis.getExpression());
    } else if (expression instanceof Addition
        || expression instanceof Subtraction
        || expression instanceof Multiplication
        || expression instanceof Division) {
      BinaryExpression binary = (BinaryExpression) expression;
      checkValue(binary.getLeftExpression());
      checkValue(binary.getRightExpression());
    } else throw unsupported();
  }

  private static SQLException unsupported() {
    return new SQLException(
        "Distributed mode supports existing-row primary-key point SELECT/UPDATE only", "0A000");
  }
}
