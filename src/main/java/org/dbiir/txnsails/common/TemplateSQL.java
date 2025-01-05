package org.dbiir.txnsails.common;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.*;
import org.dbiir.txnsails.common.constants.YCSBConstants;

@Getter
public class TemplateSQL implements Cloneable {
  private final int op; // 0 for read, 1 for write, 2 for scan
  private final String originSQL;
  private String rewriteSQL;
  private final String relation;
  @Setter private boolean skip;
  private boolean needRewriteUnderSI;
  private boolean needRewriteUnderRC;
  @Setter private List<Integer> uniqueKeyIndexList; // calculate
  private int uniqueKeyNumber;
  @Setter @Getter private List<Column> columnList;
  @Getter private List<ConditionInfo> wherePlaceholders;

  public TemplateSQL(int op, String relation, String sql) {
    this.op = op;
    this.originSQL = sql;
    this.relation = relation;
    this.columnList = null;
    this.needRewriteUnderSI = false;
    this.needRewriteUnderRC = false;
    this.skip = false;
    this.uniqueKeyIndexList = new ArrayList<>(4);
    this.rewriteSQL = "";
    this.wherePlaceholders = new ArrayList<>();

    // find the placeholders in the where clause
    findWherePlaceholders();
  }

  public void addUniqueKeyIndex(int idx) {
    this.uniqueKeyIndexList.add(idx);
    this.uniqueKeyNumber++;
  }

  public void setNeedRewriteUnderRC() {
    this.needRewriteUnderRC = true;
    rewrite();
  }

  public void setNeedRewriteUnderSI() {
    this.needRewriteUnderSI = true;
    rewrite();
  }

  private void rewrite() {
    if (identifySQLType(originSQL) == 1) {
      this.rewriteSQL = modifyUpdateQuery(originSQL, columnList);
    } else if (identifySQLType(originSQL) == 0) {
      this.rewriteSQL = modifySelectQuery(originSQL, columnList);
    } else {
      this.rewriteSQL = "";
    }
  }

  public int getUniqueKeyIdx(int idx) {
    return this.uniqueKeyIndexList.get(idx);
  }

  public String getSQL() {
    if (needRewriteUnderRC || needRewriteUnderSI) {
      return rewriteSQL;
    }
    return originSQL;
  }

  private String getSQLUnderRC() {
    if (needRewriteUnderRC) {
      return rewriteSQL;
    } else {
      return originSQL;
    }
  }

  private String getSQLUnderSI() {
    if (needRewriteUnderSI) {
      return rewriteSQL;
    } else {
      return originSQL;
    }
  }

  // Method: Add the field "vid" to the SELECT statement and ensure it is added at the end
  private String modifySelectQuery(String sql, List<Column> columnList) {
    try {
      // Parse the original SELECT statement
      CCJSqlParserManager parserManager = new CCJSqlParserManager();
      Select selectStatement = (Select) parserManager.parse(new StringReader(sql));

      // Get the SELECT body, which is the part containing the fields
      PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();

      // Get the list of SELECT items (fields)
      List<SelectItem> selectItems = plainSelect.getSelectItems();

      // Create a new column "vid"
      Column vidColumn = new Column("vid");

      // Create a new SELECT expression item (field)
      SelectExpressionItem vidSelectItem = new SelectExpressionItem(vidColumn);

      // Add the "vid" field to the SELECT clause
      selectItems.add(vidSelectItem);
      columnList = convertSelectItemsToColumns(selectItems);
      //          Arrays.stream(selectItems.toArray())
      //              .filter(obj -> obj instanceof String)
      //              .map(obj -> (String) obj)
      //              .collect(Collectors.toList());
      // Return the modified SQL statement
      return selectStatement.toString();

    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  private List<Column> convertSelectItemsToColumns(List<SelectItem> selectItems) {
    List<Column> columns = new ArrayList<>();
    for (SelectItem selectItem : selectItems) {
      if (selectItem instanceof SelectExpressionItem) {
        SelectExpressionItem selectExpressionItem = (SelectExpressionItem) selectItem;
        if (selectExpressionItem.getExpression() instanceof Column) {
          columns.add((Column) selectExpressionItem.getExpression());
        }
      }
    }
    return columns;
  }

  // Method: Modify the SQL statement to add the "vid" field and return it
  private String modifyUpdateQuery(String sql, List<Column> columnList) {
    try {
      // Parse the SQL statement
      CCJSqlParserManager parserManager = new CCJSqlParserManager();
      Update updateStatement = (Update) parserManager.parse(new StringReader(sql));

      // Create the expression "vid = vid + 1"
      Column vidColumn = new Column("vid");
      LongValue oneValue = new LongValue(1); // Represents the constant 1
      Addition addExpression = new Addition(); // Addition represents the "vid + 1" operation
      addExpression.setLeftExpression(vidColumn); // Left side is the "vid" column
      addExpression.setRightExpression(oneValue); // Right side is the constant value 1
      EqualsTo versionExpression = new EqualsTo(vidColumn, addExpression);

      // Add the SetExpression to the UPDATE statement
      updateStatement
          .getExpressions()
          .add(versionExpression); // Add "vid = vid + 1" to the SET clause

      // Manually add the RETURNING clause, since JSQLParser does not support it directly
      String returningClause = " RETURNING vid";
      columnList.add(new Column("vid"));

      // Return the modified SQL statement
      return updateStatement.toString() + returningClause;

    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  // Method: Identify the type of SQL statement (SELECT, UPDATE, etc.)
  // 0 for SELECT, 1 for UPDATE
  private int identifySQLType(String sql) {
    try {
      // Create the SQL parser
      CCJSqlParserManager parserManager = new CCJSqlParserManager();

      // Parse the SQL statement
      Statement statement = parserManager.parse(new StringReader(sql));

      // Check the type of SQL statement
      if (statement instanceof Select) {
        return 0;
      } else if (statement instanceof Update) {
        return 1;
      } else {
        return -1;
      }

    } catch (Exception e) {
      e.printStackTrace();
      return -1;
    }
  }

  private void findWherePlaceholders() {
    try {
      analyseTemplate();
      Statement statement = CCJSqlParserUtil.parse(originSQL);
      if (statement instanceof Update) {
        Update updateStatement = (Update) statement;
        Expression where = updateStatement.getWhere();
        findConditions(where);
      } else if (statement instanceof Select) {
        Select selectStatement = (Select) statement;
        PlainSelect plainSelect = (PlainSelect) selectStatement.getSelectBody();
        Expression where = plainSelect.getWhere();
        findConditions(where);
      }
    } catch (JSQLParserException e) {
      e.printStackTrace();
    }
  }

  private void findConditions(Expression expression) {
    if (expression instanceof AndExpression) {
      AndExpression andExpression = (AndExpression) expression;
      findConditions(andExpression.getLeftExpression());
      findConditions(andExpression.getRightExpression());
    } else if (expression instanceof OrExpression) {
      OrExpression orExpression = (OrExpression) expression;
      findConditions(orExpression.getLeftExpression());
      findConditions(orExpression.getRightExpression());
    } else if (expression instanceof EqualsTo
        || expression instanceof GreaterThan
        || expression instanceof MinorThan
        || expression instanceof GreaterThanEquals
        || expression instanceof MinorThanEquals) {
      BinaryExpression binaryExpression = (BinaryExpression) expression;
      Column column = (Column) binaryExpression.getLeftExpression();
      Expression rightExpression = binaryExpression.getRightExpression();
      String value = null;
      int position = -1;
      if (rightExpression instanceof JdbcParameter) {
        System.out.println(
            column.getColumnName() + ": " + ((JdbcParameter) rightExpression).getIndex());
        this.wherePlaceholders.add(new ConditionInfo(column, (JdbcParameter) rightExpression));
      } else {
        System.out.println(
            "[No placeholder]: " + column.getColumnName() + ", " + (StringValue) rightExpression);
      }
    }
  }

  // specific for YCSB workload
  private void analyseTemplate() {
    if (this.relation.equals(YCSBConstants.TABLE_NAME)) {
      this.needRewriteUnderRC = true;
      this.needRewriteUnderSI = true;
    }
  }

  @Override
  public TemplateSQL clone() {
    try {
      TemplateSQL clone = (TemplateSQL) super.clone();
      // TODO: copy mutable state here, so the clone can't change the internals of the original
      return clone;
    } catch (CloneNotSupportedException e) {
      throw new AssertionError();
    }
  }
}
