package org.dbiir.txnsails.common;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.*;

@Getter
public class TemplateSQL implements Cloneable {
  private final int op; // 0 for read, 1 for write, 2 for scan
  private final String sql;
  private String sql_rewrite;
  private final String relation;
  @Setter private boolean skip;
  @Setter private boolean needRewriteUnderSI;
  @Setter private boolean needRewriteUnderRC;
  @Setter private List<Integer> uniqueKeyIndexList; // calculate
  private int uniqueKeyNumber;
  @Setter @Getter private List<String> columnList;

  public TemplateSQL(int op, String relation, String sql) {
    this.op = op;
    this.sql = sql;
    this.sql_rewrite = "";
    this.relation = relation;
    this.columnList = null;
    this.needRewriteUnderSI = false;
    this.needRewriteUnderRC = false;
    this.skip = false;
    this.uniqueKeyIndexList = new ArrayList<>(4);
  }

  public void addUniqueKeyIndex(int idx) {
    this.uniqueKeyIndexList.add(idx);
    this.uniqueKeyNumber++;
  }

  public int getUniqueKeyIdx(int idx) {
    return this.uniqueKeyIndexList.get(idx);
  }

  public String getSQL(CCType ccType) {
    if (ccType == CCType.RC_TAILOR) {
      return getSQLUnderRC();
    } else if (ccType == CCType.SI_TAILOR || ccType == CCType.SER_TRANSITION) {
      return getSQLUnderSI();
    }
    return "";
  }

  public String getSQL() {
    if (needRewriteUnderRC || needRewriteUnderSI) {
      if (identifySQLType(sql) == 1) {
        sql_rewrite = modifyUpdateQuery(sql, columnList);
      } else if (identifySQLType(sql) == 0) {
        sql_rewrite = modifySelectQuery(sql, columnList);
      } else {
        sql_rewrite = "";
      }
    }
    return sql_rewrite;
  }

  private String getSQLUnderRC() {
    if (needRewriteUnderRC) {
      if (identifySQLType(sql) == 1) {
        sql_rewrite = modifyUpdateQuery(sql, columnList);
      } else if (identifySQLType(sql) == 0) {
        sql_rewrite = modifySelectQuery(sql, columnList);
      }
      return sql_rewrite;
    } else {
      return sql;
    }
  }

  private String getSQLUnderSI() {
    if (needRewriteUnderSI) {
      if (identifySQLType(sql) == 1) {
        sql_rewrite = modifyUpdateQuery(sql, columnList);
      } else if (identifySQLType(sql) == 0) {
        sql_rewrite = modifySelectQuery(sql, columnList);
      }
      return sql_rewrite;
    } else {
      return sql;
    }
  }

  // Method: Add the field "vid" to the SELECT statement and ensure it is added at the end
  public static String modifySelectQuery(String sql, List<String> columnList) {
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
      columnList =
          Arrays.stream(selectItems.toArray())
              .filter(obj -> obj instanceof String)
              .map(obj -> (String) obj)
              .collect(Collectors.toList());
      // Return the modified SQL statement
      return selectStatement.toString();

    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  // Method: Modify the SQL statement to add the "vid" field and return it
  public static String modifyUpdateQuery(String sql, List<String> columnList) {
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

      // Add the SetExpression to the UPDATE statement
      updateStatement.getExpressions().add(addExpression); // Add "vid = vid + 1" to the SET clause

      // Manually add the RETURNING clause, since JSQLParser does not support it directly
      String returningClause = " RETURNING vid";
      columnList.add("vid");

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

  public void rewrite() {
    sql_rewrite = sql;
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
