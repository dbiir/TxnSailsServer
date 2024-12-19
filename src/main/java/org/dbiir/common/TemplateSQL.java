package org.dbiir.common;

import java.io.StringReader;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.update.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.schema.*;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;

@Getter
public class TemplateSQL {
    @Getter
    private final int op; // 0 for read, 1 for write
    @Getter
    private final String sql;
    @Getter
    private String sql_rewrite;
    @Getter
    private String relation;
    @Getter
    @Setter
    private boolean skip;
    @Setter
    @Getter
    private boolean needRewriteUnderSI;
    @Setter
    @Getter
    private boolean needRewriteUnderRC;
    @Setter
    @Getter
    private List<String> columnList;

    public TemplateSQL(int op, String relation, String sql) {
        this.op = op;
        this.sql = sql;
        this.sql_rewrite = "";
        this.relation = relation;
        this.columnList = null;
        // These are assigned during the analysis phase
        needRewriteUnderSI = false;
        needRewriteUnderRC = false;
        skip = false;
    }

    public void parse() {
        // Check if SQL needs to be rewritten under certain conditions
        if (needRewriteUnderRC) {
            if (identifySQLType(sql).equals("UPDATE")) {
                sql_rewrite = modifyUpdateQuery(sql, columnList);
            } else if (identifySQLType(sql).equals("SELECT")) {
                sql_rewrite = modifySelectQuery(sql, columnList);
            }
        }

        if (needRewriteUnderSI) {
            if (identifySQLType(sql).equals("UPDATE")) {
                sql_rewrite = modifyUpdateQuery(sql, columnList);
            } else if (identifySQLType(sql).equals("SELECT")) {
                sql_rewrite = modifySelectQuery(sql, columnList);
            }
        }
    }

    public void rewrite() {
        sql_rewrite = sql;
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
            
            columnList= Arrays.stream(selectItems.toArray())
                            .filter(obj -> obj instanceof String)  // 过滤掉非 String 类型的元素
                            .map(obj -> (String) obj)  // 转换为 String
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
            Addition addExpression = new Addition();  // Addition represents the "vid + 1" operation
            addExpression.setLeftExpression(vidColumn);  // Left side is the "vid" column
            addExpression.setRightExpression(oneValue); // Right side is the constant value 1

            // Add the SetExpression to the UPDATE statement
            updateStatement.getExpressions().add(addExpression);  // Add "vid = vid + 1" to the SET clause

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
    public static String identifySQLType(String sql) {
        try {
            // Create the SQL parser
            CCJSqlParserManager parserManager = new CCJSqlParserManager();
            
            // Parse the SQL statement
            Statement statement = parserManager.parse(new StringReader(sql));
            
            // Check the type of SQL statement
            if (statement instanceof Select) {
                return "SELECT";
            } else if (statement instanceof Update) {
                return "UPDATE";
            } else {
                return "Unknown SQL type";
            }
            
        } catch (Exception e) {
            e.printStackTrace();
            return "Error in parsing SQL";
        }
    }
}
