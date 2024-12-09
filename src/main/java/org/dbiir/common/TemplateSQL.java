package org.dbiir.common;

import lombok.Getter;
import lombok.Setter;
import WorkloadConstants;


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

    public TemplateSQL(int op,  String relation, String sql) {
        this.op = op;
        this.sql = sql;
        this.sql_rewrite = "";
        this.templatename = templatename;
        this.workload = workload;
        this.relation = relation;
        // 在analyse的阶段已经被赋值
        needRewriteUnderSI = false;
        needRewriteUnderRC = false;
        skip = false;
    }

    public void parse() {
        if (needRewriteUnderC == true) {
            if (SQLTypeIdentifier(sql).equals("UPDATE")) {
                sql_rewrite = modifyReturningUpdateSQL(sql);
            } else if (SQLTypeIdentifier(sql).equals("SELECT")) {
                sql_rewrite = addVidToSelect(sql);
            }
        }

        if (needRewriteUnderSI == true) {
            if (SQLTypeIdentifier(sql).equals("UPDATE")) {
                sql_rewrite = modifyReturningUpdateSQL(sql);
            } else if (SQLTypeIdentifier(sql).equals("SELECT")) {
                sql_rewrite = addVidToSelect(sql);
            }
        }
    }

    public void rewrite() {
        sql_rewrite = sql;
    }

    // 方法：为 SELECT 语句添加字段 vid，确保位于最后
    public static String addVidToSelect(String sql) {
        // 检查 SQL 是否包含 "SELECT" 关键字
        if (sql.toUpperCase().startsWith("SELECT") && sql.contains("FROM")) {
            // 找到 "SELECT" 和 "FROM" 之间的部分，即字段列表
            int selectIndex = sql.indexOf("SELECT") + "SELECT".length();
            int fromIndex = sql.indexOf("FROM");

            // 获取 SELECT 和 FROM 之间的字段部分
            String selectFields = sql.substring(selectIndex, fromIndex).trim();

            // 如果字段部分已经有内容，添加 ", vid"，否则直接添加 "vid"
            if (!selectFields.isEmpty()) {
                selectFields += ", vid";
            } else {
                selectFields = "vid";
            }

            // 拼接修改后的 SQL 语句
            return "SELECT " + selectFields + sql.substring(fromIndex);
        } else {
            throw new IllegalArgumentException("Invalid SELECT SQL statement");
        }
    }

    // 方法：修改 SQL 语句，添加 vid 字段并返回
    public static String modifyUpdateSQL(String sql) {
        // 检查 SQL 是否包含 "SET" 关键字
        if (sql.toUpperCase().contains("SET")) {
            // 找到 "SET" 和 "WHERE" 之间的部分，即字段设置部分
            int setIndex = sql.indexOf("SET") + "SET".length();
            int whereIndex = sql.indexOf("WHERE");

            // 获取 "SET" 和 "WHERE" 之间的字段部分
            String setClause = sql.substring(setIndex, whereIndex).trim();

            // 确保字段部分以 ", vid = vid + 1" 结尾
            if (!setClause.isEmpty()) {
                setClause += ", vid = vid + 1";
            } else {
                setClause = "vid = vid + 1";
            }

            // 拼接修改后的 SQL 语句
            String modifiedSQL = sql.substring(0, setIndex) + " " + setClause + sql.substring(whereIndex);

            return modifiedSQL;
        } else {
            throw new IllegalArgumentException("Invalid UPDATE SQL statement: no SET clause found.");
        }
    }

    public static String modifyReturningUpdateSQL(String sql) {
        // 检查 SQL 是否包含 "SET" 关键字
        if (sql.toUpperCase().contains("SET")) {
            // 找到 "SET" 和 "WHERE" 之间的部分，即字段设置部分
            int setIndex = sql.indexOf("SET") + "SET".length();
            int whereIndex = sql.indexOf("WHERE");

            // 获取 "SET" 和 "WHERE" 之间的字段部分
            String setClause = sql.substring(setIndex, whereIndex).trim();

            // 确保字段部分以 ", vid = vid + 1" 结尾
            if (!setClause.isEmpty()) {
                setClause += ", vid = vid + 1";
            } else {
                setClause = "vid = vid + 1";
            }

            // 拼接修改后的 SQL 语句
            String modifiedSQL = sql.substring(0, setIndex) + " " + setClause + sql.substring(whereIndex)+ " RETURNING vid;";

            return modifiedSQL;
        } else {
            throw new IllegalArgumentException("Invalid UPDATE SQL statement: no SET clause found.");
        }
    }

    // 识别SQL语句
    public class SQLTypeIdentifier {
        public static String identifySQLType(String sql) {
            sql = sql.trim().toUpperCase();
            if (sql.startsWith("SELECT")) {
                return "SELECT";
            } else if (sql.startsWith("UPDATE")) {
                return "UPDATE";
            } else {
                return "Unknown";
            }
        }
    
        public static void main(String[] args) {
            String sql1 = "SELECT * FROM users WHERE id = 1";
            String sql2 = "UPDATE users SET name = 'Alice' WHERE id = 1";
            
            System.out.println(identifySQLType(sql1));  // 输出: SELECT
            System.out.println(identifySQLType(sql2));  // 输出: UPDATE
        }
    }
}
