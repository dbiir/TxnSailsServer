package org.dbiir.common;

import lombok.Getter;

public class TransactionTemplate {
    @Getter
    private final String name;
    // a template contains at most 15 sqls for now
    private final TemplateSQL[] sqls = new TemplateSQL[15];
    private int sql_idx_in_template;

    public TransactionTemplate(String name) {
        this.name = name;
        this.sql_idx_in_template = 0;
    }

    public int addTemplateSQL(int op, String relation, String sql) {
        this.sqls[sql_idx_in_template] = new TemplateSQL(op,relation, sql);
        sql_idx_in_template++;
        return sql_idx_in_template - 1;
    }

    // return the rewrite sql
    public String getSQLByIndex(int idx) {
        if (idx >= sql_idx_in_template) {
            System.out.println("template SQL's index out of bound");
            return "";
        }
        return sqls[idx].getSql_rewrite();
    }

    public int totalSQLs() {
        return sql_idx_in_template;
    }

    public int getOperationByIndex(int idx) {
        return sqls[idx].getOp();
    }

    public String getRelationByIndex(int idx) {
        return sqls[idx].getRelation();
    }

    public boolean shouldSkipIndex(int idx) {
        return sqls[idx].isSkip();
    }

    public void setSkipIndex(int idx, boolean skip) {
        sqls[idx].setSkip(skip);
    }
}
