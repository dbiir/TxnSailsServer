package org.dbiir.common;

import lombok.Getter;
import lombok.Setter;

@Getter
public class TemplateSQL {
    @Getter
    private final int op; // 0 for read, 1 for write
    private final String sql;
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

    public TemplateSQL(int op, String relation, String sql) {
        this.op = op;
        this.sql = sql;
        this.sql_rewrite = "";
        needRewriteUnderSI = false;
        needRewriteUnderRC = false;
        skip = false;
    }

    public void parse() {

    }

    public void rewrite() {
        sql_rewrite = sql;
    }
}
