package org.dbiir.txnsails.common;

import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.schema.Column;

public class ConditionInfo {
  private final Column column;
  private final String upperCaseColumnName;
  private final JdbcParameter parameter;

  public ConditionInfo(Column column, JdbcParameter parameter) {
    this.column = column;
    this.upperCaseColumnName = column.getColumnName().toUpperCase();
    this.parameter = parameter;
  }

  public String getUpperCaseColumnName() {
    return column.getColumnName();
  }

  public String getColumnName() {
    return column.getColumnName();
  }

  public String getColumnFullName() {
    return column.getFullyQualifiedName();
  }

  public int getPlaceholderIndex() {
    return parameter.getIndex();
  }
}
