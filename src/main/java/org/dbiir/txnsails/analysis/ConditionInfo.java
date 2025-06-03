package org.dbiir.txnsails.analysis;

import lombok.Getter;
import lombok.Setter;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.schema.Column;

import java.util.concurrent.locks.Condition;

public class ConditionInfo {
  @Getter
  private final Column column;
  @Getter
  private final String upperCaseColumnName;
  private final JdbcParameter parameter;
  @Getter
  private Expression expression;

  public ConditionInfo(Column column, JdbcParameter parameter) {
    this.column = column;
    this.upperCaseColumnName = column.getColumnName().toUpperCase();
    this.parameter = parameter;
  }

  public ConditionInfo(Column column, JdbcParameter parameter, Expression expression) {
    this.column = column;
    this.upperCaseColumnName = column.getColumnName().toUpperCase();
    this.parameter = parameter;
    this.expression = expression;
  }

  public String getColumnName() {
    return column.getColumnName();
  }

  public String getColumnFullName() {
    return column.getFullyQualifiedName();
  }

  public int getPlaceholderIndex() {
    return parameter.getIndex() - 1;
  }
}
