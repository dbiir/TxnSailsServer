package org.dbiir.txnsails.execution.validation;

public record Range(long start, long end) {
  public Range {
    if (start > end) throw new IllegalArgumentException("start > end");
  }
}
