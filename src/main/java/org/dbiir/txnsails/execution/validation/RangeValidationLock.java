package org.dbiir.txnsails.execution.validation;

import org.dbiir.txnsails.common.types.LockType;

import java.util.concurrent.locks.ReentrantReadWriteLock;

public class RangeValidationLock {
  private final long start;
  private final long end;
  public final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();

  public RangeValidationLock(long start, long end) {
    if (start > end) throw new IllegalArgumentException("start > end");
    this.start = start;
    this.end = end;
  }

  public boolean overlaps(long otherStart, long otherEnd) {
    return this.start <= otherEnd && otherStart <= this.end;
  }

  /**
   * NO-WAIT strategy for acquiring locks.
   */
  public boolean tryLock(LockType type) {
    if (type == LockType.SH) {
      return rwLock.readLock().tryLock();
    } else {
      return rwLock.writeLock().tryLock();
    }
  }

  public void unlock(LockType type) {
    if (type == LockType.SH) {
      rwLock.readLock().unlock();
    } else {
      rwLock.writeLock().unlock();
    }
  }
}
