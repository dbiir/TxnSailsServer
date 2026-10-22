package org.dbiir.txnsails.execution.validation;

import java.util.HashMap;
import java.util.Map;
import org.dbiir.txnsails.common.TransactionIdentity;

/** Owner-side row state. Watermarks must survive holder release and local overwrites. */
public final class DistributedValidationEntry {
  private record Holder(boolean read, boolean write, CertificationContext context) {}

  private final Map<TransactionIdentity, Holder> holders = new HashMap<>();
  private int readers;
  private int strictReaders;
  private int writers;
  private int globalWriters;
  private long latestVersion = -1;
  private long latestGlobalWriteVersion = -1;

  /** Constant-time compatibility, independent of the number of concurrent readers. */
  public boolean tryAcquire(long tid, boolean read, boolean write, CertificationContext context) {
    return tryAcquire(new TransactionIdentity(0, tid), read, write, context);
  }

  public synchronized boolean tryAcquire(
      TransactionIdentity transaction, boolean read, boolean write, CertificationContext context) {
    Holder incoming = new Holder(read, write, context);
    Holder existing = holders.get(transaction);
    if (existing != null) return existing.equals(incoming);
    if (read && (context.strictRead() ? writers : globalWriters) > 0) return false;
    if (write && (writers > 0 || (context.global() ? readers : strictReaders) > 0)) return false;
    holders.put(transaction, incoming);
    if (read) {
      readers++;
      if (context.strictRead()) strictReaders++;
    }
    if (write) {
      writers++;
      if (context.global()) globalWriters++;
    }
    return true;
  }

  public void release(long tid) {
    release(new TransactionIdentity(0, tid));
  }

  public synchronized void release(TransactionIdentity transaction) {
    Holder holder = holders.remove(transaction);
    if (holder == null) return;
    if (holder.read()) {
      readers--;
      if (holder.context().strictRead()) strictReaders--;
    }
    if (holder.write()) {
      writers--;
      if (holder.context().global()) globalWriters--;
    }
  }

  public synchronized long latestVersion() {
    return latestVersion;
  }

  public synchronized long latestGlobalWriteVersion() {
    return latestGlobalWriteVersion;
  }

  public synchronized void publish(long version, boolean globalWrite) {
    latestVersion = Math.max(latestVersion, version);
    if (globalWrite) latestGlobalWriteVersion = Math.max(latestGlobalWriteVersion, version);
  }
}
