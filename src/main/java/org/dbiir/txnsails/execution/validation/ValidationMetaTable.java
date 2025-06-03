package org.dbiir.txnsails.execution.validation;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import lombok.Getter;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.common.constants.SmallBankConstants;
import org.dbiir.txnsails.common.constants.TPCCConstants;
import org.dbiir.txnsails.common.constants.YCSBConstants;

public class ValidationMetaTable {
  private static final ValidationMetaTable INSTANCE;
  private int LOAD_THREAD = 16;
  // another lock strategy in middle-tier concurrency under RC (unused)
  private final HashMap<String, LinkedList<ValidationLock>[]> validationLocks;
  private final HashMap<String, ReentrantReadWriteLock[]> validationBucketLocks;
  private final long lockWaitTimeout = 10;
  private final int maxRetry = 5;
  @Getter private String workload;
  public List<Connection> connections;
  public List<Lock> connectionGuards = new ArrayList<>();
  private final HashMap<String, List<RangeValidationLock>> rangeValidationLocks;

  static {
    INSTANCE = new ValidationMetaTable();
  }

  public ValidationMetaTable() {
    validationLocks = new HashMap<>(4);
    validationBucketLocks = new HashMap<>(4);
    connectionGuards = new ArrayList<>();
    rangeValidationLocks = new HashMap<>(4);
  }

  public void initHotspot(String workload, List<Connection> connections) throws SQLException {
    this.workload = workload;
    this.connections = connections;
    for (int i = 0; i < connections.size(); i++) {
      connectionGuards.add(new ReentrantLock());
    }

    ExecutorService executor = Executors.newFixedThreadPool(LOAD_THREAD);
    switch (workload) {
      case "smallbank" -> {
        // init validation locks
        for (Map.Entry<String, Integer> entry :
            SmallBankConstants.TABLENAME_TO_HASH_SIZE.entrySet()) {
          if (entry.getValue() <= 0) {
            continue;
          }
          createLockTable(entry.getKey(), entry.getValue());
          for (int i = 0; i < LOAD_THREAD; i++) {
            int index = i;
            String relationName = entry.getKey();
            executor.submit(
                () -> {
                  for (int j = index;
                      j < SmallBankConstants.getHashSize(relationName);
                      j += LOAD_THREAD) {
                    try {
                      if (!validationLocks.get(relationName)[j].isEmpty()) {
                        int latestVersion =
                            fetchLatestVersion(
                                connections.get(index),
                                SmallBankConstants.getLatestVersion(relationName, j));
                        updateHotspotVersion(relationName, j, latestVersion);
                      }
                    } catch (SQLException ex) {
                      throw new RuntimeException(ex);
                    }
                  }
                });
          }
        }
      }
      case "ycsb" -> {
        for (Map.Entry<String, Integer> entry : YCSBConstants.TABLENAME_TO_HASH_SIZE.entrySet()) {
          if (entry.getValue() <= 0) {
            continue;
          }
          createLockTable(entry.getKey(), entry.getValue());
          for (int i = 0; i < LOAD_THREAD; i++) {
            int index = i;
            String relationName = entry.getKey();
            executor.submit(
                () -> {
                  for (int j = index;
                      j < YCSBConstants.getHashSize(relationName);
                      j += LOAD_THREAD) {
                    try {
                      if (!validationLocks.get(relationName)[j].isEmpty()) {
                        int latestVersion =
                            fetchLatestVersion(
                                connections.get(index),
                                YCSBConstants.getLatestVersion(relationName, j));
                        updateHotspotVersion(relationName, j, latestVersion);
                      }
                    } catch (SQLException ex) {
                      throw new RuntimeException(ex);
                    }
                  }
                });
          }
        }
      }
      case "tpcc" -> {
        for (Map.Entry<String, Integer> entry : TPCCConstants.TABLENAME_TO_HASH_SIZE.entrySet()) {
          if (entry.getValue() <= 0) {
            continue;
          }
          createLockTable(entry.getKey(), entry.getValue());
          for (int i = 0; i < LOAD_THREAD; i++) {
            int index = i;
            String relationName = entry.getKey();
            executor.submit(
                () -> {
                  for (int j = index;
                      j < TPCCConstants.getHashSize(relationName);
                      j += LOAD_THREAD) {
                    try {
                      if (!validationLocks.get(relationName)[j].isEmpty()) {
                        int latestVersion =
                            fetchLatestVersion(
                                connections.get(index),
                                TPCCConstants.getLatestVersion(relationName, j));
                        updateHotspotVersion(relationName, j, latestVersion);
                      }
                    } catch (SQLException ex) {
                      throw new RuntimeException(ex);
                    }
                  }
                });
          }
        }
      }
    }
    executor.shutdown();
  }

  private int getHashSizeByRelationName(String relationName) {
    switch (workload) {
      case "ycsb":
        {
          return YCSBConstants.getHashSize(relationName);
        }
      case "smallbank":
        {
          return SmallBankConstants.getHashSize(relationName);
        }
      case "tpcc":
        {
          return TPCCConstants.getHashSize(relationName);
        }
      default:
        {
          System.out.println("Unknown relation");
          return -1;
        }
    }
  }

  private void createLockTable(String relationName, int bucketSize) {
    validationLocks.put(relationName, new LinkedList[bucketSize]);
    validationBucketLocks.put(relationName, new ReentrantReadWriteLock[bucketSize]);
    for (int i = 0; i < bucketSize; i++) {
      validationLocks.get(relationName)[i] = new LinkedList<>();
      validationLocks.get(relationName)[i].add(new ValidationLock(i));
      validationBucketLocks.get(relationName)[i] = new ReentrantReadWriteLock();
    }
  }

  public int fetchUnknownVersionCache(String tableName, int key) throws SQLException {
    long result = -1;
    int bucketNum = -1;
    switch (workload) {
      case "ycsb" -> {
        /*
         * `if` condition is always true, because txnsails should acquire validation lock
         * before fetching the latest version.
         */
        bucketNum = key % YCSBConstants.getHashSize(tableName);
        if (!validationLocks.get(tableName)[bucketNum].isEmpty()) {
          connectionGuards.get(key % LOAD_THREAD).lock();
          try {
            result =
                fetchLatestVersion(
                    connections.get(key % LOAD_THREAD),
                    YCSBConstants.getLatestVersion(tableName, key));
            updateHotspotVersion(tableName, key, result);
          } finally {
            connectionGuards.get(key % LOAD_THREAD).unlock();
          }
        }
      }
      case "smallbank" -> {
        bucketNum = key % SmallBankConstants.getHashSize(tableName);
        if (!validationLocks.get(tableName)[bucketNum].isEmpty()) {
          connectionGuards.get(key % LOAD_THREAD).lock();
          try {
            result =
                fetchLatestVersion(
                    connections.get(key % LOAD_THREAD),
                    SmallBankConstants.getLatestVersion(tableName, key));
            updateHotspotVersion(tableName, key, result);
          } finally {
            connectionGuards.get(key % LOAD_THREAD).unlock();
          }
        }
      }
      case "tpcc" -> {
        // implement hot version cache in tpc-c
        bucketNum = key % TPCCConstants.getHashSize(tableName);
        if (!validationLocks.get(tableName)[bucketNum].isEmpty()) {
          connectionGuards.get(key % LOAD_THREAD).lock();
          try {
            result =
                fetchLatestVersion(
                    connections.get(key % LOAD_THREAD),
                    TPCCConstants.getLatestVersion(tableName, key));
            updateHotspotVersion(tableName, key, result);
          } finally {
            connectionGuards.get(key % LOAD_THREAD).unlock();
          }
        }
      }
    }
    return (int) (result % Integer.MAX_VALUE);
  }

  private int fetchLatestVersion(Connection conn, String finalSQL) throws SQLException {
    int latestVersion = -1;
    if (!finalSQL.isEmpty()) {
      Statement stmt = conn.createStatement();
      try (ResultSet res = stmt.executeQuery(finalSQL)) {
        if (!res.next()) {
          String msg = "Invalid SQL #%sql".formatted(finalSQL);
          throw new RuntimeException(msg);
        }
        latestVersion = res.getInt("vid");
      }
    }
    return latestVersion;
  }

  // tid: transaction id
  public void tryValidationLock(String table, long tid, long key, LockType type, CCType ccType)
      throws SQLException {
    if (!validationLocks.containsKey(table)) {
      String msg = "unknown table name: " + table;
      throw new RuntimeException(msg);
    }

    if (checkAndTryValidationLock(table, tid, key, type, ccType)) return;
    try {
      tryAndAddValidationLock(table, tid, key, type, ccType);
    } catch (SQLException ex) {
      throw ex;
    }
  }

  public void tryValidationLock(String table, long tid, long key, LockType type, CCType ccType, Range requestedRange) throws SQLException {
    if (!validationLocks.containsKey(table)) {
      throw new RuntimeException("unknown table name: " + table);
    }

    // 1. check range conflict
    if (requestedRange != null) {
      if (!tryAcquireRangeLock(table, requestedRange, type)) {
        throw new SQLException("Range lock conflict on table " + table + " range " + requestedRange.start() + "-" + requestedRange.end());
      }
      // try success range lock
      return;
    }

    // 2. try point lock
    synchronized (rangeValidationLocks) {
      for (RangeValidationLock rangeLock : this.rangeValidationLocks.get(table)) {
        if (rangeLock.overlaps(key, key)) {
          System.out.println("Point " + key + " conflicts with range lock: " + rangeLock);
          throw new SQLException("Point lock conflict on table " + table + " key " + key);
        }
      }
    }
    tryValidationLock(table, tid, key, type, ccType);
  }

  private boolean tryAcquireRangeLock(String table, Range requestedRange, LockType type) {
    synchronized (rangeValidationLocks) {
      List<RangeValidationLock> rangeLocks = rangeValidationLocks.computeIfAbsent(table, k -> new ArrayList<>());

      // check conflict
      for (RangeValidationLock lock : rangeLocks) {
        if (lock.overlaps(requestedRange.start(), requestedRange.end())) {
          // check rw conflict
          if (type == LockType.SH && lock.rwLock.getReadLockCount() > 0 && !lock.rwLock.isWriteLocked()) {
            continue;
          }
          // conflict, NO-WAIT strategy
          return false;
        }
      }

      // No conflict, add interval lock
      RangeValidationLock newLock = new RangeValidationLock(requestedRange.start(), requestedRange.end());
      boolean locked = newLock.tryLock(type);
      if (!locked) {
        return false;
      }
      rangeLocks.add(newLock);
      return true;
    }
  }

  public void releaseValidationLock(String table, long key, LockType type) {
    if (!validationLocks.containsKey(table)) {
      String msg = "unknown table name: " + table;
      throw new RuntimeException(msg);
    }
    int bucketNum = (int) (key % getHashSizeByRelationName(table));
    validationBucketLocks.get(table)[bucketNum].readLock().lock();
    List<ValidationLock> lockList = validationLocks.get(table)[bucketNum];
    for (ValidationLock lock : lockList) {
      if (lock.getId() == key) {
        lock.releaseLock(type);
        break;
      }
    }
    validationBucketLocks.get(table)[bucketNum].readLock().unlock();

    // gc: shrink the validation lock list
    if (validationBucketLocks.get(table)[bucketNum].writeLock().tryLock()) {
      lockList.removeIf(
          lock ->
              lock.isExpired() && lock.getId() > getHashSizeByRelationName(table) && lock.free());
      validationBucketLocks.get(table)[bucketNum].writeLock().unlock();
    }
  }

  /*
   * @return: true if you get validation lock, otherwise can not find the validation lock
   */
  private boolean checkAndTryValidationLock(
      String table, long tid, long key, LockType type, CCType ccType) throws SQLException {
    int bucketNum = (int) (key % getHashSizeByRelationName(table));
    long currentTime = System.currentTimeMillis();
    validationBucketLocks.get(table)[bucketNum].readLock().lock();
    List<ValidationLock> lockList = validationLocks.get(table)[bucketNum];
    for (ValidationLock lock : lockList) {
      if (lock.getId() == key) {
        int res = 0;
        int count = 0;
        while ((res = lock.tryLock(tid, type, ccType)) == 0) {
          try {
            Thread.sleep(0, 10);
            // System.out.println("wait for lock " + tid + " " + table + ", lock type is " + type);
          } catch (InterruptedException ex) {
            System.out.println("out of the max retry count, lock type is " + type);
            res = -1;
            break;
          }
          count++;
        }
        if (res > 0) {
          validationBucketLocks.get(table)[bucketNum].readLock().unlock();
          return true;
        } else {
          // can not keep the sequence of read and write
          String msg =
              "Transaction #"
                  + tid
                  + " can not keep the sequence of rw dependency, there maybe rw-anti-dependency";
          validationBucketLocks.get(table)[bucketNum].readLock().unlock();
          throw new SQLException(msg, "500", 0);
        }
      }
    }
    validationBucketLocks.get(table)[bucketNum].readLock().unlock();
    System.out.println(
        "Validation Lock { "
            + table
            + ", "
            + key
            + ", "
            + type
            + ", "
            + (System.currentTimeMillis() - currentTime)
            + "ms}");
    return false;
  }

  public void tryAndAddValidationLock(
      String table, long tid, long key, LockType type, CCType ccType) throws SQLException {
    int bucketNum = (int) (key % getHashSizeByRelationName(table));
    validationBucketLocks.get(table)[bucketNum].writeLock().lock();
    List<ValidationLock> lockList = validationLocks.get(table)[bucketNum];
    for (ValidationLock lock : lockList) {
      if (lock.getId() == key) {
        int res;
        int count = 0;
        while ((res = lock.tryLock(tid, type, ccType)) == 0) {
          try {
            Thread.sleep(0, 10000);
            System.out.println("xxxxxxxxxxxxxxxxxxxxx");
          } catch (InterruptedException ex) {
            System.out.println("out of the max retry count, lock type is " + type);
            res = -1;
            break;
          }
          count++;
        }
        validationBucketLocks.get(table)[bucketNum].writeLock().unlock();
        if (res <= 0) {
          // can not keep the sequence of read and write
          String msg = "can not keep the sequence of rw dependency, there maybe rw-anti-dependency";
          throw new SQLException(msg, "500", 0);
        }
        return;
      }
    }
    ValidationLock newLock = new ValidationLock(key);
    validationLocks.get(table)[bucketNum].add(newLock);
    newLock.tryLock(tid, type, ccType);
    validationBucketLocks.get(table)[bucketNum].writeLock().unlock();
  }

  public void updateHotspotVersion(String table, long key, long tid) {
    int bucketNum = (int) (key % getHashSizeByRelationName(table));
    validationBucketLocks.get(table)[bucketNum].readLock().lock();
    for (ValidationLock lock : validationLocks.get(table)[bucketNum]) {
      if (lock.getId() == key) {
        lock.updateVersion(tid);
        break;
      }
    }
    validationBucketLocks.get(table)[bucketNum].readLock().unlock();
  }

  public long getHotspotVersion(String table, long key) {
    if (!validationLocks.containsKey(table)) {
      String msg = "unknown table name: " + table;
      throw new RuntimeException(msg);
    }
    int bucketNum = (int) (key % getHashSizeByRelationName(table));
    validationBucketLocks.get(table)[bucketNum].readLock().lock();
    for (ValidationLock lock : validationLocks.get(table)[bucketNum]) {
      if (lock.getId() == key) {
        validationBucketLocks.get(table)[bucketNum].readLock().unlock();
        return lock.getVersion();
      }
    }
    validationBucketLocks.get(table)[bucketNum].readLock().unlock();
    return -1;
  }

  public static ValidationMetaTable getInstance() {
    return INSTANCE;
  }
}
