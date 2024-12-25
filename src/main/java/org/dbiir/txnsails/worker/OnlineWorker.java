package org.dbiir.txnsails.worker;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.dbiir.txnsails.common.*;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.utils.RWRecord;
import org.dbiir.txnsails.execution.utils.SQLStmt;
import org.dbiir.txnsails.execution.validation.LockTable;
import org.dbiir.txnsails.execution.validation.TransactionCollector;
import org.dbiir.txnsails.execution.validation.ValidationMeta;

public class OnlineWorker {
  protected Connection conn = null;
  protected Connection conn2 = null;
  private WorkloadConfiguration configuration = null;
  private Random random = new Random();
  @Getter private final int id;
  private boolean seenDone = false;
  CCType ccType = CCType.NUM_CC;
  @Setter @Getter protected boolean switchFinish = false;

  @Getter @Setter
  protected boolean switchPhaseReady = false; // validation transaction common, set it to true

  @Getter @Setter private TransactionStatus status = TransactionStatus.IDLE;
  private HashMap<String, TransactionTemplate> templates = new HashMap<>();
  // support at most 20 validation entries in a single transaction
  private static final int MAX_VALIDATION_META = 20;
  private static final double SAMPLE_PROBABILITY = 0.01;
  private ValidationMeta[] validationMetas = new ValidationMeta[MAX_VALIDATION_META];
  private ValidationMeta[] auxValidationMetas = new ValidationMeta[MAX_VALIDATION_META];
  private ValidationMeta[] validationMetaUnderRC = new ValidationMeta[MAX_VALIDATION_META];
  private ValidationMeta[] validationMetaUnderSI = new ValidationMeta[MAX_VALIDATION_META];
  private int validationMetaIdx = 0;
  private int validationMetaIdxUnderRC = 0;
  private int validationMetaIdxUnderSI = 0;
  private int auxValidationMetaIdx = 0;
  private boolean shouldSample = false;
  private int transactionId;
  // variables for sampling
  private final List<RWRecord> readSet;
  private final List<RWRecord> writeSet;
  private ValidationMeta sampleMeta = new ValidationMeta();
  private CCType lockManner = CCType.NUM_CC;

  public OnlineWorker(WorkloadConfiguration configuration, int id) {
    this.configuration = configuration;
    this.id = id;
    // init the connection
    try {
      this.conn = makeConnection();
      this.conn.setAutoCommit(false);
      switch (ccType) {
        case RC:
        case RC_TAILOR:
          this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
          break;
        case SI:
        case SI_TAILOR:
          this.conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
          break;
        case SER:
          this.conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      }
      // read the lasted version
      this.conn2 = makeConnection();
      this.conn2.setAutoCommit(true);
      this.conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    } catch (SQLException ex) {
      throw new RuntimeException("Failed to connect to database", ex);
    }
    // clone the analysis result to thread local
    MetaWorker.getINSTANCE().cloneTemplatesAfterAnalysis(this.templates);
    // init the validation metas
    for (int i = 0; i < MAX_VALIDATION_META; i++) {
      this.validationMetas[i] = new ValidationMeta();
    }
    // init the first transactionID
    this.transactionId = MetaWorker.getINSTANCE().fetchTransactionId();
    // init sample container
    this.readSet = new ArrayList<>(8);
    this.writeSet = new ArrayList<>(8);
    // register self into AdapterWorker
    Adapter.getInstance().addOnlineWorker(this);
  }

  private Connection makeConnection() throws SQLException {
    if (StringUtils.isEmpty(configuration.getUsername())) {
      return DriverManager.getConnection(configuration.getUrl());
    } else {
      return DriverManager.getConnection(
          configuration.getUrl(), configuration.getUsername(), configuration.getPassword());
    }
  }

  /**
   * online execution
   *
   * @param args
   * @return execution results args[0]: transaction template name args[1]: sql index in this
   *     template args[2:]: the params for
   */
  public String execute(String[] args) throws SQLException {
    if (args.length < 2) return "";
    StringBuffer results = new StringBuffer();
    TemplateSQL templateSQL =
        this.templates.get(args[0]).getSQLTemplateByIndex(Integer.parseInt(args[1]));

    // record the sql that need validate
    if (templateSQL.isNeedRewriteUnderRC() || templateSQL.isNeedRewriteUnderSI() || shouldSample) {
      ValidationMeta meta =
          templateSQL.isNeedRewriteUnderRC()
              ? validationMetaUnderRC[validationMetaIdxUnderRC]
              : templateSQL.isNeedRewriteUnderSI()
                  ? validationMetaUnderSI[validationMetaIdxUnderSI]
                  : sampleMeta;
      meta.setTemplateSQL(templateSQL);
      if (templateSQL.getUniqueKeyNumber() <= args.length - 2) {
        for (int i = 0; i < templateSQL.getUniqueKeyNumber(); i++) {
          meta.addUniqueKey(Integer.parseInt(args[2 + templateSQL.getUniqueKeyIdx(i)]));
        }

        if (shouldSample) {
          addSampleMeta(
              templateSQL.getOp(),
              MetaWorker.getINSTANCE().getRelationType(templateSQL.getRelation()),
              meta.getIdForValidation());
        }
        if (templateSQL.isNeedRewriteUnderRC()) {
          validationMetaIdxUnderRC++;
          if (templateSQL.isNeedRewriteUnderSI()) {
            validationMetaUnderSI[validationMetaIdxUnderSI] = meta;
            validationMetaIdxUnderSI++;
          }
        }
      } else {
        System.out.println("The number of real time args is not enough.");
        throw new SQLException("Not enough arguments!");
      }
    }
    String rewrite_sql = templates.get(args[0]).getSQLByIndex(Integer.parseInt(args[1]), ccType);
    // TODO: execute the sql
    try (PreparedStatement stmtc =
        this.getPreparedStatement(
            conn, new SQLStmt(rewrite_sql), Arrays.copyOfRange(args, 1, args.length))) {
      try (ResultSet r0 = stmtc.executeQuery()) {
        int v = -1;
        // TODO: parse and wrap the results

        // TODO: record the version if it needs, support scan-based
        if (templateSQL.isNeedRewriteUnderRC()) {
          validationMetaUnderSI[validationMetaIdxUnderSI - 1].setOldVersions(v);
          if (templateSQL.isNeedRewriteUnderSI()) {
            validationMetaUnderRC[validationMetaIdxUnderRC - 1].setOldVersions(v);
          }
        }
      } catch (SQLException ex) {
        // check if the error can retry automatically, in the future
        throw ex;
      }
    }

    return results.toString();
  }

  public void commit() throws SQLException {
    /* validate before commitment, release validation locks after commitment */
    try {
      validate();
      conn.commit();
      if (shouldSample) sampleTransaction(true);
    } catch (SQLException ex) {
      rollback();
      if (shouldSample) sampleTransaction(false);
      throw ex;
    }

    clearPreviousTransactionInfo();
    /* sample transaction if txnSails needs and choose whether sample next transaction */
    shouldSample = random.nextDouble() < SAMPLE_PROBABILITY;
    MetaWorker.getINSTANCE().fetchTransactionId();
    // switch the isolation mode

  }

  public void rollback() throws SQLException {
    try {
      conn.rollback();
    } catch (SQLException ex) {
      throw ex;
    } finally {
      clearPreviousTransactionInfo();
      /* sample transaction if txnSails needs and choose whether sample next transaction */
      shouldSample = random.nextDouble() < SAMPLE_PROBABILITY;
      MetaWorker.getINSTANCE().fetchTransactionId();
      // switch the isolation
      if (Adapter.getInstance().isInSwitchPhase()) {
        switchConnectionIsolationMode();
      }
    }
  }

  private void validate() throws SQLException {
    /* 0. block transaction during the transition until
     * 1. acquire validation lock (transition: acquire the lock according the stricter isolation level)
     * 2. check the version
     */
    while (Adapter.getInstance().isInSwitchPhase()
        && !Adapter.getInstance().isAllWorkersReadyForSwitch()) {
      // set current thread ready, block for all thread to ready
      if (!this.isSwitchPhaseReady()) {
        this.setSwitchPhaseReady(true);
        System.out.println(Thread.currentThread().getName() + " is ready for switch");
        // cases: SER -> SI/RC
        if (this.ccType == CCType.SER) {
          this.ccType = CCType.SER_TRANSITION;
        }
      } else {
        try {
          Thread.sleep(1);
        } catch (InterruptedException e) {
        }
      }
    }
    if (!Adapter.getInstance().isInSwitchPhase() && this.ccType == CCType.SER_TRANSITION) {
      // cases: SI/RC -> SER
      this.ccType = CCType.SER;
    }

    this.lockManner = Adapter.getInstance().getCCType();
    if (lockManner == CCType.RC_TAILOR) {
      for (int i = 0; i < validationMetaIdxUnderRC; i++) {
        ValidationMeta validationMeta = this.validationMetaUnderRC[i];
        TemplateSQL templateSQL = validationMeta.getTemplateSQL();
        LockType lockType = templateSQL.getOp() == 0 ? LockType.SH : LockType.EX;
        LockTable.getInstance()
            .tryValidationLock(
                templateSQL.getRelation(),
                this.transactionId,
                validationMeta.getIdForValidation(),
                lockType,
                Adapter.getInstance().getCCType());
      }
    } else if (lockManner == CCType.SI_TAILOR) {
      for (int i = 0; i < validationMetaIdxUnderSI; i++) {
        ValidationMeta validationMeta = this.validationMetaUnderSI[i];
        TemplateSQL templateSQL = validationMeta.getTemplateSQL();
        LockType lockType = templateSQL.getOp() == 0 ? LockType.SH : LockType.EX;
        LockTable.getInstance()
            .tryValidationLock(
                templateSQL.getRelation(),
                this.transactionId,
                validationMeta.getIdForValidation(),
                lockType,
                Adapter.getInstance().getCCType());
      }
    }

    // validation row versions
    if (ccType == CCType.SI_TAILOR || ccType == CCType.SER_TRANSITION) {
      for (ValidationMeta meta : this.validationMetaUnderSI) {
        validateSingleMeta(meta);
      }
    } else if (ccType == CCType.RC_TAILOR) {
      for (ValidationMeta meta : this.validationMetaUnderRC) {
        validateSingleMeta(meta);
      }
    }
  }

  private void validateSingleMeta(ValidationMeta meta) throws SQLException {
    long v =
        LockTable.getInstance()
            .getHotspotVersion(
                meta.getTemplateSQL().getRelation(), (long) meta.getIdForValidation());
    if (v >= 0) {
      if (v != meta.getOldVersions()) {
        releaseValidationLocks();
        String msg =
            String.format(
                "Validation failed for key #%d, %s",
                meta.getIdForValidation(), meta.getTemplateSQL().getRelation());
        throw new SQLException(msg, "500");
      }
    } else {
      try {
        v =
            LockTable.getInstance()
                .fetchUnknownVersionCache(
                    meta.getTemplateSQL().getRelation(), meta.getIdForValidation());
        if (v != meta.getOldVersions()) {
          releaseValidationLocks();
          String msg =
              String.format(
                  "Validation failed for ycsb_key #%d, usertable", meta.getIdForValidation());
          throw new SQLException(msg, "500");
        }
      } catch (SQLException ex) {
        releaseValidationLocks();
      }
    }
  }

  private void clearPreviousTransactionInfo() {
    this.validationMetaIdx = 0;
    this.transactionId = MetaWorker.getINSTANCE().fetchTransactionId();
    this.auxValidationMetaIdx = 0;
    this.validationMetaIdxUnderRC = 0;
    this.validationMetaIdxUnderSI = 0;
  }

  private void addSampleMeta(int op, int relationType, int idx) {
    if (op == 0) {
      this.readSet.add(new RWRecord(relationType, idx));
    } else if (op == 1) {
      this.writeSet.add(new RWRecord(relationType, idx));
    }
  }

  private void releaseValidationLocks() {
    for (int i = this.validationMetaIdx - 1; i >= 0; i--) {
      ValidationMeta meta = validationMetas[i];
      TemplateSQL templateSQL = meta.getTemplateSQL();
      LockType lockType = templateSQL.getOp() == 0 ? LockType.SH : LockType.EX;
      LockTable.getInstance()
          .releaseValidationLock(templateSQL.getRelation(), meta.getIdForValidation(), lockType);
      if (lockType == LockType.EX) {
        // update the hot version cache (HVC) if the entry is cached in memory
        LockTable.getInstance()
            .updateHotspotVersion(
                templateSQL.getRelation(), meta.getIdForValidation(), meta.getOldVersions());
      }
    }
    validationMetaIdx = 0;
  }

  private boolean shouldRewrite(TemplateSQL templateSQL, CCType t) {
    if ((t == CCType.SI_TAILOR || t == CCType.SER_TRANSITION)
        && templateSQL.isNeedRewriteUnderSI()) {
      return true;
    }

    if (t == CCType.RC_TAILOR && templateSQL.isNeedRewriteUnderRC()) {
      return true;
    }
    return false;
  }

  // mode: Connection.TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ,
  // TRANSACTION_SERIALIZABLE
  private void switchConnectionIsolationMode() throws SQLException {
    // block for all validate transaction completed
    while (Adapter.getInstance().isInSwitchPhase()
        && !Adapter.getInstance().isAllWorkersReadyForSwitch()) {
      if (!this.switchPhaseReady) {
        this.switchPhaseReady = true;
      }

      try {
        Thread.sleep(1L);
      } catch (InterruptedException ignored) {
      }
    }

    if (Adapter.getInstance().isInSwitchPhase() && !switchFinish) {
      this.ccType = Adapter.getInstance().getNextCCType();

      switch (this.ccType) {
        case RC:
        case RC_TAILOR:
          conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
          break;
        case SI:
        case SI_TAILOR:
          conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
          break;
        case SER:
        case SER_TRANSITION:
          conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      }
      if (this.ccType == CCType.SER) {
        this.ccType = CCType.SER_TRANSITION;
      }
      switchFinish = true;
      conn.setAutoCommit(false);
      System.out.println(
          Thread.currentThread().getName()
              + "switch finish: "
              + Adapter.getInstance().getNextCCType());
    }
  }

  private void sampleTransaction(boolean success) {
    TransactionCollector.getInstance().addTransactionSample(1, readSet, writeSet, success ? 1 : 0);
  }

  /**
   * Return a PreparedStatement for the given SQLStmt handle The underlying Procedure API will make
   * sure that the proper SQL for the target DBMS is used for this SQLStmt. This will automatically
   * call setObject for all the parameters you pass in
   *
   * @param conn
   * @param stmt
   * @param params: just support String, maybe provide more types in the future
   * @return
   * @throws SQLException
   */
  private final PreparedStatement getPreparedStatement(
      Connection conn, SQLStmt stmt, String[] params) throws SQLException {
    PreparedStatement pStmt = conn.prepareStatement(stmt.getSQL());
    for (int i = 0; i < params.length; i++) {
      pStmt.setObject(i + 1, params[i]);
    }
    return (pStmt);
  }

  private String wrapResults(List<List<String>> rows) {
    StringBuilder sb = new StringBuilder();

    for (List<String> row : rows) {
      sb.append(String.format("%02x", row.size()));
      for (String col : row) {
        // record the string length in 2 bytes
        byte[] lengthBytes = ByteBuffer.allocate(2).putShort((short) col.length()).array();
        for (byte b : lengthBytes) {
          sb.append(String.format("%02x", b));
        }
        sb.append(col); // wrap column value
      }
    }
    return sb.toString();
  }

  private List<List<String>> dewrapResults(String results) {
    List<List<String>> rows = new ArrayList<>();
    int index = 0;

    while (index < results.length()) {
      // decode the number of columns in this row
      int count = Integer.parseInt(results.substring(index, index + 2), 16);
      index += 2;

      List<String> row = new ArrayList<>();
      for (int i = 0; i < count; i++) {
        // read 4 char as the column length (2 bytes)
        String lengthHex = results.substring(index, index + 4);
        int length = Integer.parseInt(lengthHex, 16);
        index += 4;

        String value = results.substring(index, index + length); // 根据长度提取值
        index += length; // 移动到下一个位置

        row.add(value);
      }
      rows.add(row); // 将当前行的 Pair 添加到总列表中
    }

    return rows;
  }

  @Override
  public String toString() {
    return String.format("%s<%03d>", this.getClass().getSimpleName(), this.getId());
  }
}
