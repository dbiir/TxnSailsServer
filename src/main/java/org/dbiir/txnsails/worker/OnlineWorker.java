package org.dbiir.txnsails.worker;

import java.nio.ByteBuffer;
import java.sql.*;
import java.util.*;
import lombok.Getter;
import lombok.Setter;
import net.sf.jsqlparser.schema.Column;
import org.apache.commons.lang3.StringUtils;
import org.dbiir.txnsails.analysis.ConditionInfo;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.*;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.ColumnType;
import org.dbiir.txnsails.common.types.LockType;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.utils.RWRecord;
import org.dbiir.txnsails.execution.utils.SQLStmt;
import org.dbiir.txnsails.execution.validation.TransactionCollector;
import org.dbiir.txnsails.execution.validation.ValidationMeta;
import org.dbiir.txnsails.execution.validation.ValidationMetaTable;

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
  private static final long mask = 0x7FFFFFFFFFFFFFFFL;
  private final ValidationMeta[] validationMetaUnderRC = new ValidationMeta[MAX_VALIDATION_META];
  private final ValidationMeta[] validationMetaUnderSI = new ValidationMeta[MAX_VALIDATION_META];
  private int validationMetaIdxUnderRC = 0;
  private int validationMetaIdxUnderSI = 0;
  private boolean shouldSample = false;
  private int transactionId;
  // variables for sampling
  private final List<RWRecord> readSet;
  private final List<RWRecord> writeSet;
  private ValidationMeta sampleMeta = new ValidationMeta();
  private CCType lockManner = CCType.NUM_CC;
  private final SchemaInfo schema;

  public OnlineWorker(WorkloadConfiguration configuration, int id) {
    this.configuration = configuration;
    this.id = id;
    // init the connection
    try {
      this.conn = makeConnection();
      this.conn.setAutoCommit(false);
      switch (ccType) {
        case RC, RC_TAILOR -> this.conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        case SI, SI_TAILOR -> this.conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        case SER -> this.conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
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
      this.validationMetaUnderRC[i] = new ValidationMeta();
      this.validationMetaUnderSI[i] = new ValidationMeta();
    }
    // init the first transactionID
    this.transactionId =
        (int) (((System.nanoTime() << 10) | (Thread.currentThread().threadId() & 0x3ff)) & mask);
    // init sample container
    this.readSet = new ArrayList<>(8);
    this.writeSet = new ArrayList<>(8);
    // fetch the schema
    this.schema = MetaWorker.getINSTANCE().getSchema();
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
    StringBuilder results = new StringBuilder();
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
        meta.addRuntimeArgs(Arrays.asList(args).subList(2, args.length));
        System.out.println(
            templateSQL.getSQL()
                + ", "
                + Arrays.asList(args).subList(2, args.length)
                + ", Id for validation: "
                + meta.getIdForValidation());

        if (shouldSample) {
          addSampleMeta(
              templateSQL.getOp(),
              MetaWorker.getINSTANCE().getRelationType(templateSQL.getTable()),
              meta.getIdForValidation());
        }
        if (templateSQL.isNeedRewriteUnderRC()) {
          validationMetaIdxUnderRC++;
          if (templateSQL.isNeedRewriteUnderSI()) {
            validationMetaUnderSI[validationMetaIdxUnderSI++].deepCopy(meta);
          }
        }
      } else {
        System.out.println("The number of real time args is not enough.");
        throw new SQLException("Not enough arguments!");
      }
    }
    String rewrite_sql = templateSQL.getRewriteSQL();
    System.out.println("rewrite sql: " + rewrite_sql);
    // execute the sql
    try (PreparedStatement stmtc =
        this.getPreparedStatement(conn, new SQLStmt(rewrite_sql), args, templateSQL)) {
      try (ResultSet rs = stmtc.executeQuery()) {
        int v = -1;
        List<List<String>> rows = new ArrayList<>(2);
        while (rs.next()) {
          List<String> row = new ArrayList<>();
          for (Column col : templateSQL.getColumnList()) {
            String columnName = col.getColumnName();
            if (columnName.equalsIgnoreCase("vid")) {
              v = rs.getInt(columnName);
            } else {
              row.add(rs.getString(columnName));
            }
          }
          rows.add(row);
        }
        // parse and wrap the results
        results.append(wrapResults(rows));
        // record the version if it needs, support scan-based
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
      releaseValidationLocks(true);
      if (shouldSample) sampleTransaction(true);
    } catch (SQLException ex) {
      releaseValidationLocks(false);
      rollback();
      if (shouldSample) sampleTransaction(false);
      throw ex;
    }

    clearPreviousTransactionInfo();
    /* sample transaction if txnSails needs and choose whether sample next transaction */
    shouldSample = random.nextDouble() < SAMPLE_PROBABILITY;
    this.transactionId =
        (int) (((System.nanoTime() << 10) | (Thread.currentThread().threadId() & 0x3ff)) & mask);
    // switch the isolation mode
    if (Adapter.getInstance().isInSwitchPhase()) {
      switchConnectionIsolationMode();
    }
  }

  public void rollback() throws SQLException {
    try {
      conn.rollback();
    } finally {
      clearPreviousTransactionInfo();
      /* sample transaction if txnSails needs and choose whether sample next transaction */
      shouldSample = random.nextDouble() < SAMPLE_PROBABILITY;
      this.transactionId =
          (int) (((System.nanoTime() << 10) | (Thread.currentThread().threadId() & 0x3ff)) & mask);
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
        ValidationMetaTable.getInstance()
            .tryValidationLock(
                templateSQL.getTable(),
                this.transactionId,
                validationMeta.getIdForValidation(),
                lockType,
                Adapter.getInstance().getCCType());
        validationMeta.setLocked(true);
      }
    } else if (lockManner == CCType.SI_TAILOR) {
      for (int i = 0; i < validationMetaIdxUnderSI; i++) {
        ValidationMeta validationMeta = this.validationMetaUnderSI[i];
        TemplateSQL templateSQL = validationMeta.getTemplateSQL();
        LockType lockType = templateSQL.getOp() == 0 ? LockType.SH : LockType.EX;
        ValidationMetaTable.getInstance()
            .tryValidationLock(
                templateSQL.getTable(),
                this.transactionId,
                validationMeta.getIdForValidation(),
                lockType,
                Adapter.getInstance().getCCType());
        validationMeta.setLocked(true);
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
        ValidationMetaTable.getInstance()
            .getHotspotVersion(
                meta.getTemplateSQL().getTable(), (long) meta.getIdForValidation());
    if (v >= 0) {
      if (v != meta.getOldVersions()) {
        String msg =
            String.format(
                "Validation failed for key #%d, %s",
                meta.getIdForValidation(), meta.getTemplateSQL().getTable());
        throw new SQLException(msg, "500");
      }
    } else {
      v =
          ValidationMetaTable.getInstance()
              .fetchUnknownVersionCache(
                  meta.getTemplateSQL().getTable(), meta.getIdForValidation());
      if (v != meta.getOldVersions()) {
        //          releaseValidationLocks(false);
        String msg =
            String.format(
                "Validation failed for ycsb_key #%d, usertable", meta.getIdForValidation());
        throw new SQLException(msg, "500");
      }
    }
  }

  private void clearPreviousTransactionInfo() {
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

  private void releaseValidationLocks(boolean success) {
    if (this.lockManner == CCType.RC_TAILOR) {
      for (int i = 0; i < validationMetaIdxUnderRC; i++) {
        ValidationMeta meta = validationMetaUnderRC[i];
        if (!meta.isLocked()) continue;
        releaseSingleMeta(meta, success);
      }
    } else if (this.lockManner == CCType.SI_TAILOR) {
      for (int i = 0; i < validationMetaIdxUnderSI; i++) {
        ValidationMeta meta = validationMetaUnderSI[i];
        if (!meta.isLocked()) continue;
        releaseSingleMeta(meta, success);
      }
    }
  }

  private void releaseSingleMeta(ValidationMeta meta, boolean success) {
    TemplateSQL templateSQL = meta.getTemplateSQL();
    LockType lockType = templateSQL.getOp() == 0 ? LockType.SH : LockType.EX;
    ValidationMetaTable.getInstance()
        .releaseValidationLock(templateSQL.getTable(), meta.getIdForValidation(), lockType);
    if (success && lockType == LockType.EX) {
      // update the hot version cache (HVC) if the entry is cached in memory
      ValidationMetaTable.getInstance()
          .updateHotspotVersion(
              templateSQL.getTable(), meta.getIdForValidation(), meta.getOldVersions() + 1);
    }
    meta.setLocked(false);
    meta.clearInfo();
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
        case RC, RC_TAILOR -> conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        case SI, SI_TAILOR -> conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        case SER, SER_TRANSITION -> conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
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
   * @param templateSQL
   * @return
   * @throws SQLException
   */
  private final PreparedStatement getPreparedStatement(
      Connection conn, SQLStmt stmt, String[] params, TemplateSQL templateSQL) throws SQLException {
    PreparedStatement pStmt = conn.prepareStatement(stmt.getSQL());
    if (params.length - 2 != templateSQL.getAllPlaceholders().size()) {
      String msg = "The length of parameters and placeholders are not matching";
      System.out.println(msg);
      throw new SQLException(msg);
    }
    List<ConditionInfo> phList = templateSQL.getAllPlaceholders();
    for (int i = 2; i < params.length; i++) {
      // rewrite by the type of the params
      ColumnType columnType = schema.getColumnTypeByName(templateSQL.getTable(), phList.get(i - 2).getColumnName());
      switch (columnType) {
          case INTEGER -> pStmt.setObject(i - 1, Integer.valueOf(params[i]));
          case BIGINT -> pStmt.setObject(i - 1, Long.valueOf(params[i]));
          case FLOAT -> pStmt.setObject(i - 1, Float.valueOf(params[i]));
          case VARCHAR, TEXT -> pStmt.setObject(i - 1, params[i]);
          case DOUBLE -> pStmt.setObject(i - 1, Double.valueOf(params[i]));
          case BOOLEAN -> pStmt.setObject(i - 1, Boolean.valueOf(params[i]));
          default -> throw new AssertionError();
      }
    }
    return (pStmt);
  }

  private String wrapResults(List<List<String>> rows) {
    StringBuilder sb = new StringBuilder();

    for (List<String> row : rows) {
      sb.append(String.format("%02x", row.size()));   // record the column size in this row
      for (String col : row) {
        // record the string length in 4 bytes
//        System.out.println("col.length() = " + col.length());
        sb.append(String.format("%04x", col.length()));
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

        String value = results.substring(index, index + length); 
        index += length; // move to the next

        row.add(value);
      }
      rows.add(row); // add row
    }

    return rows;
  }

  @Override
  public String toString() {
    return String.format("%s<%03d>", this.getClass().getSimpleName(), this.getId());
  }
}
