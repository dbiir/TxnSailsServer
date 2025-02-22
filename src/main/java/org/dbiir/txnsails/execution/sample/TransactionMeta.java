package org.dbiir.txnsails.execution.sample;

import java.util.ArrayList;
import java.util.List;

import org.dbiir.txnsails.execution.utils.RWRecord;

import lombok.Getter;

@Getter
public class TransactionMeta {
  private int id;
  @Getter private final List<RWRecord> rset;
  @Getter private final List<RWRecord> wset;
  private int processing;

  public TransactionMeta(int id) {
    this.id = id;
    this.rset = new ArrayList<>(12);
    this.wset = new ArrayList<>(12);
  }

  public TransactionMeta(int id, List<RWRecord> rset, List<RWRecord> wset, int processing) {
    this.id = id;
    this.rset = new ArrayList<>(12);
    this.wset = new ArrayList<>(12);
    this.rset.addAll(rset);
    this.wset.addAll(wset);
    this.processing = processing;
  }

  public void addReadEntry(int tableIdx, int id) {
    this.rset.add(new RWRecord(tableIdx, id));
  }

  public void addWriteEntry(int tableIdx, int id) {
    this.wset.add(new RWRecord(tableIdx, id));
  }

  public void clearInfo() {
    this.rset.clear();
    this.wset.clear();
    this.id = -1;
  }

  //    static String featureFormat = "%d,%.2f";
  static String featureFormat = "%d, %d";

  public String transactionFeature() {
    //        return featureFormat.formatted(id, processing);
    // return featureFormat.formatted(id);
    return featureFormat.formatted(rset.size(), wset.size());
  }
}
