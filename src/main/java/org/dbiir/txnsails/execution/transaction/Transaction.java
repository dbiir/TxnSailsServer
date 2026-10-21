package org.dbiir.txnsails.execution.transaction;

import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.TransactionStatus;
import org.dbiir.txnsails.execution.utils.SpinLock;

@Getter
@Setter
public class Transaction {
  private long tid;
  private List<Participant> participants;
  @Getter @Setter private boolean prepared;
  @Getter @Setter private TransactionStatus status = TransactionStatus.ACTIVE;
  private SpinLock lock = new SpinLock();
  private boolean rollbackOnly;

  public Transaction(long tid, List<Participant> participants) {
    this.tid = tid;
    this.participants = participants;
  }

  public Transaction(long tid) {
    this.tid = tid;
    this.participants = new LinkedList<>();
  }

  public void init(long tid) {
    this.tid = tid;
    this.status = TransactionStatus.ACTIVE;
    this.prepared = false;
    this.rollbackOnly = false;
  }

  public void addParticipant(Participant participant) {
    this.participants.add(participant);
  }

  public void reset() {
    this.participants.clear();
    prepared = false;
    rollbackOnly = false;
    this.status = TransactionStatus.ACTIVE;
  }

  public void spinLock() {
    this.lock.lock();
  }

  public void spinUnlock() {
    this.lock.unlock();
  }
}
