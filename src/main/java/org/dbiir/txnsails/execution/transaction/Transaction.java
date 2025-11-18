package org.dbiir.txnsails.execution.transaction;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.TransactionStatus;
import org.dbiir.txnsails.execution.utils.SpinLock;

import java.util.LinkedList;
import java.util.List;

@Getter
@Setter
public class Transaction {
  private long tid;
  private List<Participant> participants;
  @Getter @Setter private boolean prepared;
  @Getter @Setter private TransactionStatus status;
  private SpinLock lock;

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
  }

  public void addParticipant(Participant participant) {
    this.participants.add(participant);
  }

  public void reset() {
    this.participants.clear();
    prepared = false;
    this.status = TransactionStatus.ACTIVE;
  }

  public void spinLock() {
    this.lock.lock();
  }

  public void spinUnlock() {
    this.lock.unlock();
  }
}
