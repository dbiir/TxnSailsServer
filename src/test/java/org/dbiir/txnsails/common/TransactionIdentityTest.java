package org.dbiir.txnsails.common;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import org.dbiir.txnsails.common.messages.FinalRequest;
import org.dbiir.txnsails.common.messages.ValidationRequest;
import org.dbiir.txnsails.execution.validation.CertificationContext;
import org.junit.jupiter.api.Test;

class TransactionIdentityTest {
  @Test
  void fullCoordinatorAndSequenceSurviveSerialization() throws Exception {
    var identity = new TransactionIdentity(Integer.MAX_VALUE, Long.MAX_VALUE);
    var bytes = new ByteArrayOutputStream();
    try (var output = new ObjectOutputStream(bytes)) {
      output.writeObject(identity);
    }
    try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      assertEquals(identity, input.readObject());
    }
    assertNotEquals(new TransactionIdentity(0, 17), new TransactionIdentity(256, 17));
    assertThrows(IllegalArgumentException.class, () -> new TransactionIdentity(-1, 0));
    assertThrows(IllegalArgumentException.class, () -> new TransactionIdentity(0, -1));
  }

  @Test
  void rpcCompatibilityConstructorsUseCoordinatorZero() {
    assertEquals(
        new TransactionIdentity(0, 17),
        new ValidationRequest(17, List.of()).getTransactionIdentity());
    assertEquals(
        new TransactionIdentity(0, 17),
        new FinalRequest(17, false, List.of()).getTransactionIdentity());
    var identity = new TransactionIdentity(256, 17);
    var validation =
        new ValidationRequest(identity, List.of(), CertificationContext.FULL, "catalogue");
    var finish = new FinalRequest(identity, true, List.of());
    assertEquals(identity, validation.getTransactionIdentity());
    assertEquals(identity, finish.getTransactionIdentity());
    assertEquals(17, validation.getTid());
    assertEquals(17, finish.getTid());
    assertEquals(256, validation.getCoordinatorId());
    assertEquals(256, finish.getCoordinatorId());
  }
}
