package org.dbiir.txnsails.execution.validation;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.TemplateSQL;

@Getter
@Setter
public class ValidationMeta {
  private static final int SEED = 100000;
  private TemplateSQL templateSQL;
  /*
   * Change the type of oldVersions to List<> for scan operation in the future,
   * we do not support this because the absence of interval validation lock in the middle tier,
   * which would be implemented in the next iteration version and has little impact on our contribution.
   * TODO: We would refer to some existing mature technique solutions, such as SIREAD Lock in PostgreSQL.
   */
  private int oldVersions;
  // params for placeholder
  private final List<Integer> uniqueKeys = new ArrayList<>(4);
  private int uniqueKeyNumber;
  private int idForValidation; // identification for validation (mod INT_MAX for hash value)

  public void addUniqueKey(int key) {
    if (uniqueKeyNumber >= uniqueKeys.size()) {
      uniqueKeys.add(key);
    } else {
      uniqueKeys.set(uniqueKeyNumber, key);
      uniqueKeyNumber++;
    }

    // calculate the identification for navigating the validation entry
    if (uniqueKeyNumber == templateSQL.getUniqueKeyNumber()) {
      calculateId();
    }
  }

  // calculate the identification for navigating the validation entry
  private void calculateId() {
    for (int i = 0; i < uniqueKeyNumber; i++) {
      idForValidation = (uniqueKeys.get(i) + SEED * idForValidation) % Integer.MAX_VALUE;
    }
  }

  public void clearInfo() {
    oldVersions = -1;
    uniqueKeyNumber = 0;
    idForValidation = -1;
  }
}
