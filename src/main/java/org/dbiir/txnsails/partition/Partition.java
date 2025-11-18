package org.dbiir.txnsails.partition;

import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.types.CCType;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class Partition {
  private int id;
  private List<String> tableLists;
  private CCType ccType;

  public Partition(int id, CCType ccType) {
    this.id = id;
    this.tableLists = new ArrayList<>();
    this.ccType = ccType;
  }

  public Partition(int id, List<String> tableLists, CCType ccType) {
    this.id = id;
    this.tableLists = tableLists;
    this.ccType = ccType;
  }
}
