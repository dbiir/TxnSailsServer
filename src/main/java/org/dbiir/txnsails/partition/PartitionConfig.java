package org.dbiir.txnsails.partition;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.common.types.CCType;

@Getter
@Setter
@JacksonXmlRootElement(localName = "partition")
public class PartitionConfig {
  @JacksonXmlProperty(localName = "id")
  private int id;
  @JacksonXmlProperty(localName = "tables")
  private String tableLists;
  @JacksonXmlProperty(localName = "concurrencyControlType")
  private CCType ccType;

  public PartitionConfig(int id, CCType ccType) {
    this.id = id;
    this.tableLists = "";
    this.ccType = ccType;
  }

  public PartitionConfig(int id, String tableLists, CCType ccType) {
    this.id = id;
    this.tableLists = tableLists;
    this.ccType = ccType;
  }
}
