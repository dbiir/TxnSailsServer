package org.dbiir.txnsails.common;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class DistributedTables {
  @JacksonXmlProperty(localName = "table")
  private List<DistributedTable> tableList;

}
