package org.dbiir.txnsails.common;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.Getter;
import lombok.Setter;

@Getter @Setter
public class DistributedTable {
  @JacksonXmlProperty(localName = "name")
  private String name;

  @JacksonXmlProperty(localName = "locations")
  private Locations locations;
}
