package org.dbiir.txnsails.common;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter @Setter
@JacksonXmlRootElement(localName = "parameters")
public class DistributedConfig {
  @JacksonXmlProperty(localName = "instances")
  private DBInstances instances;

  @JacksonXmlProperty(localName = "tables")
  private DistributedTables tables;
}

