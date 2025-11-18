package org.dbiir.txnsails.common;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

public class DBInstances {
  @Getter @Setter
  @JacksonXmlProperty(localName = "instance")
  private List<DBInstance> instanceList;
}