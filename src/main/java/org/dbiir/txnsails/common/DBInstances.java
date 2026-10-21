package org.dbiir.txnsails.common;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

public class DBInstances {
  @Getter
  @Setter
  @JacksonXmlElementWrapper(useWrapping = false)
  @JacksonXmlProperty(localName = "instance")
  private List<DBInstance> instanceList;
}
