package org.dbiir.txnsails.common;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class Locations {
  @JacksonXmlElementWrapper(useWrapping = false)
  @JacksonXmlProperty(localName = "location")
  private List<Integer> locationList;
}
