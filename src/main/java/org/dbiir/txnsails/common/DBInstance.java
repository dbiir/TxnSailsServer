package org.dbiir.txnsails.common;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import lombok.Getter;
import lombok.Setter;

@Setter
@Getter
public class DBInstance {
  @JacksonXmlProperty(localName = "id")
  private int id;

  @JacksonXmlProperty(localName = "url")
  private String url;

  @JacksonXmlProperty(localName = "host")
  private String host;

  @JacksonXmlProperty(localName = "username")
  private String username;

  @JacksonXmlProperty(localName = "password")
  private String password;

  public DBInstance(int id, String url, String host, String username, String password){
    this.id = id;
    this.url = url;
    this.host = host;
    this.username = username;
    this.password = password;
  }

  @Override
  public String toString() {
    return String.format("url:%s, host: %s, username:%s, password:%s",
            this.getUrl(), this.getHost(), this.getUsername(),this.getPassword());
  }
}
