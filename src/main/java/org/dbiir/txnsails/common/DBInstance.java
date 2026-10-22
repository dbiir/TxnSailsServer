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

  @JacksonXmlProperty(localName = "isolation")
  private String isolation;

  @JacksonXmlProperty(localName = "rpcPort")
  private int rpcPort = 23333;

  public DBInstance() {}

  public int jdbcIsolation(org.dbiir.txnsails.common.types.CCType fallback)
      throws java.sql.SQLException {
    String level = isolation == null ? fallback.name() : isolation;
    return switch (level.toUpperCase(java.util.Locale.ROOT).replace("TRANSACTION_", "")) {
      case "RC", "RC_TAILOR", "READ_COMMITTED" -> java.sql.Connection.TRANSACTION_READ_COMMITTED;
      case "SI", "SI_TAILOR", "REPEATABLE_READ" -> java.sql.Connection.TRANSACTION_REPEATABLE_READ;
      case "SER", "SERIALIZABLE", "DYNAMIC" -> java.sql.Connection.TRANSACTION_SERIALIZABLE;
      default -> throw new java.sql.SQLException("Unsupported database isolation " + level);
    };
  }

  public DBInstance(int id, String url, String host, String username, String password) {
    this.id = id;
    this.url = url;
    this.host = host;
    this.username = username;
    this.password = password;
  }

  @Override
  public String toString() {
    return String.format("id:%s, url:%s, host:%s", id, url, host);
  }
}
