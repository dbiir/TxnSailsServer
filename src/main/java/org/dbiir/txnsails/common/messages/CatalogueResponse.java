package org.dbiir.txnsails.common.messages;

import lombok.Getter;
import org.dbiir.txnsails.common.types.MessageType;

@Getter
public final class CatalogueResponse extends Message {
  private final int instanceID;
  private final String fingerprint;

  public CatalogueResponse(int instanceID, String fingerprint) {
    super(MessageType.CATALOGUE_RESP);
    this.instanceID = instanceID;
    this.fingerprint = fingerprint;
  }
}
