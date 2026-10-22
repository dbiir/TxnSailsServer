package org.dbiir.txnsails.common.messages;

import org.dbiir.txnsails.common.types.MessageType;

/** Startup catalogue agreement has no transaction state or side effects. */
public final class CatalogueRequest extends Message {
  public CatalogueRequest() {
    super(MessageType.CATALOGUE_REQ);
  }
}
