/*
 * Copyright 2020 by OLTPBenchmark Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dbiir.txnsails.common.constants;

import java.util.HashMap;

public abstract class TPCCConstants {
  public static final String TABLENAME_DISTRICT = "district";
  public static final String TABLENAME_WAREHOUSE = "warehouse";
  public static final String TABLENAME_ITEM = "item";
  public static final String TABLENAME_STOCK = "stock";
  public static final String TABLENAME_CUSTOMER = "customer";
  public static final String TABLENAME_HISTORY = "history";
  public static final String TABLENAME_OPENORDER = "oorder";
  public static final String TABLENAME_ORDERLINE = "order_line";
  public static final String TABLENAME_NEWORDER = "new_order";

  public static final int configWhseCount = 1;
  public static final int configItemCount = 100000; // tpc-c std = 100,000
  public static final int configDistPerWhse = 10; // tpc-c std = 10
  public static final int configCustPerDist = 3000; // tpc-c std = 3,000

  /** An invalid item id used to rollback a new order transaction. */
  public static final int INVALID_ITEM_ID = -12345;

  public static final HashMap<String, Integer> TABLENAME_TO_INDEX = new HashMap<>(12);

  static {
    TABLENAME_TO_INDEX.put(TABLENAME_WAREHOUSE, 0);
    TABLENAME_TO_INDEX.put(TABLENAME_DISTRICT, 1);
    TABLENAME_TO_INDEX.put(TABLENAME_ITEM, 2);
    TABLENAME_TO_INDEX.put(TABLENAME_STOCK, 4);
    TABLENAME_TO_INDEX.put(TABLENAME_CUSTOMER, 8);
    TABLENAME_TO_INDEX.put(TABLENAME_OPENORDER, 16);
    TABLENAME_TO_INDEX.put(TABLENAME_ORDERLINE, 32);
    TABLENAME_TO_INDEX.put(TABLENAME_NEWORDER, 64);
  }
}
