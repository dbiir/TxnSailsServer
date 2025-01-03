/***************************************************************************
 *  Copyright (C) 2013 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package org.dbiir.txnsails.common.constants;

import java.util.HashMap;

public abstract class SmallBankConstants {

  // ----------------------------------------------------------------
  // TABLE NAMES
  // ----------------------------------------------------------------
  public static final String TABLENAME_ACCOUNTS = "accounts";
  public static final String TABLENAME_SAVINGS = "savings";
  public static final String TABLENAME_CHECKING = "checking";
  public static final String TABLENAME_CONFLICT = "conflict";
  public static final HashMap<String, Integer> TABLENAME_TO_INDEX = new HashMap<>(4);

  static {
    TABLENAME_TO_INDEX.put(TABLENAME_ACCOUNTS, 0);
    TABLENAME_TO_INDEX.put(TABLENAME_SAVINGS, 1);
    TABLENAME_TO_INDEX.put(TABLENAME_CHECKING, 2);
  }

  public static final HashMap<String, Integer> TABLENAME_TO_HASH_SIZE = new HashMap<>(4);

  static {
    TABLENAME_TO_HASH_SIZE.put(TABLENAME_ACCOUNTS, 0);
    TABLENAME_TO_HASH_SIZE.put(TABLENAME_SAVINGS, 4000000);
    TABLENAME_TO_HASH_SIZE.put(TABLENAME_CHECKING, 4000000);
  }

  // ----------------------------------------------------------------
  // ACCOUNT INFORMATION
  // ----------------------------------------------------------------

  // Default number of customers in bank
  public static final int NUM_ACCOUNTS = 1000000;

  public static final boolean HOTSPOT_USE_FIXED_SIZE = true;
  public static final double HOTSPOT_PERCENTAGE = 25; // [0% - 100%]
  public static final int HOTSPOT_FIXED_SIZE = 100; // fixed number of tuples

  // ----------------------------------------------------------------
  // ADDITIONAL CONFIGURATION SETTINGS
  // ----------------------------------------------------------------

  // Initial balance amount
  // We'll just make it really big so that they never run out of money
  public static final int MIN_BALANCE = 10000;
  public static final int MAX_BALANCE = 50000;

  // ----------------------------------------------------------------
  // PROCEDURE PARAMETERS
  // These amounts are from the original code
  // ----------------------------------------------------------------
  public static final double PARAM_SEND_PAYMENT_AMOUNT = 5.0d;
  public static final double PARAM_DEPOSIT_CHECKING_AMOUNT = 1.3d;
  public static final double PARAM_TRANSACT_SAVINGS_AMOUNT = 20.20d;
  public static final double PARAM_WRITE_CHECK_AMOUNT = 5.0d;

  public static final String getSavingVersion =
      "SELECT vid FROM " + TABLENAME_SAVINGS + " WHERE custid = %d";
  public static final String getCheckingVersion =
      "SELECT vid FROM " + TABLENAME_CHECKING + " WHERE custid = %d";

  public static int getHashSize(String tableName) {
    return TABLENAME_TO_HASH_SIZE.get(tableName);
  }

  public static int calculateUniqueId(HashMap<String, Integer> keys, String tableName) {
    int res = -1;
    switch (tableName) {
      case TABLENAME_ACCOUNTS:
        if (keys.containsKey("custid")) {
          res = keys.get("custid");
        } else if (keys.containsKey("name")) {
          res = keys.get("name");
        }
        break;
      case TABLENAME_CHECKING, TABLENAME_SAVINGS:
        res = keys.get("custid");
        break;
      default:
        System.out.println("Unknown relation name: " + tableName);
        break;
    }
    return res;
  }

  public static String getLatestVersion(String tableName, int validationId) {
    int latestVersion = -1;
    String finalSQL = "";
    switch (tableName) {
      case TABLENAME_ACCOUNTS:
        break;
      case TABLENAME_CHECKING:
        finalSQL = getCheckingVersion.formatted(validationId);
        break;
      case TABLENAME_SAVINGS:
        finalSQL = getSavingVersion.formatted(validationId);
        break;
      default:
        System.out.println("Unknown relation name: " + tableName);
        break;
    }

    return finalSQL;
  }
}
