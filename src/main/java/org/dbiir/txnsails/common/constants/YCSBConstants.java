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

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import org.dbiir.txnsails.analysis.ConditionInfo;
import org.dbiir.txnsails.common.TemplateSQL;
import org.dbiir.txnsails.execution.validation.RangeValidationLock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class YCSBConstants {

  public static final int RECORD_COUNT = 1000;

  public static final int NUM_FIELDS = 10;

  /**
   * The max size of each field in the USERTABLE. NOTE: If you increase this value here in the code,
   * then you must update all the DDL files.
   */
  public static final int MAX_FIELD_SIZE = 100; // chars

  /** How many records will each thread load. */
  public static final int THREAD_BATCH_SIZE = 50000;

  public static final int MAX_SCAN = 1000;

  public static final String TABLE_NAME = "usertable";

  public static final HashMap<String, Integer> TABLENAME_TO_INDEX = new HashMap<>(1);

  public static String getUserTableVersion =
      "SELECT vid FROM " + TABLE_NAME + " WHERE YCSB_KEY = %d";

  static {
    TABLENAME_TO_INDEX.put(TABLE_NAME, 1);
  }

  public static final HashMap<String, Integer> TABLENAME_TO_HASH_SIZE = new HashMap<>(1);

  static {
    TABLENAME_TO_HASH_SIZE.put(TABLE_NAME, 1000000);
  }

  public static int getHashSize(String tableName) {
    return TABLENAME_TO_HASH_SIZE.get(tableName);
  }

  public static int calculateUniqueId(HashMap<String, Integer> keys, String tableName) {
    int res = -1;
    if (tableName.equals(TABLE_NAME)) {
      res = keys.get("YCSB_KEY");
    } else {
      System.out.println("Unknown relation name: " + tableName);
    }
    return res;
  }

  public static RangeValidationLock getRangeValidationLock(List<Integer> keyList, TemplateSQL templateSQL) {
    HashMap<String, List<ConditionInfo>> range = templateSQL.getRanges();
    RangeValidationLock rangeValidationLock = null;
    int start = Integer.MIN_VALUE;
    int end = Integer.MAX_VALUE;

    for (Map.Entry<String, List<ConditionInfo>> entry : range.entrySet()) {
      String keyName = entry.getKey();
      List<ConditionInfo> conditions = entry.getValue();
      for (int i = 0; i < conditions.size(); i++) {
        ConditionInfo condition = conditions.get(i);
        Expression expr = condition.getExpression();
        switch (expr) {
          case GreaterThan greaterThan -> start = Math.max(start, keyList.get(i) + 1);
          case GreaterThanEquals greaterThanEquals -> start = Math.max(start, keyList.get(i));
          case MinorThan minorThan -> end = Math.max(start, keyList.get(i) - 1);
          case MinorThanEquals minorThanEquals -> end = Math.max(start, keyList.get(i));
          case null, default ->
                  System.out.println("Unknown expression type in range condition: " + expr.getClass().getSimpleName());
        }
      }
    }

    if (start <= end) {
      return new RangeValidationLock(start, end);
    } else {
      return null;
    }
  }

  public static String getLatestVersion(String tableName, int validationId) {
    String finalSQL = "";
    if (tableName.equals(TABLE_NAME)) {
      finalSQL = getUserTableVersion.formatted(validationId);
    } else {
      System.out.println("Unknown relation name: " + tableName);
    }

    return finalSQL;
  }
}
