package com.google.cloud.spanner.myadapter.translator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryTranslator {
  private static final Logger logger = Logger.getLogger(QueryTranslator.class.getName());

  private static Set<String> QUERY_TRANSLATORS = new HashSet<String>(
      Arrays.asList("show databases", "show tables", "select @@version_comment limit 1"));

  public static boolean bypassQuery(String sql) {
    if (QUERY_TRANSLATORS.contains(sql)) {
      logger.log(Level.INFO, () -> String.format("SQL query: %s bypassed.", sql));
      return true;
    }
    return false;
  }
}
