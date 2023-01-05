// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.cloud.spanner.myadapter.translator;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryTranslator {
  private static final Logger logger = Logger.getLogger(QueryTranslator.class.getName());

  private static Set<String> QUERY_TRANSLATORS =
      new HashSet<String>(
          Arrays.asList("show databases", "show tables", "select @@version_comment limit 1"));

  public static boolean bypassQuery(String sql) {
    if (QUERY_TRANSLATORS.contains(sql)) {
      logger.log(Level.INFO, () -> String.format("SQL query: %s bypassed.", sql));
      return true;
    }
    return false;
  }
}
