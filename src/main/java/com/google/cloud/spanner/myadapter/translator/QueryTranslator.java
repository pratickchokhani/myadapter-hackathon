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

import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.stream.JsonReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.simple.parser.ParseException;

public class QueryTranslator {
  private static final Logger logger = Logger.getLogger(QueryTranslator.class.getName());
  private static final Gson GSON = new Gson();

  private static Map<String, TranslatedQuery> QUERY_TRANSLATION = new HashMap<>();

  private static Set<String> QUERY_BYPASS =
      new HashSet<String>(
          Arrays.asList("show databases", "show tables", "select @@version_comment limit 1"));

  public QueryTranslator(OptionsMetadata optionsMetadata) {
    QUERY_TRANSLATION.put(sessionJDBCQueryTranslator().getSqlStatement(),
        sessionJDBCQueryTranslator());
    QUERY_TRANSLATION.put(sessionTransactionReadOnly().getSqlStatement(),
        sessionTransactionReadOnly());
    QUERY_TRANSLATION.put(sessionTransactionIsolationLevel().getSqlStatement(),
        sessionTransactionIsolationLevel());
  }

  public static boolean bypassQuery(String sql) {
    if (sql.startsWith("SET ")) {
      return true;
    }
    if (QUERY_BYPASS.contains(sql)) {
      logger.log(Level.INFO, () -> String.format("SQL query: %s bypassed.", sql));
      return true;
    }
    return false;
  }

  public static void main(String[] ar) {
    System.out.println("dekakw");
  }

  public TranslatedQuery translatedQuery(ParsedStatement parsedStatement,
      Statement originalStatement) {
    return QUERY_TRANSLATION.getOrDefault(parsedStatement.getSqlWithoutComments(),
        new TranslatedQuery(originalStatement));
  }

  private static TranslatedQuery sessionTransactionIsolationLevel() {
    String sourceQuery = "SELECT @@session.transaction_isolation";
    String outputQuery =
        "with session_state_bla_bla as (\n"
            + "SELECT  \n"
            + "'REPEATABLE-READ' AS transaction_isolation, \n"
            + ") \n"
            + "select * from session_state_bla_bla;";

    List<TranslationOverride> translationOverrides = new ArrayList<>();
    translationOverrides.add(
        new ColumnOverride(
            OverrideType.COLUMN_NAME,
            "transaction_isolation",
            "@@session.transaction_isolation"));

    return new TranslatedQuery(sourceQuery, outputQuery, translationOverrides);

  }

  private static TranslatedQuery sessionTransactionReadOnly() {
    String sourceQuery = "SELECT @@session.transaction_read_only";
    String outputQuery =
        "with session_state_bla_bla as (\n"
            + "SELECT  \n"
            + "0 AS transaction_read_only, \n"
            + ") \n"
            + "select * from session_state_bla_bla;";

    List<TranslationOverride> translationOverrides = new ArrayList<>();
    translationOverrides.add(
        new ColumnOverride(
            OverrideType.COLUMN_NAME,
            "transaction_read_only",
            "@@session.transaction_read_only"));

    return new TranslatedQuery(sourceQuery, outputQuery, translationOverrides);

  }

  private static TranslatedQuery sessionJDBCQueryTranslator() {
    String sourceQuery =
        "SELECT  @@session.auto_increment_increment AS auto_increment_increment, @@character_set_client AS character_set_client, @@character_set_connection AS character_set_connection, @@character_set_results AS character_set_results, @@character_set_server AS character_set_server, @@collation_server AS collation_server, @@collation_connection AS collation_connection, @@init_connect AS init_connect, @@interactive_timeout AS interactive_timeout, @@license AS license, @@lower_case_table_names AS lower_case_table_names, @@max_allowed_packet AS max_allowed_packet, @@net_write_timeout AS net_write_timeout, @@performance_schema AS performance_schema, @@sql_mode AS sql_mode, @@system_time_zone AS system_time_zone, @@time_zone AS time_zone, @@transaction_isolation AS transaction_isolation, @@wait_timeout AS wait_timeout";
    String outputQuery =
        "with session_state_bla_bla as (\n"
            + "SELECT  \n"
            + "1 AS auto_increment_increment, \n"
            + "'utf8mb4' AS character_set_client, \n"
            + "'utf8mb4' AS character_set_connection, \n"
            + "'utf8mb4' AS character_set_results, \n"
            + "'utf8mb4' AS character_set_server, \n"
            + "'utf8mb4_0900_ai_ci' AS collation_server,\n"
            + "'utf8mb4_general_ci' AS collation_connection, \n"
            + "CAST(NULL AS STRING) AS init_connect,\n"
            + "28800 AS interactive_timeout, \n"
            + "'GPL' AS license,\n"
            + "2 AS lower_case_table_names,\n"
            + "67108864 AS max_allowed_packet,\n"
            + "60 AS net_write_timeout, \n"
            + "1 AS performance_schema, \n"
            + "'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' AS sql_mode,\n"
            + "'IST' AS system_time_zone, \n"
            + "'SYSTEM' AS time_zone,\n"
            + "'REPEATABLE-READ' AS transaction_isolation, \n"
            + "28800 AS wait_timeout\n"
            + ") \n"
            + "select * from session_state_bla_bla;";
    return new TranslatedQuery(sourceQuery, outputQuery);
  }

  @VisibleForTesting
  public List<TranslatedQuery> parseQueryTranslatorFile(String filePath) throws IOException {
    return parse(this.getClass().getClassLoader().getResourceAsStream(filePath));
  }

  private static List<TranslatedQuery> parse(InputStream inputStream)
      throws JsonIOException, JsonSyntaxException, IOException {
    try (InputStreamReader reader = new InputStreamReader(inputStream)) {
      return GSON.fromJson(new JsonReader(reader), new TypeToken<List<TranslatedQuery>>() {
      }.getType());
    }
  }
}
