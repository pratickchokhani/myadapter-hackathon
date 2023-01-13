package com.google.cloud.spanner.myadapter.translator;

import com.google.cloud.spanner.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TranslatedQuery {

  private String sqlStatement;
  private Statement outputQuery;
  private List<TranslationOverride> translationOverrides = Collections.emptyList();
  private Map<String, ColumnOverride> columnOverrideMap = Collections.emptyMap();

  public TranslatedQuery(
      String sqlStatement, String outputQuery, List<TranslationOverride> translationOverrides) {
    this.sqlStatement = sqlStatement;
    this.outputQuery = Statement.of(outputQuery);
    this.translationOverrides = translationOverrides;
    this.columnOverrideMap = translationOverrides.stream()
        .filter(override -> override.getOverrideType() == OverrideType.COLUMN_NAME)
        .map(override -> (ColumnOverride) override)
        .collect(Collectors.toMap(ColumnOverride::getResultSetColumnName, Function.identity()));
  }

  public TranslatedQuery(String sqlStatement, String outputQuery) {
    this(sqlStatement, outputQuery, Collections.emptyList());
  }

  public TranslatedQuery(Statement sourceStatement) {
    this.sqlStatement = sourceStatement.getSql();
    this.outputQuery = sourceStatement;
  }

  public String getSqlStatement() {
    return sqlStatement;
  }

  public Statement getOutputQuery() {
    return outputQuery;
  }

  public String overrideColumn(String resultSetColumn) {
    if (columnOverrideMap.isEmpty() || !columnOverrideMap.containsKey(resultSetColumn)) {
      return resultSetColumn;
    }
    return columnOverrideMap.get(resultSetColumn).getOutputColumnName();
  }

  public List<TranslationOverride> getTranslationOverrides() {
    return translationOverrides;
  }
}
