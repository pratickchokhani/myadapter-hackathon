package com.google.cloud.spanner.myadapter.translator.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ColumnOverride {

  private String prefix;
  private String suffix;
  @JsonIgnore
  private Map<String, ColumnNameOverride> columnNameOverrides = Collections.emptyMap();

  public String getPrefix() {
    return prefix;
  }

  public String getSuffix() {
    return suffix;
  }
  public String getColumnNameOverride(String resultSetColumnName) {
    if (columnNameOverrides.containsKey(resultSetColumnName)) {
      return columnNameOverrides.get(resultSetColumnName).getOutputColumnName();
    }
    return resultSetColumnName;
  }

  public void setColumnNameOverrides(List<ColumnNameOverride> columnNameOverrides) {
    this.columnNameOverrides = columnNameOverrides.stream().collect(Collectors.toMap(
        ColumnNameOverride::getResultSetColumnName,
        Function.identity()));
  }

  @Override
  public String toString() {
    return "ColumnOverride{" +
        "prefix='" + prefix + '\'' +
        ", suffix='" + suffix + '\'' +
        ", columnNameOverrides=" + columnNameOverrides +
        '}';
  }
}
