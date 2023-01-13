package com.google.cloud.spanner.myadapter.translator.models;

public class ColumnNameOverride {

  private String resultSetColumnName;
  private String outputColumnName;

  public String getResultSetColumnName() {
    return resultSetColumnName;
  }

  public String getOutputColumnName() {
    return outputColumnName;
  }

  @Override
  public String toString() {
    return "ColumnNameOverride{" +
        "resultSetColumnName='" + resultSetColumnName + '\'' +
        ", outputColumnName='" + outputColumnName + '\'' +
        '}';
  }
}
