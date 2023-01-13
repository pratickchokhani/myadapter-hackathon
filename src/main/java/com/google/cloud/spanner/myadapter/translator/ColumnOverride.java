package com.google.cloud.spanner.myadapter.translator;

public class ColumnOverride extends TranslationOverride {

  private String resultSetColumnName;
  private String outputColumnName;

  public ColumnOverride(
      OverrideType overrideType, String resultSetColumnName, String outputColumnName) {
    super(overrideType);
    this.resultSetColumnName = resultSetColumnName;
    this.outputColumnName = outputColumnName;
  }

  public String getResultSetColumnName() {
    return resultSetColumnName;
  }

  public String getOutputColumnName() {
    return outputColumnName;
  }
}
