package com.google.cloud.spanner.myadapter.translator.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.cloud.spanner.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class QueryReplacement {

  private String inputCommand;
  @JsonIgnore
  private Statement outputQuery;
  @JsonIgnore
  private Set<MatcherType> matcherSet = Collections.emptySet();
  private QueryAction action;
  private OverrideOperation columnOverrideType = OverrideOperation.NOT_APPLICABLE;
  private ColumnOverride columnOverride;

  public QueryReplacement(Statement sourceStatement) {
    this.inputCommand = sourceStatement.getSql();
    this.outputQuery = sourceStatement;
  }

  public QueryReplacement() {
  }

  public String getInputCommand() {
    return inputCommand;
  }

  public void setOutputCommand(String outputCommand) {
    this.outputQuery = Statement.of(outputCommand);
  }

  public Set<MatcherType> getMatcherSet() {
    return matcherSet;
  }

  public void setMatcherArray(List<MatcherType> matcherArray) {
    this.matcherSet = new HashSet<>(matcherArray);
  }

  public void setAction(QueryAction action) {
    this.action = action;
  }

  public QueryAction getAction() {
    return action;
  }

  public void setColumnOverrideType(
      OverrideOperation columnOverrideType) {
    this.columnOverrideType = columnOverrideType;
  }

  public void setColumnOverride(ColumnOverride columnOverride) {
    this.columnOverride = columnOverride;
  }

  public Statement getOutputQuery() {
    return outputQuery;
  }

  public String overrideColumn(String resultSetColumnName) {
    switch (columnOverrideType) {
      case NOT_APPLICABLE:
        return resultSetColumnName;
      case PREFIX:
        return columnOverride.getPrefix().concat(resultSetColumnName);
      case SUFFIX:
        return resultSetColumnName.concat(columnOverride.getSuffix());
      case NAME_OVERRIDE:
        return columnOverride.getColumnNameOverride(resultSetColumnName);
      default:
        return resultSetColumnName;
    }
  }

  @Override
  public String toString() {
    return "QueryReplacement{" +
        "inputCommand='" + inputCommand + '\'' +
        ", outputQuery=" + outputQuery +
        ", matcherSet=" + matcherSet +
        ", action=" + action +
        ", columnOverrideType=" + columnOverrideType +
        ", columnOverride=" + columnOverride +
        '}';
  }
}
