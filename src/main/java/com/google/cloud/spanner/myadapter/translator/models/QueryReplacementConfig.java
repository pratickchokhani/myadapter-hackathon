package com.google.cloud.spanner.myadapter.translator.models;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class QueryReplacementConfig {

  private List<QueryReplacement> commands;

  public List<QueryReplacement> getCommands() {
    return commands;
  }

  public Map<String, QueryReplacement> getCompleteMatcherReplacementMap() {
    return commands.stream()
        .filter(command -> command.getMatcherSet().contains(MatcherType.COMPLETE)).collect(
            Collectors.toMap(QueryReplacement::getInputCommand, Function.identity()));
  }

  @Override
  public String toString() {
    return "QueryReplacementConfig{" +
        "commands=" + commands +
        '}';
  }
}
