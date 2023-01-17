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

package com.google.cloud.spanner.myadapter.statements;

import com.google.api.client.util.Preconditions;
import com.google.api.client.util.Strings;
import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Type.Code;
import com.google.cloud.spanner.Type.StructField;
import com.google.cloud.spanner.connection.AbstractStatementParser.ParsedStatement;
import com.google.cloud.spanner.connection.AbstractStatementParser.StatementType;
import com.google.cloud.spanner.connection.BackendConnection.QueryResult;
import com.google.cloud.spanner.connection.BackendConnection.UpdateCount;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.myadapter.parsers.Parser;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.session.SessionState.SessionVariableScope;
import com.google.cloud.spanner.myadapter.session.SystemVariable;
import com.google.cloud.spanner.myadapter.statements.SimpleParser.TableOrIndexName;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import javax.annotation.Nullable;

/** Simple parser for session management commands (SET/SHOW/RESET variable_name) */
@InternalApi
public class SessionStatementParser {
  public abstract static class SessionStatement {
    public abstract StatementResult execute(SessionState sessionState);
  }

  static class VariableColumn {
    static class Builder {
      Builder columnName(String columnName) {
        this.columnName = columnName;
        return this;
      }

      Builder variableName(String variableName) {
        this.variableName = variableName;
        return this;
      }

      Builder setGlobal() {
        this.scope = SessionVariableScope.GLOBAL;
        return this;
      }

      Builder setSystemVariable() {
        this.systemVariable = systemVariable;
        return this;
      }

      VariableColumn build() {
        return new VariableColumn(this);
      }

      String columnName;
      String variableName;
      SessionVariableScope scope = SessionVariableScope.SESSION;
      boolean systemVariable = false;
    }

    String columnName;
    String variableName;
    SessionVariableScope scope;
    boolean systemVariable;

    VariableColumn(Builder builder) {
      this.columnName = builder.columnName;
      ;
      this.variableName = builder.variableName;
      this.scope = builder.scope;
      this.systemVariable = builder.systemVariable;
    }
  }

  static class SetStatement extends SessionStatement {
    static class Builder {
      boolean systemVariable = false;
      SessionVariableScope scope = SessionVariableScope.SESSION;
      String name = null;
      String value = null;

      Builder systemVariable() {
        this.systemVariable = true;
        return this;
      }

      Builder global() {
        this.scope = SessionVariableScope.GLOBAL;
        return this;
      }

      Builder name(String name) {
        this.name = name;
        return this;
      }

      Builder value(String value) {
        this.value = value;
        return this;
      }

      SetStatement build() {
        return new SetStatement(this);
      }
    }

    boolean systemVariable;
    SessionVariableScope scope;
    String name;
    String value;

    SetStatement(Builder builder) {
      this.systemVariable = builder.systemVariable;
      this.scope = builder.scope;
      this.name = builder.name;
      this.value = builder.value;
    }

    @Override
    public StatementResult execute(SessionState sessionState) {
      sessionState.set(null, name, value, scope);
      return new UpdateCount(0L);
    }
  }

  static class SelectStatement extends SessionStatement {
    static class Builder {
      Builder addVariableColumn(VariableColumn variableColumn) {
        variableColumnList.add(variableColumn);
        return this;
      }

      ArrayList<VariableColumn> variableColumnList = new ArrayList<VariableColumn>();
    }

    SelectStatement(Builder builder) {
      this.variableColumns = builder.variableColumnList;
    }

    @Override
    public StatementResult execute(SessionState sessionState) {
      ArrayList<StructField> structFields = new ArrayList<Type.StructField>();
      Struct.Builder rowBuilder = Struct.newBuilder();
      for (VariableColumn column : variableColumns) {
        SystemVariable variable = sessionState.get(null, column.variableName, column.scope);
        System.out.println(
            "flog: Adding column '"
                + column.columnName
                + "' with value '"
                + variable.getValue()
                + "'");
        structFields.add(StructField.of(column.columnName, variable.getType()));
        rowBuilder
            .set(column.columnName)
            .to(Parser.parseToSpannerValue(variable.getValue(), variable.getType().getCode()));
      }

      Type rowType = Type.struct(structFields);
      ImmutableList<Struct> rows = ImmutableList.of(rowBuilder.build());
      Preconditions.checkNotNull(rowType);
      Preconditions.checkNotNull(rows);
      Preconditions.checkArgument(rowType.getCode() == Code.STRUCT);
      System.out.println("preconditions passed!!!!!!!");
      ResultSet resultSet = ResultSets.forRows(rowType, rows);
      return new QueryResult(resultSet);
    }

    ArrayList<VariableColumn> variableColumns;
  }

  public static @Nullable SessionStatement parse(ParsedStatement parsedStatement) {
    if (parsedStatement.getType() == StatementType.CLIENT_SIDE) {
      // This statement is handled by the Connection API.
      return null;
    }
    SimpleParser parser = new SimpleParser(parsedStatement.getSqlWithoutComments());
    if (parser.eatKeyword("set")) {
      System.out.println("It's a set statement!!!!!");
      return parseSetStatement(parser);
    }
    if (parser.eatKeyword("select")
        && !parser.peek(true, true, "FROM")
        && parser.peek(true, false, "@@")) {
      System.out.println("It's a select @@ statement!!!!!");
      return parseSelectStatement(parser);
    }

    return null;
  }

  static SetStatement parseSetStatement(SimpleParser parser) {
    SetStatement.Builder builder = new SetStatement.Builder();
    if (parser.eatToken("@@") || !parser.eatToken("@")) {
      // This is a system variable
      builder.systemVariable();
    }

    TableOrIndexName name = parser.readTableOrIndexName();
    if (name == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: " + parser.getSql() + ". Expected configuration parameter name.");
    }

    if ("global".equals(name.getUnquotedSchema())) {
      builder.global();
    }

    builder.name(name.getUnquotedName());
    if (!parser.eatToken("=")) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: " + parser.getSql() + ". Expected =.");
    }
    String value = parser.unquoteOrFoldIdentifier(parser.parseExpression());
    if (value == null) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: " + parser.getSql() + ". Expected value.");
    }
    builder.value(value);
    String remaining = parser.parseExpression();
    if (!Strings.isNullOrEmpty(remaining)) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.INVALID_ARGUMENT,
          "Invalid SET statement: "
              + parser.getSql()
              + ". Expected end of statement after "
              + value);
    }

    return builder.build();
  }

  static SelectStatement parseSelectStatement(SimpleParser parser) {
    SelectStatement.Builder builder = new SelectStatement.Builder();
    String[] columnExpressions = parser.getSql().substring(parser.getPos()).split(",");
    for (String expression : columnExpressions) {
      System.out.println("flog: Expression: " + expression);
      SimpleParser expressionParser = new SimpleParser(expression);
      VariableColumn.Builder columnBuilder = new VariableColumn.Builder();
      columnBuilder.columnName(expression.trim());
      if (expressionParser.eatToken("@@") || !expressionParser.eatToken("@")) {
        columnBuilder.setSystemVariable();
      }
      TableOrIndexName name = expressionParser.readTableOrIndexName();
      if (name == null) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Invalid SELECT statement expression: " + expressionParser.getSql());
      }

      if ("global".equals(name.getUnquotedSchema())) {
        columnBuilder.setGlobal();
      }

      columnBuilder.variableName(name.getUnquotedName());
      builder.addVariableColumn(columnBuilder.build());
    }
    return new SelectStatement(builder);
  }
}
