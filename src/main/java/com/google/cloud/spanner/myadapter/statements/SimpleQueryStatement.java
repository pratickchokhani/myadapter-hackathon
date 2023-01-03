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

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.myadapter.ConnectionHandler;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.myadapter.wireoutput.ColumnCountResponse;
import com.google.cloud.spanner.myadapter.wireoutput.ColumnDefinitionResponse;
import com.google.cloud.spanner.myadapter.wireoutput.EofResponse;
import com.google.cloud.spanner.myadapter.wireoutput.OkResponse;
import com.google.cloud.spanner.myadapter.wireoutput.RowResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;

/**
 * Class that represents a simple query protocol statement. This statement can contain multiple
 * semicolon separated SQL statements, but only a single statement is supported right now. The
 * statement is executed in Spanner autocommit mode.
 */
@InternalApi
public class SimpleQueryStatement {
  private final ConnectionHandler connectionHandler;
  private final OptionsMetadata options;
  private final ImmutableList<Statement> statements;
  private int rowsSent = 0;

  public SimpleQueryStatement(
      OptionsMetadata options, Statement originalStatement, ConnectionHandler connectionHandler) {
    this.connectionHandler = connectionHandler;
    this.options = options;
    this.statements = parseStatements(originalStatement);
  }

  public void execute() throws Exception {
    for (Statement originalStatement : this.statements) {
      System.out.println("flog: statement: " + originalStatement.getSql());
      try {
        System.out.println("flog: sending executing");

        StatementResult statementResult =
            connectionHandler.getSpannerConnection().execute(originalStatement);
        ResultSet resultSet = statementResult.getResultSet();
        while (resultSet.next()) {
          sendResultSetRow(resultSet);
        }

        new EofResponse(connectionHandler).send(true);
      } catch (Exception ignore) {
        System.out.println("flog: got exception" + ignore.toString());
        new OkResponse(connectionHandler).send(true);
        // Stop further processing if an exception occurs.
        break;
      }
    }
  }

  void sendResultSetRow(ResultSet resultSet) throws Exception {
    System.out.println("row " + resultSet.getColumnType(0));
    if (rowsSent == 0) {
      sendColumnDefinitions(resultSet);
    }
    new RowResponse(connectionHandler, resultSet).send();
    rowsSent++;
  }

  private void sendColumnDefinitions(ResultSet resultSet) throws IOException {
    new ColumnCountResponse(connectionHandler, resultSet.getColumnCount()).send();
    for (int i = 0; i < resultSet.getColumnCount(); ++i) {
      new ColumnDefinitionResponse(connectionHandler, resultSet, i).send();
    }
  }

  protected static ImmutableList<Statement> parseStatements(Statement statement) {
    Preconditions.checkNotNull(statement);
    ImmutableList.Builder<Statement> builder = ImmutableList.builder();
    SimpleParser parser = new SimpleParser(statement.getSql());
    for (String sql : parser.splitStatements()) {
      builder.add(Statement.of(sql));
    }
    return builder.build();
  }
}
