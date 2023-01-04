// Copyright 2020 Google LLC
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

package com.google.cloud.spanner.myadapter.command.commands;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.connection.BackendConnection;
import com.google.cloud.spanner.connection.StatementResult;
import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.statements.SimpleParser;
import com.google.cloud.spanner.myadapter.wireinput.QueryMessage;
import com.google.cloud.spanner.myadapter.wireinput.WireMessage;
import com.google.cloud.spanner.myadapter.wireoutput.ColumnCountResponse;
import com.google.cloud.spanner.myadapter.wireoutput.ColumnDefinitionResponse;
import com.google.cloud.spanner.myadapter.wireoutput.EofResponse;
import com.google.cloud.spanner.myadapter.wireoutput.OkResponse;
import com.google.cloud.spanner.myadapter.wireoutput.RowResponse;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;

public class QueryMessageProcessor extends MessageProcessor {

  private int currentSequenceNumber = -1;
  private final BackendConnection backendConnection;

  public QueryMessageProcessor(
      ConnectionMetadata connectionMetadata,
      SessionState sessionState,
      BackendConnection backendConnection) {
    super(connectionMetadata, sessionState);
    this.backendConnection = backendConnection;
  }

  @Override
  public void processMessage(WireMessage message) throws Exception {
    QueryMessage queryMessage = (QueryMessage) message;
    ImmutableList<Statement> statements = parseStatements(queryMessage.getOriginalStatement());
    currentSequenceNumber = queryMessage.getMessageSequenceNumber();

    for (Statement originalStatement : statements) {
      System.out.println("flog: statement: " + originalStatement.getSql());
      try {
        System.out.println("flog: sending executing");

        StatementResult statementResult =
            backendConnection.getSpannerConnection().execute(originalStatement);
        ResultSet resultSet = statementResult.getResultSet();
        int rowSent = 0;
        while (resultSet.next()) {
          rowSent = sendResultSetRow(resultSet, rowSent);
        }

        currentSequenceNumber =
            new EofResponse(currentSequenceNumber, connectionMetadata).send(true);
      } catch (Exception ignore) {
        System.out.println("flog: got exception" + ignore.toString());
        new OkResponse(currentSequenceNumber, connectionMetadata).send(true);
        // Stop further processing if an exception occurs.
        break;
      }
    }
  }

  private int sendResultSetRow(ResultSet resultSet, int rowsSent) throws Exception {
    System.out.println("row " + resultSet.getColumnType(0));
    if (rowsSent == 0) {
      sendColumnDefinitions(resultSet);
    }
    currentSequenceNumber =
        new RowResponse(currentSequenceNumber, connectionMetadata, resultSet).send();
    rowsSent++;
    return rowsSent;
  }

  private void sendColumnDefinitions(ResultSet resultSet) throws IOException {
    currentSequenceNumber =
        new ColumnCountResponse(
                currentSequenceNumber, connectionMetadata, resultSet.getColumnCount())
            .send();
    for (int i = 0; i < resultSet.getColumnCount(); ++i) {
      currentSequenceNumber =
          new ColumnDefinitionResponse(currentSequenceNumber, connectionMetadata, resultSet, i)
              .send();
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
