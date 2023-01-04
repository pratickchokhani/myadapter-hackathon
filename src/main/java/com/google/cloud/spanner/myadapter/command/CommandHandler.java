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

package com.google.cloud.spanner.myadapter.command;

import com.google.cloud.spanner.connection.BackendConnection;
import com.google.cloud.spanner.myadapter.command.commands.ClientHandShakeMessageProcessor;
import com.google.cloud.spanner.myadapter.command.commands.QueryMessageProcessor;
import com.google.cloud.spanner.myadapter.command.commands.ServerGreetingsMessage;
import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.session.ProtocolStatus;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.wireinput.ClientHandshakeMessage;
import com.google.cloud.spanner.myadapter.wireinput.QueryMessage;
import com.google.cloud.spanner.myadapter.wireinput.ServerHandshakeMessage;

public class CommandHandler {

  private final ConnectionMetadata connectionMetadata;
  private final SessionState sessionState;
  private final BackendConnection backendConnection;

  private final ServerGreetingsMessage serverGreetingsMessage;
  private final ClientHandShakeMessageProcessor clientHandShakeMessageProcessor;
  private final QueryMessageProcessor queryMessageProcessor;

  public CommandHandler(
      ConnectionMetadata connectionMetadata,
      SessionState sessionState,
      BackendConnection backendConnection) {
    this.connectionMetadata = connectionMetadata;
    this.sessionState = sessionState;
    this.backendConnection = backendConnection;

    this.serverGreetingsMessage = new ServerGreetingsMessage(connectionMetadata, sessionState);
    this.clientHandShakeMessageProcessor =
        new ClientHandShakeMessageProcessor(connectionMetadata, sessionState);
    this.queryMessageProcessor =
        new QueryMessageProcessor(connectionMetadata, sessionState, backendConnection);
  }

  public void processMessage(ServerHandshakeMessage serverHandshakeMessage) throws Exception {
    serverGreetingsMessage.processMessage(serverHandshakeMessage);
    sessionState.setProtocolStatus(ProtocolStatus.SERVER_GREETINGS_SENT);
  }

  public void processMessage(ClientHandshakeMessage clientHandshakeMessage) throws Exception {
    clientHandShakeMessageProcessor.processMessage(clientHandshakeMessage);
    sessionState.setProtocolStatus(ProtocolStatus.AUTHENTICATED);
  }

  public void processMessage(QueryMessage queryMessage) throws Exception {
    queryMessageProcessor.processMessage(queryMessage);
  }
}