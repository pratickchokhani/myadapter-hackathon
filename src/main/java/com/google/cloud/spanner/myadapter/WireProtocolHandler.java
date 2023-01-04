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

package com.google.cloud.spanner.myadapter;

import com.google.cloud.spanner.connection.BackendConnection;
import com.google.cloud.spanner.myadapter.command.CommandHandler;
import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.session.ProtocolStatus;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.wireinput.ClientHandshakeMessage;
import com.google.cloud.spanner.myadapter.wireinput.HeaderMessage;
import com.google.cloud.spanner.myadapter.wireinput.QueryMessage;
import com.google.cloud.spanner.myadapter.wireinput.ServerHandshakeMessage;
import java.io.IOException;

/**
 * Handles wire messages from the client, and initiates the correct workflow based on the current
 * protocol status and the incoming packet.
 */
public class WireProtocolHandler {

  private final ConnectionMetadata connectionMetadata;
  private final CommandHandler commandHandler;
  private final SessionState sessionState;
  private final BackendConnection backendConnection;

  public WireProtocolHandler(
      ConnectionMetadata connectionMetadata,
      SessionState sessionState,
      BackendConnection backendConnection) {
    this.backendConnection = backendConnection;
    this.commandHandler = new CommandHandler(connectionMetadata, sessionState, backendConnection);
    this.connectionMetadata = connectionMetadata;
    this.sessionState = sessionState;
  }

  public void run() throws Exception {
    commandHandler.processMessage(ServerHandshakeMessage.getInstance());
    while (sessionState.getProtocolStatus() != ProtocolStatus.TERMINATED) {
      processNextMessage();
      if (sessionState.getProtocolStatus() == ProtocolStatus.AUTHENTICATED) {
        System.out.println("connecting to spanner");
        backendConnection.connectToSpanner("test", null);
        sessionState.setProtocolStatus(ProtocolStatus.QUERY_WAIT);
      }
    }
  }

  private void processNextMessage() throws Exception {
    HeaderMessage headerMessage = parseHeaderMessage(connectionMetadata);
    switch (sessionState.getProtocolStatus()) {
      case SERVER_GREETINGS_SENT:
        System.out.println("unauthenticated message");
        ClientHandshakeMessage clientHandshakeMessage = new ClientHandshakeMessage(headerMessage);
        commandHandler.processMessage(clientHandshakeMessage);
        break;
      case QUERY_WAIT:
        System.out.println("authenticated message");
        nextCommandMessage(headerMessage);
        break;
      default:
        throw new Exception("Illegal protocol message state");
    }
  }

  private void nextCommandMessage(HeaderMessage headerMessage) throws Exception {
    int nextCommand = headerMessage.getBufferedInputStream().read();
    switch (nextCommand) {
      case QueryMessage.IDENTIFIER:
        System.out.println("query received");
        QueryMessage queryMessage = new QueryMessage(headerMessage);
        commandHandler.processMessage(queryMessage);
      default:
        throw new Exception("Unknown command");
    }
  }

  private HeaderMessage parseHeaderMessage(ConnectionMetadata connectionMetadata)
      throws IOException {
    return HeaderMessage.create(connectionMetadata.getInputStream());
  }
}