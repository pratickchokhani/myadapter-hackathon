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

import com.google.cloud.spanner.myadapter.wireoutput.ServerGreetingResponse;
import com.google.cloud.spanner.myadapter.wireprotocol.ClientHandshakeMessage;
import com.google.cloud.spanner.myadapter.wireprotocol.QueryMessage;
import com.google.cloud.spanner.myadapter.wireprotocol.WireMessage;
import java.io.IOException;

/**
 * Handles wire messages from the client, and initiates the correct workflow based on the current
 * protocol status and the incoming packet.
 */
public class WireProtocolHandler {
  private final ConnectionHandler connection;

  public void setProtocolStatus(ProtocolStatus protocolStatus) {
    this.protocolStatus = protocolStatus;
  }

  private volatile ProtocolStatus protocolStatus = ProtocolStatus.UNAUTHENTICATED;
  private int remainingPayloadLength;

  public int getNextMessageSequenceNumber() {
    messageSequenceNumber++;
    return messageSequenceNumber;
  }

  private int messageSequenceNumber = -1;

  public WireProtocolHandler(ConnectionHandler connection) {
    this.connection = connection;
  }

  public void run() throws Exception {
    new ServerGreetingResponse(connection).send(true);
    while (protocolStatus != ProtocolStatus.TERMINATED) {
      nextMessage().process();
    }
  }

  private WireMessage nextMessage() throws Exception {
    readMessageHeader(connection);
    switch (protocolStatus) {
      case UNAUTHENTICATED:
        System.out.println("unauthenticated message");
        return new ClientHandshakeMessage(connection, remainingPayloadLength);
      case AUTHENTICATED:
        System.out.println("authenticated message");
        return nextCommandMessage();
      default:
        throw new Exception("Illegal protocol message state");
    }
  }

  private WireMessage nextCommandMessage() throws Exception {
    int nextCommand = connection.getConnectionMetadata().getInputStream().readUnsignedByte();
    remainingPayloadLength--;
    switch (nextCommand) {
      case QueryMessage.IDENTIFIER:
        System.out.println("query received");
        return new QueryMessage(connection, remainingPayloadLength);
      default:
        throw new Exception("Unknown command");
    }
  }

  public void readMessageHeader(ConnectionHandler connection) throws IOException {
    this.remainingPayloadLength =
        connection.getConnectionMetadata().getInputStream().readUnsignedByte()
            + (connection.getConnectionMetadata().getInputStream().readUnsignedByte() << 8)
            + (connection.getConnectionMetadata().getInputStream().readUnsignedByte() << 16);
    this.messageSequenceNumber =
        connection.getConnectionMetadata().getInputStream().readUnsignedByte();
  }

  /** Status of a {@link WireProtocolHandler} */
  public enum ProtocolStatus {
    UNAUTHENTICATED,
    AUTHENTICATED,
    TERMINATED,
  }
}
