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

import com.google.api.core.InternalApi;
import com.google.auth.Credentials;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.DatabaseNotFoundException;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.InstanceNotFoundException;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.connection.Connection;
import com.google.cloud.spanner.connection.ConnectionOptions;
import com.google.cloud.spanner.connection.ConnectionOptionsHelper;
import com.google.cloud.spanner.myadapter.WireProtocolHandler.ProtocolStatus;
import com.google.cloud.spanner.myadapter.error.MyException;
import com.google.cloud.spanner.myadapter.error.SQLState;
import com.google.cloud.spanner.myadapter.error.Severity;
import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.myadapter.wireprotocol.WireMessage;
import com.google.common.annotations.VisibleForTesting;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;
import javax.net.ssl.SSLSocketFactory;

/**
 * Handles a connection from a client to Spanner. This {@link ConnectionHandler} uses {@link
 * WireMessage} to receive and send all messages from and to the client.
 *
 * <p>Each {@link ConnectionHandler} is also a {@link Thread}. Although a TCP connection does not
 * necessarily need to have its own thread, this makes the implementation more straightforward.
 */
@InternalApi
public class ConnectionHandler extends Thread {

  private static final Logger logger = Logger.getLogger(ConnectionHandler.class.getName());
  private static final AtomicLong CONNECTION_HANDLER_ID_GENERATOR = new AtomicLong(0L);
  private static final String CHANNEL_PROVIDER_PROPERTY = "CHANNEL_PROVIDER";

  private final ProxyServer server;
  private Socket socket;
  private static final Map<Integer, ConnectionHandler> CONNECTION_HANDLERS =
      new ConcurrentHashMap<>();
  private final int connectionId;
  // Separate the following from the threat ID generator, since PG connection IDs are maximum
  //  32 bytes, and shouldn't be incremented on failed startups.
  private static final AtomicInteger incrementingConnectionId = new AtomicInteger(0);
  private ConnectionMetadata connectionMetadata;
  private Connection spannerConnection;
  private DatabaseId databaseId;
  private int sequenceNumber;

  public WireProtocolHandler getWireHandler() {
    return wireHandler;
  }

  private WireProtocolHandler wireHandler;

  ConnectionHandler(ProxyServer server, Socket socket) {
    this(server, socket, null);
  }

  /** Constructor only for testing. */
  @VisibleForTesting
  ConnectionHandler(ProxyServer server, Socket socket, Connection spannerConnection) {
    super("ConnectionHandler-" + CONNECTION_HANDLER_ID_GENERATOR.incrementAndGet());
    this.server = server;
    this.socket = socket;
    this.connectionId = incrementingConnectionId.incrementAndGet();
    CONNECTION_HANDLERS.put(this.connectionId, this);
    setDaemon(true);
    logger.log(
        Level.INFO,
        () ->
            String.format(
                "Connection handler with ID %s created for client %s",
                getName(), socket.getInetAddress().getHostAddress()));
    this.spannerConnection = spannerConnection;
    this.wireHandler = new WireProtocolHandler(this);
  }

  void createSSLSocket() throws IOException {
    this.socket =
        ((SSLSocketFactory) SSLSocketFactory.getDefault()).createSocket(socket, null, true);
  }

  @InternalApi
  public void connectToSpanner(String database, @Nullable Credentials credentials) {
    OptionsMetadata options = getServer().getOptions();
    String uri =
        options.hasDefaultConnectionUrl()
            ? options.getDefaultConnectionUrl()
            : options.buildConnectionURL(database);
    if (uri.startsWith("jdbc:")) {
      uri = uri.substring("jdbc:".length());
    }
    uri = appendPropertiesToUrl(uri, getServer().getProperties());
    if (System.getProperty(CHANNEL_PROVIDER_PROPERTY) != null) {
      uri =
          uri
              + ";"
              + ConnectionOptions.CHANNEL_PROVIDER_PROPERTY_NAME
              + "="
              + System.getProperty(CHANNEL_PROVIDER_PROPERTY);
      // This forces the connection to use NoCredentials.
      uri = uri + ";usePlainText=true";
      try {
        Class.forName(System.getProperty(CHANNEL_PROVIDER_PROPERTY));
      } catch (ClassNotFoundException e) {
        throw SpannerExceptionFactory.newSpannerException(
            ErrorCode.INVALID_ARGUMENT,
            "Unknown or invalid channel provider: "
                + System.getProperty(CHANNEL_PROVIDER_PROPERTY));
      }
    }
    ConnectionOptions.Builder connectionOptionsBuilder = ConnectionOptions.newBuilder().setUri(uri);
    if (credentials != null) {
      connectionOptionsBuilder =
          ConnectionOptionsHelper.setCredentials(connectionOptionsBuilder, credentials);
    }
    ConnectionOptions connectionOptions = connectionOptionsBuilder.build();
    Connection spannerConnection = connectionOptions.getConnection();
    try {
      // Note: Calling getDialect() will cause a SpannerException if the connection itself is
      // invalid, for example as a result of the credentials being wrong.
      if (spannerConnection.getDialect() != Dialect.GOOGLE_STANDARD_SQL) {
        spannerConnection.close();
        throw MyException.newBuilder(
                String.format(
                    "The database uses dialect %s. Currently PGAdapter only supports connections to PostgreSQL dialect databases. "
                        + "These can be created using https://cloud.google.com/spanner/docs/quickstart-console#postgresql",
                    spannerConnection.getDialect()))
            .setSeverity(Severity.FATAL)
            .setSQLState(SQLState.SQLServerRejectedEstablishmentOfSQLConnection)
            .build();
      }
    } catch (InstanceNotFoundException | DatabaseNotFoundException notFoundException) {
      SpannerException exceptionToThrow = notFoundException;
      spannerConnection.close();
      throw exceptionToThrow;
    } catch (SpannerException e) {
      spannerConnection.close();
      throw e;
    }
    this.spannerConnection = spannerConnection;
    this.databaseId = connectionOptions.getDatabaseId();
  }

  private String appendPropertiesToUrl(String url, Properties info) {
    if (info == null || info.isEmpty()) {
      return url;
    }
    StringBuilder result = new StringBuilder(url);
    for (Entry<Object, Object> entry : info.entrySet()) {
      if (entry.getValue() != null && !"".equals(entry.getValue())) {
        result.append(";").append(entry.getKey()).append("=").append(entry.getValue());
      }
    }
    return result.toString();
  }

  /**
   * Simple runner starts a loop which keeps taking inbound messages, processing them, sending them
   * to Spanner, getting a result, processing that result, and replying to the client (in that
   * order). Also instantiates input and output streams from the client and handles auth.
   */
  @Override
  public void run() {
    logger.log(
        Level.INFO,
        () ->
            String.format(
                "Connection handler with ID %s starting for client %s",
                getName(), socket.getInetAddress().getHostAddress()));
    runConnection();
  }

  public void setSequenceNumber(int sequenceNumber) {
    this.sequenceNumber = sequenceNumber;
  }

  public int getSequenceNumber() {
    return sequenceNumber;
  }

  /**
   * Starts listening for incoming messages on the network socket. Returns RESTART_WITH_SSL if the
   * listening process should be restarted with SSL.
   */
  private void runConnection() {
    try (ConnectionMetadata connectionMetadata =
        new ConnectionMetadata(this.socket.getInputStream(), this.socket.getOutputStream())) {
      this.connectionMetadata = connectionMetadata;

      try {
        wireHandler.run();
      } catch (MyException myException) {
        this.handleError(myException);
      } catch (Exception exception) {
        this.handleError(
            MyException.newBuilder(exception)
                .setSeverity(Severity.FATAL)
                .setSQLState(SQLState.InternalError)
                .build());
      }
    } catch (Exception e) {
      logger.log(
          Level.WARNING,
          e,
          () ->
              String.format(
                  "Exception on connection handler with ID %s for client %s: %s",
                  getName(), socket.getInetAddress().getHostAddress(), e));
    }
  }

  /** Called when a Terminate message is received. This closes this {@link ConnectionHandler}. */
  public void handleTerminate() {
    synchronized (this) {
      if (this.spannerConnection != null) {
        this.spannerConnection.close();
      }
      this.wireHandler.setProtocolStatus(ProtocolStatus.TERMINATED);
      CONNECTION_HANDLERS.remove(this.connectionId);
    }
  }

  /**
   * Terminates this connection at the request of the server. This is called if the server is
   * shutting down while the connection is still active.
   */
  void terminate() {
    handleTerminate();
    try {
      if (!socket.isClosed()) {
        socket.close();
      }
    } catch (IOException exception) {
      logger.log(
          Level.WARNING,
          exception,
          () ->
              String.format(
                  "Failed to close connection handler with ID %s: %s", getName(), exception));
    }
  }

  /**
   * Takes an Exception Object and relates its results to the user within the client.
   *
   * @param exception The exception to be related.
   * @throws IOException if there is some issue in the sending of the error messages.
   */
  void handleError(MyException exception) throws Exception {
    logger.log(
        Level.WARNING,
        exception,
        () ->
            String.format("Exception on connection handler with ID %s: %s", getName(), exception));
    DataOutputStream output = getConnectionMetadata().getOutputStream();
  }

  public ProxyServer getServer() {
    return this.server;
  }

  public Connection getSpannerConnection() {
    return this.spannerConnection;
  }

  public DatabaseId getDatabaseId() {
    return this.databaseId;
  }

  public ConnectionMetadata getConnectionMetadata() {
    return connectionMetadata;
  }
}
