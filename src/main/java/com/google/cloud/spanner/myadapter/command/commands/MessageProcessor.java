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

import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.session.SessionState;
import com.google.cloud.spanner.myadapter.wireinput.WireMessage;

public abstract class MessageProcessor {
  protected final ConnectionMetadata connectionMetadata;
  protected final SessionState sessionState;
  public final int UTF8_MB4 = 255;

  protected MessageProcessor(ConnectionMetadata connectionMetadata, SessionState sessionState) {
    this.connectionMetadata = connectionMetadata;
    this.sessionState = sessionState;
  }

  public abstract void processMessage(WireMessage message) throws Exception;
}
