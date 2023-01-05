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

package com.google.cloud.spanner.myadapter.wireoutput;

import com.google.cloud.spanner.myadapter.metadata.ConnectionMetadata;
import com.google.cloud.spanner.myadapter.parsers.LongParser;
import com.google.cloud.spanner.myadapter.parsers.Parser;
import com.google.cloud.spanner.myadapter.parsers.Parser.FormatCode;
import java.io.IOException;

public class OkResponse extends WireOutput {

  public OkResponse(int currentSequenceNumber, ConnectionMetadata connectionMetadata,
      long updateCount)
      throws IOException {
    super(currentSequenceNumber, connectionMetadata);

    byte[] okIdentifier = new byte[]{0x00};
    writePayload(okIdentifier);

    byte[] affectedRows = LongParser.getLengthEncodedBytes(updateCount);
    writePayload(affectedRows);

    byte[] lastInsertId = new byte[]{0x00};
    writePayload(lastInsertId);

    byte[] serverStatus = {(byte) 2, (byte) 0};
    writePayload(serverStatus);

    byte[] warnings = {(byte) 0, (byte) 0};
    writePayload(warnings);
  }

  public OkResponse(int currentSequenceNumber, ConnectionMetadata connectionMetadata)
      throws IOException {
    this(currentSequenceNumber, connectionMetadata, 0);
  }

  @Override
  protected String getMessageName() {
    return "OkResponse";
  }

  @Override
  protected String getPayloadString() {
    return "";
  }
}
