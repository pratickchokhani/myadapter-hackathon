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

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.myadapter.ConnectionHandler;
import com.google.cloud.spanner.myadapter.parsers.IntegerParser;
import com.google.cloud.spanner.myadapter.parsers.StringParser;
import com.google.cloud.spanner.myadapter.utils.Converter;
import java.io.IOException;

public class ColumnDefinitionResponse extends WireOutput {
  public ColumnDefinitionResponse(
      ConnectionHandler connection, ResultSet resultSet, int columnIndex) throws IOException {
    super(connection);

    String catalog = "def"; // This is the only supported catalog in MySQL right now.
    writePayload(StringParser.getLengthEncodedBytes(catalog));
    String schemaName = "schemaName";
    writePayload(StringParser.getLengthEncodedBytes(schemaName));
    String table = "tableName";
    writePayload(StringParser.getLengthEncodedBytes(table));
    String originalTable = "oTableName";
    writePayload(StringParser.getLengthEncodedBytes(originalTable));
    String columnName = "columnName";
    writePayload(StringParser.getLengthEncodedBytes(columnName));
    String originalColumnName = "originalColumnName";
    writePayload(StringParser.getLengthEncodedBytes(originalColumnName));
    int fixedLengthFieldsLength = 12;
    writePayload(new byte[] {(byte) fixedLengthFieldsLength});
    byte[] charSet = new byte[] {(byte) 0xff, (byte) 0x00}; // binary charset
    writePayload(charSet);
    int maxColumnLength = 20;
    writePayload(IntegerParser.binaryParse(maxColumnLength));
    byte[] columnType =
        new byte[] {Converter.convertToMySqlCode(resultSet.getColumnType(columnIndex))};
    writePayload(columnType);
    byte[] columnDefinitionFlags = new byte[] {(byte) 0x00, (byte) 0x00};
    writePayload(columnDefinitionFlags);
    byte[] decimals = new byte[] {(byte) 0x00};
    writePayload(decimals);
    byte[] iDontKnowWhatTheseBytesMean = new byte[] {(byte) 0x00, (byte) 0x00};
    writePayload(iDontKnowWhatTheseBytesMean);
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
