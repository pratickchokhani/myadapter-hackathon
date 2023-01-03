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

package com.google.cloud.spanner.myadapter.utils;

import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.myadapter.ProxyServer.DataFormat;
import com.google.cloud.spanner.myadapter.parsers.Parser;
import com.google.cloud.spanner.myadapter.parsers.Parser.FormatCode;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Utility class for converting between generic MySQL conversions. */
public class Converter {
  private final ResultSet resultSet;
  private final DataFormat dataFormat;

  public Converter(ResultSet resultSet, DataFormat dataFormat) {
    this.resultSet = resultSet;
    this.dataFormat = dataFormat;
  }

  public static byte[] convertResultSetRowToDataRowResponse(ResultSet resultSet)
      throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream(256);
    for (int i = 0; i < resultSet.getColumnCount(); ++i) {
      buffer.write(
          Parser.create(resultSet, resultSet.getColumnType(i), i).parse(FormatCode.LENGTH_ENCODED));
    }
    return buffer.toByteArray();
  }

  public static byte convertToMySqlCode(Type spannerType) {
    switch (spannerType.getCode()) {
      case BOOL:
        return (byte) 1;
      case INT64:
        return (byte) 3;
      case STRING:
        return (byte) 253;
      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + spannerType);
    }
  }
}
