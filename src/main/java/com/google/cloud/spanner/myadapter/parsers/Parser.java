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

package com.google.cloud.spanner.myadapter.parsers;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Type;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Parser is the parsing superclass, used to take wire format data types and convert them
 * specifically to desired byte types. Each subclass is assigned with a specific type.
 */
@InternalApi
public abstract class Parser<T> {
  public enum FormatCode {
    LENGTH_ENCODED,
    FIXED_LENGTH;
  }

  protected static final Charset UTF8 = StandardCharsets.UTF_8;
  protected T item;

  /**
   * Factory method for parsing given a result set, for the specified column.
   *
   * @param result The result set object containing all data retrieved.
   * @param type The type of the data to be parsed.
   * @param columnarPosition Column from the result to be parsed.
   * @return The parser object for the designated data type.
   */
  public static Parser<?> create(ResultSet result, Type type, int columnarPosition) {
    switch (type.getCode()) {
      case INT64:
        return new LongParser(result, columnarPosition);
      case STRING:
        return new StringParser(result, columnarPosition);
      case FLOAT64:
        return new Float64Parser(result, columnarPosition);
      case BYTES:
        return new BinaryParser(result, columnarPosition);
      case DATE:
        return new DateParser(result, columnarPosition);
      case TIMESTAMP:
        return new TimestampParser(result, columnarPosition);
      case NUMERIC:
        return new NumericParser(result, columnarPosition);
      case JSON:
        return new JsonParser(result, columnarPosition);

      default:
        throw new IllegalArgumentException("Illegal or unknown element type: " + type);
    }
  }

  /**
   * Parses data based on specified format code(only length encoded for now)
   *
   * @param format One of possible {@link FormatCode} types to parse data.
   * @return Byte format version of the input object.
   */
  public byte[] parse(FormatCode format) throws IOException {
    switch (format) {
      case LENGTH_ENCODED:
        return this.toLengthEncodedBytes();
      default:
        throw new IllegalArgumentException("Unknown format: " + format);
    }
  }

  public abstract byte[] toLengthEncodedBytes() throws IOException;
}
