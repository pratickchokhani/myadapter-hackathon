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
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/** Translate from wire protocol to string and vice versa. */
@InternalApi
public class StringParser extends Parser<String> {

  StringParser(ResultSet item, int position) {
    if (!item.isNull(position)) {
      this.item = item.getString(position);
    } else {
      this.item = null;
    }
  }

  public StringParser(String item) {
    this.item = item;
  }

  public byte[] toLengthEncodedBytes() throws IOException {
    return getLengthEncodedBytes(item);
  }

  public static byte[] getLengthEncodedBytes(String string) throws IOException {
    long stringLength;
    byte[] bytes;
    if (string == null) {
      stringLength = 0;
      bytes = new byte[0];
    } else {
      stringLength = string.length();
      bytes = string.getBytes();
    }
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(LongParser.getLengthEncodedBytes(stringLength));
    outputStream.write(bytes);
    return outputStream.toByteArray();
  }
}
