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
import com.google.cloud.ByteArray;
import com.google.cloud.spanner.ResultSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Parse specified type to binary (generally this is the simplest parse class, as items are
 * generally represented in binary for wire format).
 */
@InternalApi
public class BinaryParser extends Parser<ByteArray> {

  BinaryParser(ResultSet item, int position) {
    this.item = item.getBytes(position);
  }

  @Override
  public byte[] toLengthEncodedBytes() throws IOException {
    return getLengthEncodedBytes(item.toByteArray());
  }

  public static byte[] getLengthEncodedBytes(byte[] bytes) throws IOException {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    outputStream.write(LongParser.getLengthEncodedBytes(bytes.length));
    outputStream.write(bytes);
    return outputStream.toByteArray();
  }
}