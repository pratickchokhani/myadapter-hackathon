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

package com.google.cloud.spanner.parser;

import com.google.cloud.spanner.pgadapter.metadata.ConnectionMetadata;
import com.google.common.primitives.Bytes;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Parser {

  static byte sequenceId = 0;

  public static void consumeInputStream(ConnectionMetadata connectionMetadata)
      throws IOException, InterruptedException {
    int packetLengthSequence = connectionMetadata.getInputStream().readInt();
    byte[] bytes = Ints.toByteArray(packetLengthSequence);
    for (int i = 0; i < bytes.length / 2; i++) {
      byte temp = bytes[i];
      bytes[i] = bytes[bytes.length - i - 1];
      bytes[bytes.length - i - 1] = temp;
    }
    sequenceId = bytes[0];
    bytes[0] = (byte) 0;
    int packetLength = ByteBuffer.wrap(bytes).getInt();
    ArrayList<Byte> packetData = new ArrayList<>(packetLength);
    for (int i = 0; i < packetLength; i++) {
      packetData.add(connectionMetadata.getInputStream().readByte());
    }
    packetData.stream().forEach(b -> System.out.print(String.format("%02x", b) + " "));
    System.out.println();
    System.out.println("reached");
  }

  public static void parse(ConnectionMetadata connectionMetadata) throws IOException {
    int length = connectionMetadata.getInputStream().readInt();
  }

  public static byte[] createAuthSwitchRequest() {
    ArrayList<Byte> outputBytes = new ArrayList<>();
    byte protocol = (byte) 0xFE;
    outputBytes.add(protocol);

    outputBytes.addAll(Bytes.asList("mysql_native_password".getBytes(StandardCharsets.UTF_8)));
    outputBytes.add((byte) 0);
    outputBytes.addAll(Bytes.asList("lelelejeklwajlkdewjl".getBytes(StandardCharsets.UTF_8)));
    outputBytes.add((byte) 0);

    int packetSize = outputBytes.size();
    System.out.println("Packet size: " + outputBytes.size());
    List<Byte> pack = Bytes.asList(Ints.toByteArray(packetSize));
    Collections.reverse(pack);
    pack.set(3, (byte) 2);
    outputBytes.addAll(0, pack);

    System.out.println("Auth: " + new String(Bytes.toArray(outputBytes)));

    return Bytes.toArray(outputBytes);
  }

  public static byte[] createResponseOk() {
    ArrayList<Byte> outputBytes = new ArrayList<>();
    byte protocol = (byte) 0x00;
    outputBytes.add(protocol);

    outputBytes.add((byte) 0);
    outputBytes.add((byte) 0);
    byte[] serverStatus = {(byte) 2, (byte) 0};
    outputBytes.addAll(Bytes.asList(serverStatus));
    outputBytes.add((byte) 0);
    outputBytes.add((byte) 0);

    int packetSize = outputBytes.size();
    System.out.println("Packet size: " + outputBytes.size());
    List<Byte> pack = Bytes.asList(Ints.toByteArray(packetSize));
    Collections.reverse(pack);
    pack.set(3, (byte) 4);
    outputBytes.addAll(0, pack);

    System.out.println("Auth: " + new String(Bytes.toArray(outputBytes)));

    return Bytes.toArray(outputBytes);
  }

  public static byte[] createServerGreeting() {
    ArrayList<Byte> outputBytes = new ArrayList<>();
    byte protocol = 10;
    outputBytes.add(protocol);

    outputBytes.addAll(Bytes.asList("8.0.31\0".getBytes(StandardCharsets.UTF_8)));
    int thread = 0;
    outputBytes.addAll(Bytes.asList(Ints.toByteArray(thread)));

    byte[] salt = new byte[8];
    salt[7] = (byte) 255;
    outputBytes.addAll(Bytes.asList(salt));
    outputBytes.add((byte) 0);

    //
    byte[] serverCapabilities = {(byte) 255, (byte) 255};
    outputBytes.addAll(Bytes.asList(serverCapabilities));
    outputBytes.add((byte) 255);

    byte[] serverStatus = {(byte) 2, (byte) 0};
    outputBytes.addAll(Bytes.asList(serverStatus));

    byte[] eServerCapabilities = {(byte) 255, (byte) 223};
    outputBytes.addAll(Bytes.asList(eServerCapabilities));

    outputBytes.add((byte) 21);

    outputBytes.addAll(Bytes.asList(new byte[10]));

    byte[] eSalt = new byte[13];
    eSalt[7] = (byte) 255;
    outputBytes.addAll(Bytes.asList(eSalt));

    byte[] authPlugin = "caching_sha2_password".getBytes(StandardCharsets.UTF_8);
    outputBytes.addAll(Bytes.asList(authPlugin));

    outputBytes.add((byte) 0);

    int packetSize = outputBytes.size();
    System.out.println("Packet size: " + outputBytes.size());
    List<Byte> pack = Bytes.asList(Ints.toByteArray(packetSize));
    Collections.reverse(pack);
    outputBytes.addAll(0, pack);

    System.out.println(new String(Bytes.toArray(outputBytes)));

    return Bytes.toArray(outputBytes);
  }
}
