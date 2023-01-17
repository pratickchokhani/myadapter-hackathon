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

package com.google.cloud.spanner.myadapter.session;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.myadapter.error.MyException;
import com.google.cloud.spanner.myadapter.utils.Converter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Represents a row in the pg_settings table. */
@InternalApi
public class SystemVariable {
  private static final int NAME_INDEX = 0;
  private static final int DATATYPE_INDEX = 1;
  private static final int VALUE_INDEX = 2;

  private String extension;
  private String name;
  private Type spannerType;
  private String value;
  private boolean readOnly;

  static ImmutableList<SystemVariable> read() {
    ImmutableList.Builder<SystemVariable> builder = ImmutableList.builder();
    try (Scanner scanner =
        new Scanner(
            Objects.requireNonNull(
                SystemVariable.class.getResourceAsStream("system_variables.txt")))) {
      while (scanner.hasNextLine()) {
        builder.add(parse(scanner.nextLine()));
      }
    }
    return builder.build();
  }

  static @Nonnull SystemVariable parse(String line) {
    String[] values = line.split("\t");
    Preconditions.checkArgument(values.length == 3);
    return new SystemVariable(
        null,
        parseString(values[NAME_INDEX]),
        Converter.typeCodeToSpannerType(parseString(values[DATATYPE_INDEX])),
        parseString(values[VALUE_INDEX]));
  }

  static String parseString(String value) {
    if ("\\N".equals(value)) {
      return "";
    }
    return value;
  }

  static String[] parseStringArray(String value) {
    if ("\\N".equals(value)) {
      return null;
    }
    Preconditions.checkArgument(value.startsWith("{") && value.endsWith("}"));
    return value.substring(1, value.length() - 1).split(",");
  }

  static Integer parseInteger(String value) {
    if ("\\N".equals(value)) {
      return null;
    }
    return Integer.valueOf(value);
  }

  SystemVariable(String extension, String name, Type variableType, String value) {
    this.extension = extension;
    this.name = name;
    this.spannerType = variableType;
    this.value = value;
  }

  /** Returns a copy of this {@link SystemVariable}. */
  SystemVariable copy() {
    return new SystemVariable(null, name, spannerType, value);
  }

  /** Converts a string to a SQL literal expression that can be used in a select statement. */
  String toSelectExpression(String value) {
    return value == null ? "null" : "'" + value + "'";
  }

  /**
   * Converts a string array to a SQL literal expression that can be used in a select statement. The
   * expression is cast to text[].
   */
  String toSelectExpression(String[] value) {
    return value == null
        ? "null::text[]"
        : "'{"
            + Arrays.stream(value)
                .map(s -> s.startsWith("\"") ? s : "\"" + s + "\"")
                .collect(Collectors.joining(", "))
            + "}'::text[]";
  }

  /** Converts an Integer to a SQL literal expression that can be used in a select statement. */
  String toSelectExpression(Integer value) {
    return value == null ? "null" : value.toString();
  }

  /** Converts a Boolean to a SQL literal expression that can be used in a select statement. */
  String toSelectExpression(Boolean value) {
    return value == null ? "null" : (value ? "'t'" : "'f'");
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return spannerType;
  }

  public String getExtension() {
    return extension;
  }

  /** Returns the value of this setting. */
  public String getValue() {
    return value;
  }

  /** Initializes the value of the setting without checking for validity. */
  void initSettingValue(String value) {
    this.value = value;
  }

  void initConnectionValue(String value) {}

  /**
   * Sets the value for this setting. Throws {@link SpannerException} if the value is not valid, or
   * if the setting is not settable.
   */
  void setValue(String value) {
    if (this.spannerType != null) {
      // Check validity of the value.
      value = checkValidValue(value);
    }
    this.value = value;
  }

  boolean isSettable() {
    return !readOnly;
  }

  private String checkValidValue(String value) {
    // TODO implement this.
    return value;
  }

  static SpannerException invalidBoolError(String key) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("parameter \"%s\" requires a Boolean value", key));
  }

  static SpannerException invalidValueError(String key, String value) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("invalid value for parameter \"%s\": \"%s\"", key, value));
  }

  static MyException invalidEnumError(String key, String value, String[] enumVals) {
    return MyException.newBuilder(
            String.format("invalid value for parameter \"%s\": \"%s\"", key, value))
        .setHints(String.format("Available values: %s.", String.join(", ", enumVals)))
        .build();
  }
}
