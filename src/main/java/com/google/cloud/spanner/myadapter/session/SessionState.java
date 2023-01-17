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

package com.google.cloud.spanner.myadapter.session;

import com.google.api.core.InternalApi;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.connection.BackendConnection;
import com.google.cloud.spanner.myadapter.command.commands.QueryMessageProcessor;
import com.google.cloud.spanner.myadapter.metadata.OptionsMetadata;
import com.google.cloud.spanner.myadapter.parsers.BooleanParser;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/** {@link SessionState} contains all session variables for a connection. */
@InternalApi
public class SessionState {
  private volatile ProtocolStatus protocolStatus;
  private final BackendConnection backendConnection;

  static final Map<String, SystemVariable> GLOBAL_SETTINGS = new HashMap<>();
  private static final Logger logger = Logger.getLogger(QueryMessageProcessor.class.getName());

  public enum SessionVariableScope {
    SESSION,
    GLOBAL
  }

  static {
    for (SystemVariable setting : SystemVariable.read()) {
      GLOBAL_SETTINGS.put(toKey(null, setting.getName()), setting);
    }
  }

  private final Map<String, SystemVariable> settings;

  public SessionState(OptionsMetadata options, BackendConnection backendConnection) {
    this(ImmutableMap.of(), options, backendConnection);
  }

  @VisibleForTesting
  SessionState(
      Map<String, SystemVariable> extraServerSettings,
      OptionsMetadata options,
      BackendConnection backendConnection) {
    this.protocolStatus = ProtocolStatus.CONNECTION_INITIATED;
    this.backendConnection = backendConnection;

    Preconditions.checkNotNull(extraServerSettings);
    Preconditions.checkNotNull(options);
    this.settings = new HashMap<>(GLOBAL_SETTINGS.size() + extraServerSettings.size());
    for (Entry<String, SystemVariable> entry : GLOBAL_SETTINGS.entrySet()) {
      this.settings.put(entry.getKey(), entry.getValue().copy());
    }
    for (Entry<String, SystemVariable> entry : extraServerSettings.entrySet()) {
      this.settings.put(entry.getKey(), entry.getValue().copy());
    }
    // this.settings.get("server_version").initSettingValue(options.getServerVersion());
    // this.settings.get("server_version_num").initSettingValue(options.getServerVersionNum());
  }

  public ProtocolStatus getProtocolStatus() {
    return protocolStatus;
  }

  public void setProtocolStatus(ProtocolStatus protocolStatus) {
    this.protocolStatus = protocolStatus;
  }

  private static String toKey(String extension, String name) {
    return extension == null
        ? name.toLowerCase(Locale.ROOT)
        : extension.toLowerCase(Locale.ROOT) + "." + name.toLowerCase(Locale.ROOT);
  }

  Map<String, SystemVariable> getVariableMapForScope(SessionVariableScope scope) {
    switch (scope) {
      case GLOBAL:
        return GLOBAL_SETTINGS;
      case SESSION:
        return settings;
      default:
        throw unknownParamError("scope");
    }
  }
  /**
   * Sets the value of the specified setting. The new value will be persisted if the current
   * transaction is committed. The value will be lost if the transaction is rolled back.
   */
  public void set(String extension, String name, String value, SessionVariableScope scope) {
    logger.log(
        Level.INFO,
        () -> String.format("Setting system variable %s to %s at scope %s", name, value, scope));
    Map<String, SystemVariable> variableMap = getVariableMapForScope(scope);
    internalSet(extension, name, value, variableMap);
  }

  private void internalSet(
      String extension, String name, String value, Map<String, SystemVariable> variableMap) {
    if ("names".equals(name)) {
      // TODO: Consider handling "SET NAMES" as a separate statement type.
      handleNames(value);
      return;
    }
    if ("autocommit".equals(name)) {
      handleAutocommit(value);
    }
    String key = toKey(extension, name);
    SystemVariable variable = variableMap.get(key);
    if (variable == null) {
      throw unknownParamError(name);
    }
    variable.setValue(value);
  }

  private void handleAutocommit(String value) {
    if (BooleanParser.TRUE_VALUES.contains(value)) {
      backendConnection.processSetAutocommit();
    } else if (BooleanParser.FALSE_VALUES.contains(value)) {
      backendConnection.processUnsetAutocommit();
    } else {
      throw unknownParamError(String.format("Value %s cannot be set for autocommit", value));
    }
  }

  private void handleNames(String value) {
    logger.log(Level.INFO, () -> String.format("Setting all character sets to %s", value));
    List<String> charasets =
        ImmutableList.of(
            "character_set_client", "character_set_connection", "character_set_results");
    Map<String, SystemVariable> variableMap = getVariableMapForScope(SessionVariableScope.SESSION);
    for (String charset : charasets) {
      SystemVariable variable = variableMap.get(charset);
      Preconditions.checkNotNull(variable);
      variable.setValue(value);
    }
  }

  /** Returns the current value of the specified setting. */
  public SystemVariable get(String extension, String name, SessionVariableScope scope) {
    Map<String, SystemVariable> variableMap = getVariableMapForScope(scope);
    return internalGet(toKey(extension, name), variableMap);
  }

  private SystemVariable internalGet(String key, Map<String, SystemVariable> variableMap) {
    SystemVariable variable = variableMap.get(key);
    Preconditions.checkNotNull(variable);
    return variable;
  }

  String getStringSetting(String extension, String name, SessionVariableScope scope) {
    SystemVariable setting = internalGet(toKey(extension, name), getVariableMapForScope(scope));
    return setting.getValue();
  }

  static SpannerException unknownParamError(String key) {
    return SpannerExceptionFactory.newSpannerException(
        ErrorCode.INVALID_ARGUMENT,
        String.format("unrecognized configuration parameter \"%s\"", key));
  }
}
