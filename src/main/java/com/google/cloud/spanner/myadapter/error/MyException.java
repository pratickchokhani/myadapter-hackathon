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

package com.google.cloud.spanner.myadapter.error;

import com.google.api.core.InternalApi;
import com.google.common.base.Preconditions;
import java.util.Objects;

/**
 * {@link MyException} contains all fields that are needed to send an {@link
 * com.google.cloud.spanner.myadapter.wireoutput.ErrorResponse}.
 */
@InternalApi
public class MyException extends RuntimeException {
  public static class Builder {
    private final String message;
    private Severity severity = Severity.ERROR;
    private SQLState sqlState;
    private String hints;
    private Throwable cause;

    private Builder(String message) {
      this.message = message;
    }

    public Builder setSeverity(Severity severity) {
      this.severity = severity;
      return this;
    }

    public Builder setSQLState(SQLState sqlState) {
      this.sqlState = sqlState;
      return this;
    }

    public Builder setHints(String hints) {
      this.hints = Preconditions.checkNotNull(hints);
      return this;
    }

    public Builder setCause(Throwable cause) {
      this.cause = cause;
      return this;
    }

    public MyException build() {
      return new MyException(cause, severity, sqlState, message, hints);
    }
  }

  public static Builder newBuilder(Exception cause) {
    Preconditions.checkNotNull(cause);
    return new Builder(cause.getMessage() == null ? cause.getClass().getName() : cause.getMessage())
        .setCause(cause);
  }

  public static Builder newBuilder(String message) {
    return new Builder(Preconditions.checkNotNull(message));
  }

  private final Severity severity;
  private final SQLState sqlState;
  private final String hints;

  private MyException(
      Throwable cause, Severity severity, SQLState sqlState, String message, String hints) {
    super(message == null ? "" : message, cause);
    this.severity = severity;
    this.sqlState = sqlState;
    this.hints = hints;
  }

  public Severity getSeverity() {
    return severity;
  }

  public SQLState getSQLState() {
    return sqlState;
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.getMessage(), this.severity, this.sqlState);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof MyException)) {
      return false;
    }
    MyException other = (MyException) o;
    return Objects.equals(this.getMessage(), other.getMessage())
        && Objects.equals(this.severity, other.severity)
        && Objects.equals(this.sqlState, other.sqlState);
  }

  public String getHints() {
    return hints;
  }
}
