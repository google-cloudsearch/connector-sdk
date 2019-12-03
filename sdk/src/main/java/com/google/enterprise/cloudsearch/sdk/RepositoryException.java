/*
 * Copyright © 2017 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.enterprise.cloudsearch.sdk;

import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import java.io.IOException;
import java.util.Optional;

/**
 * An exception that is thrown specifically for fatal repository access errors.
 */
public class RepositoryException extends IOException {

  private final Optional<Integer> errorCode;
  private final Optional<ErrorType> errorType;

  /** Repository error types as defined by Cloud Search API. */
  public enum ErrorType {
    UNKNOWN,
    NETWORK_ERROR,
    DNS_ERROR,
    CONNECTION_ERROR,
    AUTHENTICATION_ERROR,
    AUTHORIZATION_ERROR,
    SERVER_ERROR,
    QUOTA_EXCEEDED,
    CLIENT_ERROR
  }

  private RepositoryException(Builder builder) {
    super(builder.message, builder.cause);
    this.errorType = builder.errorType;
    this.errorCode = builder.errorCode;
  }

  /** Returns {@link RepositoryError} based on {@link RepositoryException} */
  public RepositoryError getRepositoryError() {
    RepositoryError error = new RepositoryError();
    error.setErrorMessage(getMessage());
    if (errorCode.isPresent()) {
      error.setHttpStatusCode(errorCode.get());
    }
    if (errorType.isPresent()) {
      error.setType(errorType.get().name());
    }
    return error;
  }

  @Override
  public String toString() {
    return getRepositoryError()
        + ((getCause() != null) ? ", cause=" + getCause() : "");
  }

  /** Builder for creating {@link RepositoryException} */
  public static class Builder {
    private String message;
    private Throwable cause;
    private Optional<Integer> errorCode = Optional.empty();
    private Optional<ErrorType> errorType = Optional.empty();

    /** Sets error message for exception. Mapped to {@link RepositoryError#getErrorMessage} */
    public Builder setErrorMessage(String errorMessage) {
      this.message = errorMessage;
      return this;
    }

    /** Sets error type for exception. Mapped to {@link RepositoryError#getType()} */
    public Builder setErrorType(ErrorType errorType) {
      this.errorType = Optional.ofNullable(errorType);
      return this;
    }

    /** Sets {@code cause} for exception. */
    public Builder setCause(Throwable cause) {
      this.cause = cause;
      return this;
    }

    /** Sets error code for exception. Mapped to {@link RepositoryError#getHttpStatusCode} */
    public Builder setErrorCode(int errorCode) {
      this.errorCode = Optional.of(errorCode);
      return this;
    }

    /** Builds an instance of {@link RepositoryException} */
    public RepositoryException build() {
      return new RepositoryException(this);
    }
  }
}
