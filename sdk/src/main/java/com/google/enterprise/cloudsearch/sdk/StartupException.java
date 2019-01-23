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

/**
 * Exception that is thrown for unrecoverable startup errors, such as fatal configuration errors or
 * running on the wrong platform.
 *
 * <p>This exception type bypasses the retry with back-off recovery logic of {@link Application} and
 * immediately terminates the connector execution.
 */
public class StartupException extends RuntimeException {

  /**
   * Constructs a startup exception with no message and no cause.
   */
  public StartupException() {
    super();
  }

  /**
   * Constructs a startup exception with a supplied message but no cause.
   *
   * @param message the message, retrievable using the {@link #getMessage()} method
   */
  public StartupException(String message) {
    super(message);
  }

  /**
   * Constructs a startup exception with message and cause.
   *
   * @param message the message, retrievable using the {@link #getMessage()} method
   * @param cause failure cause
   */
  public StartupException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructs a startup exception with specified cause, copying its message if cause
   * is non-{@code null}.
   *
   * @param cause failure cause
   */
  public StartupException(Throwable cause) {
    super(cause);
  }
}
