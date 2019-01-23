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
 * Exception that is thrown for fatal configuration errors.
 *
 * <p>This exception type bypasses the retry with back-off recovery logic of {@link Application} and
 * immediately terminates the connector execution.
 */
public class InvalidConfigurationException extends StartupException {

  /**
   * Constructs a configuration exception with no message and no cause.
   */
  public InvalidConfigurationException() {
    super();
  }
  /**
   * Constructs a configuration exception with a supplied message but no cause.
   *
   * @param message the message, retrievable using the {@link #getMessage()} method
   */
  public InvalidConfigurationException(String message) {
    super(message);
  }
  /**
   * Constructs a configuration exception with message and cause.
   *
   * @param message the message, retrievable using the {@link #getMessage()} method
   * @param cause failure cause
   */
  public InvalidConfigurationException(String message, Throwable cause) {
    super(message, cause);
  }
  /**
   * Constructs a configuration exception with specified cause, copying its message if cause
   * is non-{@code null}.
   *
   * @param cause failure cause
   */
  public InvalidConfigurationException(Throwable cause) {
    super(cause);
  }
}
