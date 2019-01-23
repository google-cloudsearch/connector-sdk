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
 * Interface for handling errors and defining a retry policy.
 */
public interface ExceptionHandler {
  /**
   * Determines how to proceed after an exception is thrown.
   *
   * <p>The connector code passes the exception along with the number of attempts to the method.
   * Implementations might call {@code Thread.sleep()} to delay or <em>back off</em> before
   * returning.
   *
   * @param ex the exception that occurred
   * @param ntries the number of consecutive failures for the current operation
   * @return {@code true} for an immediate retry or {@code false} to abort
   * @throws InterruptedException if an interruption occurs during the <em>back off</em>
   */
  public boolean handleException(Exception ex, int ntries) throws InterruptedException;
}
