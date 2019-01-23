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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * {@link ExceptionHandler} implementation that uses exponential backoff before retrying.
 */
public class ExponentialBackoffExceptionHandler implements ExceptionHandler {

  private final int maximumTries;
  private final long sleepDuration;
  private final TimeUnit sleepUnit;

  /**
   * Creates a handler that uses an exponentially increasing amount of sleep time to implement a
   * backoff before retrying.
   *
   * @param maximumTries how many times to try before permanent failure
   * @param sleepDuration initial sleep duration on failure
   * @param sleepUnit sleep duration time unit
   */
  public ExponentialBackoffExceptionHandler(
      int maximumTries, long sleepDuration, TimeUnit sleepUnit) {
    this.maximumTries = maximumTries;
    this.sleepDuration = sleepDuration;
    this.sleepUnit = checkNotNull(sleepUnit);
  }

  /**
   * Handles the exception by forcing an increasing wait time based on the total number of
   * exceptions previously issued.
   *
   * <p>This method continues to perform waits up to the maximum configured number of allowable
   * exceptions. The return value indicates whether the calling process should issue an abort.
   *
   * @param ex exception denoting the error
   * @param ntries number of consecutive failures for the same operation
   * @return {@code true} to retry immediate, {@code false} to abort
   * @throws InterruptedException if the backoff throws it
   */
  @Override
  public boolean handleException(Exception ex, int ntries) throws InterruptedException {
    if (ntries > maximumTries) {
      return false;
    }
    sleepUnit.sleep(sleepDuration * ntries);
    return true;
  }

  @Override
  public int hashCode() {
    return Objects.hash(maximumTries, sleepDuration, sleepUnit);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof ExponentialBackoffExceptionHandler)) {
      return false;
    }
    ExponentialBackoffExceptionHandler handler = (ExponentialBackoffExceptionHandler) obj;
    return maximumTries == handler.maximumTries
        && sleepDuration == handler.sleepDuration
        && Objects.equals(sleepUnit, handler.sleepUnit);
  }
}
