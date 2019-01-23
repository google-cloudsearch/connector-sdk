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

import com.google.common.base.Preconditions;
import java.util.concurrent.TimeUnit;

/**
 * Exception handler that is configured with a count of allowable retries before forcing an abort.
 *
 * <p>This is similar to the {@link ExponentialBackoffExceptionHandler} except the wait time is
 * constant.
 */
public class AbortCountExceptionHandler implements ExceptionHandler {
  private final int maximumTries;
  private final long sleepDuration;
  private final TimeUnit sleepUnit;

  /**
   * Constructs an AbortCountExceptionHandler.
   *
   * @param maximumTries number of retries to take if the traversal encounters exceptions
   * @param sleepDuration the backoff time to wait between detected handler exceptions
   * @param sleepUnit the TimeUnit for sleepDuration
   */
  public AbortCountExceptionHandler(int maximumTries, long sleepDuration, TimeUnit sleepUnit) {
    this.maximumTries = maximumTries;
    this.sleepDuration = sleepDuration;
    this.sleepUnit = Preconditions.checkNotNull(sleepUnit);
  }

  /**
   * Determines how to proceed when an exception is thrown.
   *
   * @param e occurring exception
   * @param ntries number of previous failures
   * @return true to retry, false to abort (or rethrow exception)
   * @throws InterruptedException on interruption
   */
  @Override
  public boolean handleException(Exception e, int ntries) throws InterruptedException {
    if (ntries > this.maximumTries) {
      return false;
    } else {
      this.sleepUnit.sleep(this.sleepDuration);
      return true;
    }
  }
}
