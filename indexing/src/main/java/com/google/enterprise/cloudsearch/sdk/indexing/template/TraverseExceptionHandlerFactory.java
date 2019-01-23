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
package com.google.enterprise.cloudsearch.sdk.indexing.template;

import static com.google.common.base.Preconditions.checkState;

import com.google.enterprise.cloudsearch.sdk.AbortCountExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.IgnoreExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.ConfigValue;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.Parser;
import java.util.concurrent.TimeUnit;

/**
 * Constructs an {@link ExceptionHandler} based up on configuration options.
 *
 * <p>Optional configuration parameters:
 *
 * <ul>
 *   <li>{@code traverse.exceptionHandler} - Specifies the action to take if the traversal
 *       encounters exceptions: always abort (“0”), never abort ("ignore"), abort only after
 *       # exceptions are encountered ("10").
 *   <li>{@code abortExceptionHander.backoffMilliSeconds} - Specifies the backoff time in
 *       milliseconds to wait between detected handler exceptions (typically used when
 *       traversing a repository).
 * </ul>
 */
// TODO(bmj): Support "exponential".
public class TraverseExceptionHandlerFactory {

  private static final Parser<ExceptionHandler> PARSER = new ExceptionHandlerParser();

  private static final AbortCountExceptionHandler ABORT_ON_ERROR =
      new AbortCountExceptionHandler(0, 0, TimeUnit.MILLISECONDS);
  private static final int DEFAULT_BACKOFF_MILLISECONDS = 10;

  public static final String TRAVERSE_EXCEPTION_HANDLER = "traverse.exceptionHandler";
  public static final String EXCEPTION_HANDLER_BACKOFF = "abortExceptionHander.backoffMilliSeconds";

  // Prevents instantiation.
  private TraverseExceptionHandlerFactory() {
  }

  /**
   * Creates an {@link ExceptionHandler} from a single call, as defined by the configuration file.
   *
   * @return configured exception handler
   */
  public static ExceptionHandler createFromConfig() {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    return Configuration.getValue(TRAVERSE_EXCEPTION_HANDLER, ABORT_ON_ERROR, PARSER)
        .get();
  }

  private static class ExceptionHandlerParser implements Parser<ExceptionHandler> {
    private static final String IGNORE = "ignore";

    @Override
    public ExceptionHandler parse(String value) throws InvalidConfigurationException {
      if (IGNORE.equals(value)) {
        return new IgnoreExceptionHandler();
      } else {
        int exceptionCount;
        try {
          exceptionCount = Integer.parseInt(value);
        } catch (NumberFormatException e) {
          throw new InvalidConfigurationException(
              "Unrecognized value for traversal exception handler: " + value, e);
        }
        ConfigValue<Integer> backoff =
            Configuration.getInteger(EXCEPTION_HANDLER_BACKOFF, DEFAULT_BACKOFF_MILLISECONDS);
        return new AbortCountExceptionHandler(exceptionCount, backoff.get(), TimeUnit.MILLISECONDS);
      }
    }
  }
}
