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

import java.io.IOException;

/**
 * Interface for user-specific implementation details of the connector.
 *
 * <p>Implementations must be thread-safe. Implementations are discouraged from keeping any state
 * locally except perhaps soft-state such as a connection cache.
 */
public interface Connector<T extends ConnectorContext> {

  /**
   * Initializes the connector with the current context.
   *
   * <p>Initialization is the ideal time to start any threads to do extra behind-the-scenes work.
   * The {@code context} allows access to other useful objects that can be used at any time.
   *
   * <p>If an unrecoverable fatal error occurs during initialization, throw a {@link
   * StartupException} to cancel the startup process. If a recoverable error occurs during
   * initialization, most {@link Exception} instances other than {@link StartupException} cause a
   * retry of initialization after a short delay.
   *
   * @param context {@link ConnectorContext} instance for accessing framework objects
   * @throws Exception if errors occur during connector initialization
   */
  void init(T context) throws Exception;

  /**
   * Implements a connector-specific traversal strategy.
   *
   * <p>This method may take a while and implementations may want to call {@link Thread#sleep}
   * occasionally to reduce load.
   *
   * <p>If fatal errors occur, throw an {@link IOException} or {@link RuntimeException}.
   * In the case of an error, the {@link ExceptionHandler} defined in {@link ConnectorContext}
   * determines if and when to retry.
   *
   * @throws IOException if getting data access errors
   * @throws InterruptedException if an IO operations throws it
   */
  void traverse() throws IOException, InterruptedException;

  /**
   * Saves checkpoint information such as current traversal position or incremental change tokens.
   *
   * <p>The checkpoint contents are implementation-specific as defined by the connector code. This
   * method is called during shutdown to allow the connector to save the current traversal state.
   *
   * @param isShutdown flag indicating a connector shutdown
   * @throws IOException if saving checkpoint fails.
   * @throws InterruptedException if an IO operations throws it
   */
  void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException;

  /**
   * Shuts down and releases connector resources.
   */
  void destroy();

  /** Gets the default connector ID. */
  default String getDefaultId() {
    return getClass().getName();
  }
}
