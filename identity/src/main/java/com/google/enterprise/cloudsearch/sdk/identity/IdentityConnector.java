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
package com.google.enterprise.cloudsearch.sdk.identity;

import com.google.enterprise.cloudsearch.sdk.Connector;
import com.google.enterprise.cloudsearch.sdk.StartupException;

/**
 * Interface for user-specific implementation details of the connector.
 *
 * <p>Implementations must be thread-safe. Implementations are discouraged from keeping any state
 * locally except perhaps soft-state such as a connection cache.
 */
public interface IdentityConnector extends Connector<IdentityConnectorContext> {
  /**
   * Initializes the connector with the current context.
   *
   * <p>Initialization is the ideal time to start any threads to do extra behind-the-scenes work.
   * The {@code context} allows access to other useful objects that can be used at any time such as
   * the {@link IdentityService}.
   *
   * <p>If an unrecoverable fatal error occurs during initialization, throw a {@link
   * StartupException} to cancel the startup process. If a recoverable error occurs during
   * initialization, most {@link Exception} instances other than {@link StartupException} cause a
   * retry of initialization after a short delay.
   *
   * @param context {@link IdentityConnectorContext} instance for accessing framework objects
   * @throws Exception if errors occur during connector initialization
   */
  @Override
  void init(IdentityConnectorContext context) throws Exception;

  // TODO: Perhaps include and override of traverse() with enhanced JavaDoc.
}

