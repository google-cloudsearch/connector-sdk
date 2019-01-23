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
package com.google.enterprise.cloudsearch.sdk.indexing;

import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Index;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Poll;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Push;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.enterprise.cloudsearch.sdk.Connector;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import java.io.IOException;

/**
 * Interface for user-specific implementation details of the connector.
 *
 * <p>Implementations must be thread-safe. Implementations are discouraged from keeping any state
 * locally except perhaps soft-state such as a connection cache.
 */
public interface IndexingConnector extends Connector<IndexingConnectorContext> {
  /**
   * Initializes the connector with the current context.
   *
   * <p>Initialization is the ideal time to start any threads to do extra behind-the-scenes work.
   * The {@code context} allows access to other useful objects that can be used at any time such as
   * the {@link IndexingService}.
   *
   * <p>If an unrecoverable fatal error occurs during initialization, throw a {@link
   * StartupException} to cancel the startup process. If a recoverable error occurs during
   * initialization, most {@link Exception} instances other than {@link StartupException} cause a
   * retry of initialization after a short delay.
   *
   * @param context {@link IndexingConnectorContext} instance for accessing framework objects
   * @throws Exception if errors occur during connector initialization
   */
  @Override
  void init(IndexingConnectorContext context) throws Exception;

  /**
   * Implements a connector-specific traversal strategy.
   *
   * <p>Use the {@link Push} method to push a list of {@code PushItem} instances that are to be
   * indexed. Often, many of the sent items are unchanged since the last traversal and are thus
   * being re-pushed unnecessarily. However, the cost of pushing unchanged items is low and doing
   * so heals any synchronization errors or cache inconsistencies that might exist in the system.
   *
   * <p>For flat or full traversal strategies, push {@link PushItem} instances for all items to be
   * indexed. For graph traversal strategies, push only the start node of each graph.
   *
   * <p>Alternatively, a {@link Connector} implementation can issue an {@link Index} for
   * each item in the repository instead of using {@link Push} and {@link Poll}. Such
   * implementations might be suitable for smaller repositories requiring only a periodic full sync.
   *
   * <p>This method may take a while and implementations may want to call {@link Thread#sleep}
   * occasionally to reduce load.
   *
   * <p>If fatal errors occur, throw an {@link IOException} or {@link RuntimeException}.
   * In the case of an error, the {@link com.google.enterprise.cloudsearch.sdk.ExceptionHandler}
   * defined in {@link IndexingConnectorContext} determines if and when to retry.
   *
   * @throws IOException if getting data access errors
   * @throws InterruptedException if an IO operations throws it
   */
  @Override
  void traverse() throws IOException, InterruptedException;
}
