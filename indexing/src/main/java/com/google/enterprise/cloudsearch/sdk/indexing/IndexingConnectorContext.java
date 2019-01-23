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

import com.google.enterprise.cloudsearch.sdk.ConnectorContext;
import com.google.enterprise.cloudsearch.sdk.indexing.traverser.TraverserConfiguration;
import java.util.List;

/**
 * Interface for a context object created by the SDK to pass to the {@link IndexingConnector} code.
 *
 * <p>The {@link IndexingApplication} object creates a context instance containing an
 * {@link IndexingService} instance, {@link com.google.enterprise.cloudsearch.sdk.ExceptionHandler}
 * instances, and {@link TraverserConfiguration} instances for the connector to access. It calls the
 * {@link IndexingConnector#init(IndexingConnectorContext)} method to pass the context to
 * the connector code.
 */
public interface IndexingConnectorContext extends ConnectorContext {

  /**
   * Creates and registers {@link TraverserConfiguration} instances.
   *
   * <p>A {@link TraverserConfiguration} instance defines a Cloud Search queue polling task. The
   * {@link IndexingConnector} uses this method when it implements the {@link ItemRetriever}
   * interface.
   *
   * @param configuration parameters for a Cloud Search queue polling task
   */
  void registerTraverser(TraverserConfiguration configuration);

  /**
   * Returns the {@link IndexingService} instance used to communicate with the Cloud Search API.
   *
   * @return an {@link IndexingService} instance
   */
  IndexingService getIndexingService();

  /**
   * Returns the list of {@link TraverserConfiguration} registered by the connector during
   * {@link IndexingConnector#init(IndexingConnectorContext)}.
   *
   * @return a list of {@link TraverserConfiguration}
   */
  List<TraverserConfiguration> getTraverserConfiguration();
}
