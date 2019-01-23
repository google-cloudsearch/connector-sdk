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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.enterprise.cloudsearch.sdk.ConnectorContextImpl;
import com.google.enterprise.cloudsearch.sdk.indexing.traverser.TraverserConfiguration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Context for an {@link IndexingConnector} . */
class IndexingConnectorContextImpl extends ConnectorContextImpl
      implements IndexingConnectorContext {

  private final List<TraverserConfiguration> traverserConfigurations;
  private final IndexingService indexingService;

  private IndexingConnectorContextImpl(Builder builder) {
    super(builder);
    traverserConfigurations = new ArrayList<>();
    indexingService = checkNotNull(builder.indexingService);
  }

  public static class Builder extends AbstractBuilder<Builder, IndexingConnectorContext> {
    private IndexingService indexingService;

    @Override
    protected Builder getThis() {
      return this;
    }

    public Builder setIndexingService(IndexingService indexingService) {
      this.indexingService = indexingService;
      return this;
    }

    @Override
    public IndexingConnectorContext build() {
      return new IndexingConnectorContextImpl(this);
    }
  }

  @Override
  public void registerTraverser(TraverserConfiguration traverserConfiguration) {
    traverserConfigurations.add(traverserConfiguration);
  }

  /**
   * Returns Indexing service instance.
   *
   * @return {@link IndexingServiceImpl} instance
   */
  @Override
  public IndexingService getIndexingService() {
    return indexingService;
  }

  @Override
  public List<TraverserConfiguration> getTraverserConfiguration() {
    return Collections.unmodifiableList(traverserConfigurations);
  }
}
