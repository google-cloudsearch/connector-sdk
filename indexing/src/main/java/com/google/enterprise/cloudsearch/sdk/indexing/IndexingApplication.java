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

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.cloudsearch.sdk.Application;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Main object and access point for the SDK.
 *
 * <p>Every connector begins execution by creating an instance of this class and calling its
 * {@link #start()} method. This starts processing, triggering the SDK to begin making connector
 * calls as configured in the configuration file.
 *
 * <p>Sample usage:
 * <pre>{@code
 *   public static void main(String[] args) throws IOException, InterruptedException {
 *     IndexingApplication application = new IndexingApplication.Builder(
 *         new MyConnector(new MyRepository()), args).build();
 *     application.start();
 *   } }
 * </pre>
 *
 * <p>Optional configuration parameter(s):
 * <ul>
 *   <li>{@code structuredData.localSchema} - Specifies the local structured data schema name.
 *       It is read from the data source and used for repository structured data.
 *   <li>{@code schedule.incrementalTraversalIntervalSecs} - Specifies the interval between
 *       scheduled incremental traversals (in seconds).
 *   <li>{@code schedule.performTraversalOnStart} - Specifies whether to run the traversal
 *       immediately at start up rather than waiting for the first interval to expire.
 *   <li>{@code schedule.pollQueueIntervalSecs} - Specifies the interval between scheduled poll
 *       queue intervals (in seconds).
 *   <li>{@code schedule.traversalIntervalSecs} - Specifies the interval between scheduled
 *       traversals (in seconds).
 *   <li>{@code connector.runOnce} - Specifies whether the connector should exit after a single
 *       traversal.
 * </ul>
 */
public class IndexingApplication
    extends Application<IndexingApplication.ApplicationHelper, IndexingConnectorContext> {

  private static Logger logger = Logger.getLogger(IndexingApplication.class.getName());

  /** @deprecated Use {@link StructuredData#LOCAL_SCHEMA} */
  @Deprecated public static final String LOCAL_SCHEMA = StructuredData.LOCAL_SCHEMA;

  private final ApplicationHelper helper;
  private final IndexingService indexingService;

  private IndexingApplication(Builder builder) {
    super(builder);
    this.helper = checkNotNull(builder.helper);
    this.indexingService = checkNotNull(builder.indexingService);
  }

  @Override
  protected IndexingConnectorContext buildConnectorContext() {
    return helper.createContextBuilderInstance()
        .setIndexingService(indexingService)
        .build();
  }

  @Override
  protected void startUp() throws Exception {
    startApplication();

    indexingService.startAsync().awaitRunning();
    if (!StructuredData.isInitialized()) {
      StructuredData.initFromConfiguration(indexingService);
    }

    IndexingConnectorContext context;
    try {
      context = startConnector();
    } catch (StartupException e) {
      logger.log(Level.SEVERE, "Failed to initialize connector", e);
      if (indexingService.isRunning()) {
        indexingService.stopAsync().awaitTerminated();
      }
      throw e;
    }
    startScheduler(checkNotNull(context));
  }

  @Override
  protected void shutDown() throws Exception {
    logger.log(Level.INFO, "Shutdown Indexing service");
    if ((indexingService != null) && indexingService.isRunning()) {
      indexingService.stopAsync().awaitTerminated();
    }
    super.shutDown();
  }

  /** Buider for {@code IndexingApplication} instances. */
  public static class Builder
      extends AbstractBuilder<Builder, ApplicationHelper, IndexingConnectorContext> {
    private IndexingService indexingService;

    /**
     * Builder for IndexingApplication
     *
     * @param connector instance
     * @param args command line arguments
     */
    public Builder(IndexingConnector connector, String[] args) {
      super(connector, args);
      setHelper(new ApplicationHelper());
    }

    @Override
    protected Builder getThis() {
      return this;
    }

    public Builder setIndexingService(IndexingService indexingService) {
      this.indexingService = indexingService;
      return this;
    }

    @Override
    public IndexingApplication build() throws StartupException {
      initConfig();
      if (indexingService == null) {
        if (this.credentialFactory == null) {
          this.credentialFactory = helper.createCredentialFactory();
        }

        try {
          IndexingServiceImpl.Builder builder =
              IndexingServiceImpl.Builder.fromConfiguration(
                  Optional.ofNullable(this.credentialFactory), connector.getDefaultId());
          this.indexingService = builder.build();
        } catch (GeneralSecurityException | IOException e) {
          throw new StartupException("failed to create HttpTransport for IndexingService", e);
        }
      }

      return new IndexingApplication(this);
    }
  }

  /**
   * Helper class to make our class more testable: factory and util methods
   */
  @VisibleForTesting
  static class ApplicationHelper extends Application.AbstractApplicationHelper
      <IndexingConnectorContextImpl.Builder, ConnectorTraverser.Builder, IndexingConnectorContext> {
    @Override
    public IndexingConnectorContextImpl.Builder createContextBuilderInstance() {
      return new IndexingConnectorContextImpl.Builder();
    }

    @Override
    public ConnectorTraverser.Builder createSchedulerBuilderInstance() {
      return new ConnectorTraverser.Builder();
    }
  }
}
