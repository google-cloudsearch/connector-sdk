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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.cloudsearch.sdk.Application;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import java.io.IOException;
import java.security.GeneralSecurityException;
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
 *     IdentityApplication application = new IdentityApplication.Builder(
 *         new MyIdentityConnector(new MyIdentityRepository()), args).build();
 *     application.start();
 *   } }
 * </pre>
 *
 * <p>Optional configuration parameter(s):
 * <ul>
 *   <li>{@code schedule.incrementalTraversalIntervalSecs} - Specifies the interval between
 *       scheduled incremental traversals (in seconds).
 *   <li>{@code schedule.performTraversalOnStart} - Specifies whether to run the traversal
 *       immediately at start up rather than waiting for the first interval to expire.
 *   <li>{@code schedule.traversalIntervalSecs} - Specifies the interval between scheduled
 *       traversals (in seconds).
 *   <li>{@code connector.runOnce} - Specifies whether the connector should exit after a single
 *       traversal.
 * </ul>
 */
public class IdentityApplication
    extends Application<IdentityApplication.ApplicationHelper, IdentityConnectorContext> {

  private static Logger logger = Logger.getLogger(IdentityApplication.class.getName());

  private final ApplicationHelper helper;
  private final IdentityService identityService;

  private IdentityApplication(Builder builder) {
    super(builder);
    this.helper = checkNotNull(builder.helper);
    this.identityService = checkNotNull(builder.identityService);
  }

  @Override
  protected IdentityConnectorContext buildConnectorContext() {
    return helper.createContextBuilderInstance()
        .setIdentityService(identityService)
        .build();
  }

  @Override
  protected void startUp() throws Exception {
    startApplication();
    identityService.startAsync().awaitRunning();
    IdentityConnectorContext context;
    try {
      context = startConnector();
    } catch (StartupException e) {
      logger.log(Level.SEVERE, "Failed to initialize connector", e);
      identityService.stopAsync().awaitTerminated();
      throw e;
    }
    startScheduler(checkNotNull(context));
  }

  @Override
  protected void shutDown() throws Exception {
    logger.log(Level.INFO, "Shutdown Identity Service");
    if ((identityService != null) && identityService.isRunning()) {
      identityService.stopAsync().awaitTerminated();
    }
    super.shutDown();
  }

  /** Builder for creating an instance of {@link IdentityApplication} */
  public static class Builder
      extends AbstractBuilder<Builder, ApplicationHelper, IdentityConnectorContext> {
    public IdentityService identityService;

    /**
     * Builder for IdentityApplication
     *
     * @param connector instance
     * @param args command line arguments
     */
    public Builder(IdentityConnector connector, String[] args) {
      super(connector, args);
      setHelper(new ApplicationHelper());
    }

    /** Returns this builder instance. */
    @Override
    protected Builder getThis() {
      return this;
    }

    /**
     * Sets {@link IdentityService} instance to be used for making API requests syncing user and
     * group identities.
     *
     * @param identityService to be used for making API requests syncing user and group identities.
     * @return this builder instance.
     */
    public Builder setIdentityService(IdentityService identityService) {
      this.identityService = identityService;
      return this;
    }

    /** Builds an instance of {@link IdentityApplication}. */
    @Override
    public IdentityApplication build() throws StartupException {
      initConfig();
      if (identityService == null) {
        if (this.credentialFactory == null) {
          this.credentialFactory = helper.createCredentialFactory();
        }
        try {
          this.identityService =
              new IdentityServiceImpl.Builder()
                  .setGroupsService(GroupsServiceImpl.fromConfiguration(credentialFactory))
                  .setUsersService(UsersServiceImpl.fromConfiguration(credentialFactory))
                  .build();
        } catch (GeneralSecurityException | IOException e) {
          throw new StartupException("failed to create HttpTransport for IdentityService", e);
        }
      }
      return new IdentityApplication(this);
    }
  }

  /**
   * Helper class to make our class more testable: factory and util methods
   */
  @VisibleForTesting
  static class ApplicationHelper extends Application.AbstractApplicationHelper
      <IdentityConnectorContextImpl.Builder, IdentityScheduler.Builder, IdentityConnectorContext> {

    @Override
    public IdentityConnectorContextImpl.Builder createContextBuilderInstance() {
      return new IdentityConnectorContextImpl.Builder();
    }

    @Override
    public IdentityScheduler.Builder createSchedulerBuilderInstance() {
      return new IdentityScheduler.Builder();
    }
  }
}
