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
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.enterprise.cloudsearch.sdk.ConnectorScheduler.ShutdownHolder;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
 *     Application application = new Application.Builder(
 *         new MyConnector(new MyRepository()), args).build();
 *     application.start();
 *   } }
 * </pre>
 *
 * <p>Optional configuration parameter(s):
 *
 * <ul>
 *   <li>{@value #INCREMENTAL_INTERVAL_SECONDS} - Specifies the interval between scheduled
 *       incremental traversals (in seconds).
 *   <li>{@value #TRAVERSE_ON_START} - Specifies whether to run the traversal immediately at start
 *       up rather than waiting for the first interval to expire.
 *   <li>{@value #POLL_INTERVAL_SECONDS} - Specifies the interval between scheduled poll queue
 *       intervals (in seconds).
 *   <li>{@value #TRAVERSE_INTERVAL_SECONDS} - Specifies the interval between scheduled traversals
 *       (in seconds).
 *   <li>{@value #RUN_ONCE} - Specifies whether the connector should exit after a single traversal.
 * </ul>
 */
public class Application<
        H extends Application.AbstractApplicationHelper, T extends ConnectorContext>
    extends AbstractIdleService {

  private static Logger logger = Logger.getLogger(Application.class.getName());

  /** Configuration key for incremental traversal interval. */
  public static final String INCREMENTAL_INTERVAL_SECONDS =
      "schedule.incrementalTraversalIntervalSecs";

  /**
   * Configuration key to indicate if traversal should be performed at start of the connector
   * process.
   */
  public static final String TRAVERSE_ON_START = "schedule.performTraversalOnStart";

  /** Configuration key for interval between poll requests. */
  public static final String POLL_INTERVAL_SECONDS = "schedule.pollQueueIntervalSecs";

  /** Configuration key for full traversal intervals. */
  public static final String TRAVERSE_INTERVAL_SECONDS = "schedule.traversalIntervalSecs";

  /** Configuration key to specify if connector should exit after full traversal. */
  public static final String RUN_ONCE = "connector.runOnce";

  protected static final ExceptionHandler DEFAULT_EXCEPTION_HANDLER =
      new ExponentialBackoffExceptionHandler(10, 5, TimeUnit.SECONDS);
  protected final AtomicBoolean started = new AtomicBoolean(false);
  protected final Connector<T> connector;
  private final H helper;

  protected ConnectorScheduler<T> connectorScheduler;

  private Thread shutdownThread;
  protected ShutdownHolder shutdownHolder;

  protected Application(AbstractBuilder<? extends AbstractBuilder, H, T> builder) {
    this.helper = checkNotNull(builder.helper);
    this.connector = checkNotNull(builder.connector);
  }

  /**
   * Returns a connector context. Connectors that extend {@link ConnectorContext},
   * should override this method.
   */
  @SuppressWarnings("unchecked")
  protected T buildConnectorContext() {
    return (T) helper.createContextBuilderInstance().build();
  }

  /**
   * Begins connector execution by initializing and starting the SDK.
   *
   * <p>This is the main entry point for the SDK. This method initializes all the objects used in
   * traversing a repository and schedules the traversals based on the configuration parameters.
   *
   * @throws InterruptedException if aborted during start up
   */
  public void start() throws InterruptedException {
    startAsync().awaitRunning();
  }

  protected void startApplication() {
    synchronized (this) {
      checkState(started.compareAndSet(false, true), "Application already started");
      shutdownThread = helper.createShutdownHookThread(new ShutdownHook());
      helper.getRuntimeInstance().addShutdownHook(shutdownThread);
      shutdownHolder = () -> shutdownThread.start();
    }
  }

  protected T startConnector() throws InterruptedException {
    ExceptionHandler exceptionHandler = helper.getDefaultExceptionHandler();
    int tries = 1;
    T context = null;
    while (true) {
      try {
        // Reset context for each try
        context = checkNotNull(buildConnectorContext());
        connector.init(context);
        break;
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw ex;
      } catch (StartupException ex) {
        throw ex;
      } catch (RuntimeException ex) {
        throw new StartupException("Failed to initialize connector", ex);
      } catch (GeneralSecurityException ex) {
        throw new StartupException("Failed to create credential", ex);
      } catch (Exception ex) {
        logger.log(Level.WARNING, "Failed to initialize connector", ex);
        if (!exceptionHandler.handleException(ex, tries)) {
          throw new StartupException("Failed to initialize connector", ex);
        }
        tries++;
      }
    }
    checkState(context != null, "Context not initialized successfully");
    return context;
  }

  @SuppressWarnings("unchecked")
  protected void startScheduler(T context) {
    connectorScheduler = helper.createSchedulerBuilderInstance()
        .setConnector(connector)
        .setContext(context)
        .setShutdownHolder(shutdownHolder)
        .build();
    connectorScheduler.start();
  }

  /**
   * Shutdown Connector in response to an event.
   *
   * @param event triggering shutdown.
   */
  public synchronized void shutdown(String event) {
    logger.log(Level.INFO, "Shutdown Connector {0}", event);
    stopAsync().awaitTerminated();
  }

  /**
   * {@link Runnable} implementation to register with {@link Runtime#addShutdownHook}. Responsible
   * for shutting down SDK worker threads and calling {@link Connector#destroy}
   */
  @VisibleForTesting
  public class ShutdownHook implements Runnable {

    @Override
    public void run() {
      shutdown("ShutdownHook initiated");
    }
  }

  /**
   * Helper method to override default {@link Logger} used by {@link Application} implementation.
   *
   * @param logger to use
   */
  @VisibleForTesting
  public static void setLogger(Logger logger) {
    Application.logger = logger;
  }

  @Override
  protected void startUp() throws Exception {
    startApplication();
    T context = startConnector();
    startScheduler(context);
  }

  @Override
  protected void shutDown() throws Exception {
    if ((connectorScheduler != null) && connectorScheduler.isStarted()) {
      connectorScheduler.stop();
    }
    connector.destroy();
  }

  protected abstract static class AbstractBuilder<B extends AbstractBuilder<B, H, T>,
      H extends AbstractApplicationHelper, T extends ConnectorContext> {
    public H helper;
    protected Connector<T> connector;
    protected CredentialFactory credentialFactory;
    protected String[] args;

    protected abstract B getThis();
    public abstract Application<H, T> build();

    /**
     * Builder for Application
     *
     * @param connector instance
     * @param args command line arguments
     */
    public AbstractBuilder(Connector<T> connector, String[] args) {
      this.connector = checkNotNull(connector);
      this.args = checkNotNull(args);
    }

    @VisibleForTesting
    public B setHelper(H helper) {
      this.helper = helper;
      return getThis();
    }

    /**
     * Creates a GoogleCredential object.
     *
     * @return {@link CredentialFactory} object for creating {@link GoogleCredential} instance.
     */
    public B setCredentialFactory(CredentialFactory credentialFactory) {
      this.credentialFactory = credentialFactory;
      return getThis();
    }

    protected void initConfig() throws StartupException {
      try {
        if (!Configuration.isInitialized()) {
          Configuration.initConfig(args);
        }
      } catch (IOException configException) {
        throw new StartupException("failed to load configuration", configException);
      }
    }
  }

  /** Builder object for creating {@link Application} instance. */
  public static class Builder
      extends AbstractBuilder<Builder, ApplicationHelper, ConnectorContext> {
    /**
     * Builder for Application
     *
     * @param connector instance
     * @param args command line arguments
     */
    public Builder(Connector<ConnectorContext> connector, String[] args) {
      super(connector, args);
      setHelper(new ApplicationHelper());
    }

    @Override
    protected Builder getThis() {
      return this;
    }

    /** Create an instance of {@link Application} */
    @Override
    public Application<ApplicationHelper, ConnectorContext> build() throws StartupException {
      initConfig();
      return new Application<ApplicationHelper, ConnectorContext>(this);
    }
  }

  /**
   * Helper class to make our class more testable: factory and util methods
   */
  public abstract static class AbstractApplicationHelper<
      C extends ConnectorContextImpl.AbstractBuilder<C, T>,
      S extends ConnectorScheduler.AbstractBuilder<S, T>,
      T extends ConnectorContext> {
    /**
     * Helper method to create an instance of {@link ConnectorContextImpl.Builder}
     *
     * @return instance of {@link ConnectorContextImpl.Builder}
     */
    public abstract C createContextBuilderInstance();

    /**
     * Helper method to create an instance of {@link ConnectorScheduler.Builder}
     *
     * @return instance of {@link ConnectorContextImpl.Builder}
     */
    public abstract S createSchedulerBuilderInstance();

    /**
     * Helper method to create an instance of {@link LocalFileCredentialFactory}
     *
     * @return instance of {@link LocalFileCredentialFactory}
     */
    public CredentialFactory createCredentialFactory() {
      return LocalFileCredentialFactory.fromConfiguration();
    }

    /**
     * Helper method for creating {@link Thread} to be register as ShutdownHook
     *
     * @param task {@link Runnable} to be executed as part of ShutdownHook
     * @return thread containing runnable for ShutdownHook
     */
    public Thread createShutdownHookThread(Runnable task) {
      return new Thread(task, "connector-shutdown");
    }

    /**
     * Helper method to get instance of current {@link Runtime}
     *
     * @return instance of current {@link Runtime}
     */
    public Runtime getRuntimeInstance() {
      return Runtime.getRuntime();
    }

    /**
     * helper method to get default instance of {@link ExceptionHandler}
     *
     * @return instance of {@link ExceptionHandler}
     */
    public ExceptionHandler getDefaultExceptionHandler() {
      return DEFAULT_EXCEPTION_HANDLER;
    }
  }

  @VisibleForTesting
  static class ApplicationHelper extends AbstractApplicationHelper
      <ConnectorContextImpl.Builder, ConnectorScheduler.Builder, ConnectorContext> {

    @Override
    public ConnectorContextImpl.Builder createContextBuilderInstance() {
      return new ConnectorContextImpl.Builder();
    }

    @Override
    public ConnectorScheduler.Builder createSchedulerBuilderInstance() {
      return new ConnectorScheduler.Builder();
    }
  }
}
