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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Handles scheduling and execution of connector traversal related tasks. */
public class ConnectorScheduler <T extends ConnectorContext> {
  private static final Logger logger = Logger.getLogger(ConnectorScheduler.class.getName());

  private final Connector<T> connector;
  private final T context;
  private final ShutdownHolder shutdownHolder;

  /**
   * Executor service for scheduling traversal related tasks. Task executed here should complete
   * quickly as this executor service is single threaded.
   *
   * <p>The {@link ScheduledExecutorService} implementation provided by Java is a fixed size thread
   * pool. For efficient use of fixed number of threads, this service delegates actual tasks to
   * {@link #getBackgroundExecutor} for execution. This helps avoid blocking single threaded {@link
   * ScheduledExecutorService} on long running tasks.
   */
  private ScheduledExecutorService scheduleExecutor;

  //TODO(imysak): add backgroundExecutor in Context to share with TraverserWorkers
  /**
   * Executor for performing work in the background. This executor is general purpose and is
   * commonly used in conjunction with {@link #scheduleExecutor}.
   */
  private ExecutorService backgroundExecutor;

  /**
   * Connector schedule specified in the configuration.
   */
  private ConnectorSchedule connectorSchedule;

  /**
   * Task for calling {@link Connector#traverse} periodically.
   */
  private BackgroundRunnable traversalRunnable;

  private BackgroundRunnable incrementalTraversalRunnable;

  private final AtomicBoolean isRunning;

  /** Pointer to shutdown method to be executed when traversal is complete. */
  @FunctionalInterface
  public interface ShutdownHolder {
    /** Shutdown method to be executed when traversal is complete. */
    void shutdown();
  }

  protected ConnectorScheduler(
      AbstractBuilder<? extends AbstractBuilder, T> builder) {
    this.connector = checkNotNull(builder.connector);
    this.context = checkNotNull(builder.context);
    this.shutdownHolder = checkNotNull(builder.shutdownHolder);
    this.isRunning = new AtomicBoolean(false);
    this.connectorSchedule = new ConnectorSchedule();
  }

  protected ConnectorSchedule getConnectorSchedule() {
    return connectorSchedule;
  }

  protected ScheduledExecutorService getScheduledExecutor() {
    return scheduleExecutor;
  }

  protected ExecutorService getBackgroundExecutor() {
    return backgroundExecutor;
  }

  /** Starts traversal process and worker threads. */
  public synchronized void start() {
    checkNotNull(context);
    boolean success = isRunning.compareAndSet(false, true);
    checkState(success, "Connector traverser already started.");
    // Making executor service threads non daemon to avoid automatic shutdown of connector process.
    // TODO(tvartak) : Revert to use daemon threads after adding dashboard server which will keep
    // connector process running.
    scheduleExecutor =
        Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("schedule").build());
    backgroundExecutor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setDaemon(false).setNameFormat("background").build());
    ConnectorSchedule traversalSchedule = new ConnectorSchedule();

    if (traversalSchedule.isRunOnce()) {
      startToRunOnce(traversalSchedule);
    } else {
      startToRunContinuously(traversalSchedule);
    }

    // TODO(sfruhwald)
    // TODO(imysak): Temporary solution to print all stats once per 5 minute
    Runnable loggingStatsRunnable = new BackgroundRunnable(new OneAtATimeRunnable(
        () -> logger.info(StatsManager.getInstance().printStats()), "StatsLog"));
    scheduleExecutor.scheduleAtFixedRate(loggingStatsRunnable, 1, 5, TimeUnit.MINUTES);
  }

  private void startToRunContinuously(ConnectorSchedule traversalSchedule) {
    long initialDelay =
        traversalSchedule.isPerformTraversalOnStart()
            ? 0
            : traversalSchedule.getTraversalIntervalSeconds();
    traversalRunnable =
        new BackgroundRunnable(
            new OneAtATimeRunnable(
                new ConnectorTraversal(connector, context.getTraversalExceptionHandler()),
                "Traversal"));
    scheduleExecutor.scheduleAtFixedRate(
        traversalRunnable,
        initialDelay,
        traversalSchedule.getTraversalIntervalSeconds(),
        TimeUnit.SECONDS);
    if (connector instanceof IncrementalChangeHandler) {
      incrementalTraversalRunnable =
          new BackgroundRunnable(
              new OneAtATimeRunnable(
                  new IncrementalTraversal(
                      (IncrementalChangeHandler) connector,
                      context.getIncrementalTraversalExceptionHandler()),
                  "Incremental traversal"));
      long initialDelayIncremental =
          traversalSchedule.isPerformTraversalOnStart()
              ? traversalSchedule.getIncrementalTraversalIntervalSeconds()
              : 0;
      scheduleExecutor.scheduleAtFixedRate(
          incrementalTraversalRunnable,
          initialDelayIncremental,
          traversalSchedule.getIncrementalTraversalIntervalSeconds(),
          TimeUnit.SECONDS);
    }
  }

  private void startToRunOnce(ConnectorSchedule traversalSchedule) {
    if (connector instanceof IncrementalChangeHandler) {
      logger.log(Level.WARNING, "RunOnce mode. Incremental updates will be disabled.");
    }

    long initialDelay =
        traversalSchedule.isPerformTraversalOnStart()
            ? 0
            : traversalSchedule.getTraversalIntervalSeconds();
    scheduleExecutor.schedule(
        new ShutdownAfterCompleteRunnable(
            new ConnectorTraversal(connector, context.getTraversalExceptionHandler()))
        , initialDelay, TimeUnit.SECONDS);
  }

  /** Stops traversal process and worker threads. */
  public synchronized void stop() {
    checkState(isRunning.get(), "Connector traverser not started.");
    shutdownExecutor(scheduleExecutor);
    shutdownExecutor(backgroundExecutor);
    traversalRunnable = null;
    isRunning.set(false);
    logger.info(StatsManager.getInstance().printStats());
  }

  /**
   * Check if {@link ConnectorScheduler} is started.
   *
   * @return true if ConnectorScheduler started, false otherwise.
   */
  public boolean isStarted() {
    return isRunning.get();
  }

  private synchronized void shutdownExecutor(ExecutorService executor) {
    if ((executor == null) || executor.isShutdown()) {
      return;
    }
    executor.shutdown();
    try {
      executor.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      logger.log(Level.WARNING, "Interrupted during executor termination.", ex);
      Thread.currentThread().interrupt();
    }
    executor.shutdownNow();
  }

  protected abstract static class AbstractBuilder <B extends AbstractBuilder<B, T>,
      T extends ConnectorContext> {
    public Connector<T> connector;
    public T context;
    public ShutdownHolder shutdownHolder = () -> {};

    protected abstract B getThis();
    public abstract ConnectorScheduler<T> build();

    public B setConnector(Connector<T> connector) {
      this.connector = connector;
      return getThis();
    }

    public B setContext(T context) {
      this.context = context;
      return getThis();
    }

    public B setShutdownHolder(ShutdownHolder shutdownHolder) {
      this.shutdownHolder = shutdownHolder;
      return getThis();
    }
  }

  /** Builder for {@link ConnectorScheduler} instances. */
  public static class Builder extends AbstractBuilder<Builder, ConnectorContext> {
    @Override
    protected Builder getThis() {
      return this;
    }

    /** Build an instance of {@link ConnectorScheduler}. */
    @Override
    public ConnectorScheduler<ConnectorContext> build() {
      return new ConnectorScheduler<ConnectorContext>(this);
    }
  }

  /** Wrapper object to maintain connector traversal schedule. */
  protected static class ConnectorSchedule {
    // Number of seconds in a day 24 * 60 * 60
    private static final int NUM_SECONDS_IN_A_DAY = 86400;
    private final int traversalIntervalSecs;
    private final int incrementalTraversalSecs;
    private final int pollQueueIntervalSecs;
    private final boolean performTraversalOnStart;
    private final boolean runOnce;

    public ConnectorSchedule() {
      checkState(Configuration.isInitialized(), "configuration should be initialized");
      traversalIntervalSecs =
          Configuration.getInteger(Application.TRAVERSE_INTERVAL_SECONDS, NUM_SECONDS_IN_A_DAY)
              .get();
      incrementalTraversalSecs =
          Configuration.getInteger(Application.INCREMENTAL_INTERVAL_SECONDS, 300).get();
      performTraversalOnStart =
          Configuration.getBoolean(Application.TRAVERSE_ON_START, true).get();
      pollQueueIntervalSecs = Configuration.getInteger(Application.POLL_INTERVAL_SECONDS, 10).get();
      runOnce = Configuration.getBoolean(Application.RUN_ONCE, false).get();
    }

    public int getTraversalIntervalSeconds() {
      return traversalIntervalSecs;
    }

    public int getIncrementalTraversalIntervalSeconds() {
      return incrementalTraversalSecs;
    }

    public int getPollQueueIntervalSecs() {
      return pollQueueIntervalSecs;
    }

    public boolean isPerformTraversalOnStart() {
      return performTraversalOnStart;
    }

    public boolean isRunOnce() {
      return runOnce;
    }
  }

  private static class IncrementalTraversal implements Runnable {
    private final IncrementalChangeHandler incrementalChangeHandler;
    private final ExceptionHandler exceptionHandler;

    private IncrementalTraversal(
        IncrementalChangeHandler incrementalChangeHandler, ExceptionHandler exceptionHandler) {
      this.incrementalChangeHandler = checkNotNull(incrementalChangeHandler);
      this.exceptionHandler = checkNotNull(exceptionHandler);
    }

    private void incrementalTraversal() throws InterruptedException {
      logger.info("Beginning incremental traversal.");
      for (int ntries = 1; ; ntries++) {
        boolean keepGoing = true;
        try {
          incrementalChangeHandler.handleIncrementalChanges();
          break; // Success
        } catch (InterruptedException ex) {
          throw ex;
        } catch (Exception ex) {
          logger.log(Level.WARNING, "Exception during incremental traversal.", ex);
          keepGoing = exceptionHandler.handleException(ex, ntries);
        }
        if (keepGoing) {
          logger.log(Level.INFO, "Trying again... Number of attempts: {0}", ntries);
        } else {
          logger.warning("Failed incremental traversal.");
          return;
        }
      }
      logger.info("Completed incremental traversal.");
    }

    @Override
    public void run() {
      try {
        incrementalTraversal();
      } catch (InterruptedException ex) {
        logger.log(Level.WARNING, "Interrupted. Aborted connector traversal.", ex);
        Thread.currentThread().interrupt();
      } catch (Throwable t) {
        logger.log(Level.WARNING, "Failure during incremental traversal", t);
      }
    }
  }

  private static class ConnectorTraversal implements Runnable {
    private final Connector connector;
    private final ExceptionHandler handler;

    private ConnectorTraversal(Connector connector, ExceptionHandler handler) {
      this.connector = checkNotNull(connector);
      this.handler = checkNotNull(handler);
    }

    /**
     * Calls {@link Connector#traverse}. This method blocks until all traversal is complete or
     * retrying failed.
     */
    private void connectorTraversal() throws InterruptedException {
      logger.info("Beginning connector traversal.");
      for (int ntries = 1; ; ntries++) {
        boolean keepGoing = true;
        try {
          connector.traverse();
          break; // Success
        } catch (InterruptedException ex) {
          throw ex;
        } catch (Exception ex) {
          logger.log(Level.WARNING, "Exception during connector traversal.", ex);
          keepGoing = handler.handleException(ex, ntries);
        }
        if (keepGoing) {
          logger.log(Level.INFO, "Trying again... Number of attempts: {0}", ntries);
        } else {
          logger.warning("Failed connector traversal.");
          return;
        }
      }
      logger.info("Completed connector traversal.");
    }

    @Override
    public void run() {
      try {
        connectorTraversal();
      } catch (InterruptedException ex) {
        logger.log(Level.WARNING, "Interrupted. Aborted connector traversal.", ex);
        Thread.currentThread().interrupt();
      } catch (Throwable t) {
        logger.log(Level.WARNING, "Failure during traversal", t);
      }
    }
  }

  /**
   * Runnable that when invoked executes the delegate with {@link #getBackgroundExecutor} and then
   * returns before completion. That implies that uses of this class must ensure they do not add an
   * instance directly to {@link #getBackgroundExecutor}, otherwise an odd infinite loop will occur.
   */
  protected class BackgroundRunnable implements Runnable {
    private final Runnable delegate;

    public BackgroundRunnable(Runnable delegate) {
      this.delegate = checkNotNull(delegate);
    }

    @Override
    public void run() {
      try {
        backgroundExecutor.execute(delegate);
      } catch (Throwable t) {
        logger.log(Level.WARNING, "Failed to start background runnable", t);
      }
    }
  }

  protected class ShutdownAfterCompleteRunnable implements Runnable {
    private final Runnable toRun;

    ShutdownAfterCompleteRunnable(Runnable toRun) {
      this.toRun = toRun;
    }
    @Override
    public void run() {
      try {
        toRun.run();
      } finally {
        ConnectorScheduler.this.shutdownHolder.shutdown();
      }
    }
  }

  /**
   * {@link Runnable} implementation which allows only one thread to run given runnable at a time.
   */
  @VisibleForTesting
  public static class OneAtATimeRunnable implements Runnable {
    private AtomicBoolean isRunning = new AtomicBoolean();
    private final Runnable toRun;
    private final Runnable alreadyRunning;

    /**
     * Create {@link OneAtATimeRunnable} wrapper for {@code toRun}.
     *
     * @param toRun Runnable to run one at a time
     * @param tag message to log when when toRun is already running.
     */
    public OneAtATimeRunnable(Runnable toRun, String tag) {
      this(toRun, new AlreadyRunningRunnable(tag));
    }

    /**
     * Create {@link OneAtATimeRunnable} wrapper for {@code toRun}.
     *
     * @param toRun Runnable to run one at a time
     * @param alreadyRunning Runnable to run when {@code toRun} is already running.
     */
    public OneAtATimeRunnable(Runnable toRun, Runnable alreadyRunning) {
      this.toRun = checkNotNull(toRun);
      this.alreadyRunning = checkNotNull(alreadyRunning);
    }

    @Override
    public void run() {
      boolean success = isRunning.compareAndSet(false, true);
      if (!success) {
        alreadyRunning.run();
        return;
      }
      try {
        toRun.run();
      } finally {
        isRunning.set(false);
      }
    }
  }

  protected static class AlreadyRunningRunnable implements Runnable {
    private final String tag;

    AlreadyRunningRunnable(String tag) {
      this.tag = tag;
    }

    @Override
    public void run() {
      logger.log(
          Level.INFO, "Skipping current run for {0}. Previous invocation is still running.", tag);
    }
  }
}
