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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.enterprise.cloudsearch.sdk.ConnectorScheduler.OneAtATimeRunnable;
import com.google.enterprise.cloudsearch.sdk.StatsManager.ResetStatsRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link ConnectorScheduler}. */
@RunWith(MockitoJUnitRunner.class)
public class ConnectorSchedulerTest {
  static final Logger logger = Logger.getLogger(ConnectorSchedulerTest.class.getName());

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetStatsRule resetStats = new ResetStatsRule();

  private abstract static class AbstractConnector implements Connector<ConnectorContext> {

    @Override
    public void init(ConnectorContext context) throws Exception {}

    @Override
    public void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException {}

    @Override
    public void destroy() {}
  }

  private static class NothingConnector extends AbstractConnector {
    private final CountDownLatch latch;

    public NothingConnector(int count) {
      latch = new CountDownLatch(count);
    }

    @Override
    public void traverse() throws IOException, InterruptedException {
      latch.countDown();
    }
  }

  private abstract static class IncrementalConnector extends AbstractConnector
      implements IncrementalChangeHandler {}

  private static class RetryExceptionHandler implements ExceptionHandler {

    private final int maxTries;

    public RetryExceptionHandler(int maxTries) {
      this.maxTries = maxTries;
    }

    @Override
    public boolean handleException(Exception ex, int ntries) throws InterruptedException {
      if (maxTries == -1) {
        fail("unexpected handleException call");
      }
      return ntries < maxTries;
    }
  }

  private ConnectorContextImpl.Builder getContextBuilder() {
    return new ConnectorContextImpl.Builder();
  }

  private ConnectorContext getContextWithExceptionHandler(final int maxTries) {
    return getContextWithExceptionHandler(maxTries, maxTries);
  }

  private ConnectorContext getContextWithExceptionHandler(
      final int maxTriesTraversal, final int maxTriesIncremental) {

    return getContextBuilder()
        .setTraversalExceptionHandler(new RetryExceptionHandler(maxTriesTraversal))
        .setIncrementalTraversalExceptionHandler(new RetryExceptionHandler(maxTriesIncremental))
        .build();
  }

  @Test
  public void testNullConnector() {
    setupConfig(Collections.emptyMap());
    thrown.expect(NullPointerException.class);
    new ConnectorScheduler.Builder()
        .setConnector(null)
        .setContext(getContextWithExceptionHandler(1, 1))
        .build();
  }

  @Test
  public void testConstructor() {
    setupConfig(Collections.emptyMap());
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(new NothingConnector(1))
            .setContext(getContextWithExceptionHandler(1, 1))
            .build();
    traverser.start();
    traverser.stop();
  }

  @Test
  public void testStartAndStop() throws Exception {
    setupConfig(Collections.emptyMap());
    NothingConnector connector = new NothingConnector(1);
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(connector)
            .setContext(getContextWithExceptionHandler(-1))
            .build();
    traverser.start();
    assertTrue(connector.latch.await(30, TimeUnit.SECONDS));
    traverser.stop();
  }

  @Test
  public void testStartAndRunOnce() throws Exception {
    setupConfig(Collections.singletonMap("connector.runOnce", "true"));
    CountDownLatch shutdown = new CountDownLatch(1);
    ConnectorContext context = new ConnectorContextImpl.Builder().build();
    NothingConnector connector = new NothingConnector(1);
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(connector)
            .setContext(context)
            .setShutdownHolder(() -> shutdown.countDown())
            .build();
    traverser.start();
    assertTrue(connector.latch.await(30, TimeUnit.SECONDS));
    assertTrue(shutdown.await(30, TimeUnit.SECONDS));
  }

  @Test
  public void testStartAlreadyStarted() throws Exception {
    setupConfig(Collections.emptyMap());
    NothingConnector connector = new NothingConnector(1);
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(connector)
            .setContext(getContextWithExceptionHandler(-1))
            .build();
    traverser.start();
    thrown.expect(IllegalStateException.class);
    traverser.start();
  }

  @Test
  public void testTraverseNoRetry() throws Exception {
    setupConfig(Collections.emptyMap());
    final CountDownLatch counter = new CountDownLatch(1);
    Connector<ConnectorContext> failedOnce =
        new AbstractConnector() {
          @Override
          public void traverse() throws IOException, InterruptedException {
            if (counter.getCount() == 0) {
              fail("Unexpected traverse call.");
            }
            counter.countDown();
            throw new IOException("Don't call me again.");
          }
        };
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(failedOnce)
            .setContext(getContextWithExceptionHandler(1))
            .build();
    traverser.start();
    assertTrue(counter.await(30, TimeUnit.SECONDS));
    traverser.stop();
  }

  @Test
  public void testTraverseRetryAndSuccess() throws Exception {
    setupConfig(Collections.emptyMap());
    final CountDownLatch counter = new CountDownLatch(4);
    final AtomicBoolean success = new AtomicBoolean(false);
    Connector<ConnectorContext> fail3Times =
        new AbstractConnector() {
          @Override
          public void traverse() throws IOException, InterruptedException {
            assertFalse(success.get());
            if (counter.getCount() == 0) {
              fail("Unexpected traverse call.");
            }
            // Fail for 3 times before success
            if (counter.getCount() > 1) {
              counter.countDown();
              throw new IOException("Try 3 times");
            }
            success.set(true);
            counter.countDown();
          }
        };
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(fail3Times)
            .setContext(getContextWithExceptionHandler(4))
            .build();
    traverser.start();
    assertTrue(counter.await(30, TimeUnit.SECONDS));
    assertTrue(success.get());
    traverser.stop();
  }

  @Test
  public void testTraverseRetryAndFail() throws Exception {
    setupConfig(Collections.emptyMap());
    CountDownLatch latch = new CountDownLatch(3);
    Connector<ConnectorContext> failedAlways =
        new AbstractConnector() {
          @Override
          public void traverse() throws IOException, InterruptedException {
            if (latch.getCount() == 0) {
              fail("Unexpected traverse call.");
            }
            latch.countDown();
            throw new IOException("Always exception");
          }
        };
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(failedAlways)
            .setContext(getContextWithExceptionHandler(3))
            .build();
    traverser.start();
    assertTrue(latch.await(30, TimeUnit.SECONDS));
    traverser.stop();
  }

  @Test
  public void testTraverseLongRunning() throws Exception {
    CountDownLatch alreadyRunningLatch = new CountDownLatch(2);
    Runnable alreadyRunningRunnable =
        () -> {
          assertThat(alreadyRunningLatch.getCount(), not(equalTo(0)));
          alreadyRunningLatch.countDown();
        };
    CountDownLatch longRunningLatch = new CountDownLatch(2);
    CountDownLatch firstRunLatch = new CountDownLatch(1);
    Runnable longRunningRunnable =
        () -> {
          try {
            assertTrue(alreadyRunningLatch.await(30, TimeUnit.SECONDS));
            longRunningLatch.countDown();
            firstRunLatch.countDown();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
          }
        };

    OneAtATimeRunnable subject =
        new OneAtATimeRunnable(longRunningRunnable, alreadyRunningRunnable);
    Thread thread1 = new Thread(subject);
    Thread thread2 = new Thread(subject);
    Thread thread3 = new Thread(subject);
    Thread thread4 = new Thread(subject);
    thread1.start();
    thread2.start();
    thread3.start();
    // Try to re-run task after initial task is done.
    assertTrue(firstRunLatch.await(30, TimeUnit.SECONDS));
    thread4.start();
    assertTrue(longRunningLatch.await(30, TimeUnit.SECONDS));
    assertTrue(alreadyRunningLatch.await(0, TimeUnit.SECONDS));
  }

  @Test
  public void traverseIntervalSecs_traverseOnStartTrue_succeeds() throws Exception {
    int traverseIntervalSecs = 2;
    int latchWaitSecs = 6;
    Map<String, String> config = new HashMap<>();
    config.put(Application.TRAVERSE_INTERVAL_SECONDS, String.valueOf(traverseIntervalSecs));
    config.put(Application.TRAVERSE_ON_START, "true");
    config.put(Application.RUN_ONCE, "false");

    setupConfig(config);
    NothingConnector connector = new NothingConnector(2);
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(connector)
            .setContext(getContextWithExceptionHandler(-1))
            .build();
    traverser.start();
    assertTrue(connector.latch.await(latchWaitSecs, TimeUnit.SECONDS));
    traverser.stop();
  }

  @Test
  public void traverseIntervalSecs_traverseOnStartFalse_waitsToStart() throws Exception {
    int traverseIntervalSecs = 4;
    int latchWaitSecs = 2;
    Map<String, String> config = new HashMap<>();
    config.put(Application.TRAVERSE_INTERVAL_SECONDS, String.valueOf(traverseIntervalSecs));
    config.put(Application.TRAVERSE_ON_START, "false");
    config.put(Application.RUN_ONCE, "false");

    setupConfig(config);
    NothingConnector connector = new NothingConnector(2);
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(connector)
            .setContext(getContextWithExceptionHandler(-1))
            .build();
    traverser.start();
    // Latch await times out before traverse is called the first time.
    assertEquals(false, connector.latch.await(latchWaitSecs, TimeUnit.SECONDS));
    // Traverse called (at least) twice.
    assertEquals(true, connector.latch.await(traverseIntervalSecs * 2, TimeUnit.SECONDS));
    traverser.stop();
  }

  @Test
  public void testIncrementalTraversal() throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    IncrementalConnector incremental =
        new IncrementalConnector() {

          @Override
          public void traverse() throws IOException, InterruptedException {
            throw new UnsupportedOperationException("Traversal disabled on start");
          }

          @Override
          public void handleIncrementalChanges() throws IOException, InterruptedException {
            latch.countDown();
          }
        };
    setupConfig(Collections.singletonMap("schedule.performTraversalOnStart", "false"));
    ConnectorContext context =
        new ConnectorContextImpl.Builder()
            .setIncrementalTraversalExceptionHandler(new RetryExceptionHandler(-1))
            .build();
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder().setConnector(incremental).setContext(context).build();
    traverser.start();
    assertTrue(latch.await(30, TimeUnit.SECONDS));
    traverser.stop();

    assertEquals(
        1,
        StatsManager.getInstance()
            .getComponent("IncrementalTraverser")
            .getSuccessCount("complete"));
  }

  @Test
  public void testIncrementalTraversalRetryAndSuccess() throws Exception {
    final CountDownLatch latch = new CountDownLatch(4);
    final AtomicBoolean success = new AtomicBoolean(false);
    IncrementalConnector incremental =
        new IncrementalConnector() {

          @Override
          public void traverse() throws IOException, InterruptedException {
            throw new UnsupportedOperationException("Traversal disabled on start");
          }

          @Override
          public void handleIncrementalChanges() throws IOException, InterruptedException {
            if (latch.getCount() == 0) {
              fail("Unexpected incremental traverse call.");
            }
            try {
              if (latch.getCount() > 1) {
                throw new IOException("Service unavailable");
              } else {
                success.set(true);
              }
            } finally {
              latch.countDown();
            }
          }
        };
    setupConfig(Collections.singletonMap("schedule.performTraversalOnStart", "false"));
    ConnectorContext context =
        new ConnectorContextImpl.Builder()
            .setIncrementalTraversalExceptionHandler(new RetryExceptionHandler(4))
            .build();
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder().setConnector(incremental).setContext(context).build();
    traverser.start();
    assertTrue(latch.await(30, TimeUnit.SECONDS));
    assertTrue(success.get());
    traverser.stop();
    assertEquals(
        1,
        StatsManager.getInstance()
            .getComponent("IncrementalTraverser")
            .getSuccessCount("complete"));
  }

  @Test
  public void incrementalTraversalIntervalSecs_traverseOnStartFalse_calledImmediately()
      throws Exception {
    final CountDownLatch latch = new CountDownLatch(1);
    IncrementalConnector incremental =
        new IncrementalConnector() {

          @Override
          public void traverse() throws IOException, InterruptedException {
          }

          @Override
          public void handleIncrementalChanges() throws IOException, InterruptedException {
            latch.countDown();
          }
        };
    int incrementalTraversalIntervalSecs = 10;
    int latchWaitSecs = 2;
    Map<String, String> config = new HashMap<>();
    config.put(Application.INCREMENTAL_INTERVAL_SECONDS,
        String.valueOf(incrementalTraversalIntervalSecs));
    config.put(Application.TRAVERSE_ON_START, "false");
    config.put(Application.RUN_ONCE, "false");
    setupConfig(config);
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(incremental)
            .setContext(getContextWithExceptionHandler(-1))
            .build();
    traverser.start();
    assertEquals(true, latch.await(latchWaitSecs, TimeUnit.SECONDS));
    traverser.stop();
  }

  @Test
  public void incrementalTraversalIntervalSecs_traverseOnStartTrue_waitsToStart()
      throws Exception {
    final CountDownLatch latch = new CountDownLatch(2);
    IncrementalConnector incremental =
        new IncrementalConnector() {

          @Override
          public void traverse() throws IOException, InterruptedException {
          }

          @Override
          public void handleIncrementalChanges() throws IOException, InterruptedException {
            latch.countDown();
          }
        };
    int incrementalTraversalIntervalSecs = 4;
    int latchWaitSecs = 2;
    Map<String, String> config = new HashMap<>();
    config.put(Application.INCREMENTAL_INTERVAL_SECONDS,
        String.valueOf(incrementalTraversalIntervalSecs));
    config.put(Application.TRAVERSE_ON_START, "true");
    config.put(Application.RUN_ONCE, "false");
    setupConfig(config);
    ConnectorScheduler<ConnectorContext> traverser =
        new ConnectorScheduler.Builder()
            .setConnector(incremental)
            .setContext(getContextWithExceptionHandler(-1))
            .build();
    traverser.start();
    assertEquals(false, latch.await(latchWaitSecs, TimeUnit.SECONDS));
    assertEquals(true, latch.await(incrementalTraversalIntervalSecs * 2, TimeUnit.SECONDS));
    traverser.stop();
  }

  @Test
  public void connectorSchedule_configDefaults_valuesSet() {
    setupConfig(Collections.emptyMap());
    ConnectorScheduler.ConnectorSchedule schedule = new ConnectorScheduler.ConnectorSchedule();
    assertEquals(86400, schedule.getTraversalIntervalSeconds());
    assertEquals(300, schedule.getIncrementalTraversalIntervalSeconds());
    assertEquals(10, schedule.getPollQueueIntervalSecs());
    assertEquals(true, schedule.isPerformTraversalOnStart());
    assertEquals(false, schedule.isRunOnce());
  }

  @Test
  public void connectorSchedule_configProperties_valuesSet() {
    Map<String, String> config = new HashMap<>();
    config.put(Application.TRAVERSE_INTERVAL_SECONDS, "42");
    config.put(Application.INCREMENTAL_INTERVAL_SECONDS, "11");
    config.put(Application.POLL_INTERVAL_SECONDS, "22");
    config.put(Application.TRAVERSE_ON_START, "false");
    config.put(Application.RUN_ONCE, "true");
    setupConfig(config);
    ConnectorScheduler.ConnectorSchedule schedule = new ConnectorScheduler.ConnectorSchedule();
    assertEquals(42, schedule.getTraversalIntervalSeconds());
    assertEquals(11, schedule.getIncrementalTraversalIntervalSeconds());
    assertEquals(22, schedule.getPollQueueIntervalSecs());
    assertEquals(false, schedule.isPerformTraversalOnStart());
    assertEquals(true, schedule.isRunOnce());
  }

  private void setupConfig(Map<String, String> configuration) {
    Properties properties = new Properties();
    properties.putAll(configuration);
    properties.put("api.customerId", "customerId");
    properties.put("api.sourceId", "sourceId");
    setupConfig.initConfig(properties);
  }
}
