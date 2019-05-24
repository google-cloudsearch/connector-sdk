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

import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items;
import com.google.api.services.cloudsearch.v1.model.DebugOptions;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.api.services.cloudsearch.v1.model.PollItemsResponse;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.PushItemRequest;
import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service.State;
import com.google.common.util.concurrent.ServiceManager;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.ConnectorScheduler.OneAtATimeRunnable;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.IncrementalChangeHandler;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.RetryPolicy;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl.ServiceManagerHelper;
import com.google.enterprise.cloudsearch.sdk.indexing.traverser.TraverserConfiguration;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link ConnectorTraverser}. */

@RunWith(MockitoJUnitRunner.class)
public class ConnectorTraverserTest {
  static final Logger logger = Logger.getLogger(ConnectorTraverserTest.class.getName());
  static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  static final JsonObjectParser JSON_PARSER = new JsonObjectParser(JSON_FACTORY);

  static final List<String> PUSH_QUEUE_REQUEST =
      ImmutableList.of(
          "POST",
          "https://cloudsearch.googleapis.com/v1/indexing/customers/customerId"
              + "/datasources/sourceId/items:push");

  static final List<String> POLL_QUEUE_REQUEST =
      ImmutableList.of(
          "POST",
          "https://cloudsearch.googleapis.com/v1/indexing/customers/customerId"
              + "/datasources/sourceId/items:poll");

  private static final CredentialFactory credentialFactory =
      scopes -> new MockGoogleCredential.Builder()
      .setTransport(GoogleNetHttpTransport.newTrustedTransport())
      .setJsonFactory(JSON_FACTORY)
      .build();

  @Mock IndexingService mockIndexingService;
  @Mock BatchingIndexingService batchingService;
  @Mock ContentUploadService contentUploadService;
  @Mock ServiceManagerHelper serviceManagerHelper;

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  private abstract static class AbstractConnector implements IndexingConnector {

    @Override
    public void init(IndexingConnectorContext context) throws Exception {}

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

  private abstract static class ItemRetrieverConnector implements IndexingConnector, ItemRetriever {
    @Override
    public void traverse() throws IOException, InterruptedException {}

    @Override
    public void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException {}

    @Override
    public void destroy() {}
  }

  private abstract static class BatchItemRetrieverConnector
  implements IndexingConnector, BatchItemRetriever {
    @Override
    public void traverse() throws IOException, InterruptedException {}

    @Override
    public void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException {}

    @Override
    public void destroy() {}
  }

  private IndexingConnectorContextImpl.Builder getContextBuilder() {
    return new IndexingConnectorContextImpl.Builder().setIndexingService(mockIndexingService);
  }

  private IndexingConnectorContext getContextWithExceptionHandler(final int maxTries) {
    return getContextWithExceptionHandler(maxTries, maxTries);
  }

  private IndexingConnectorContext getContextWithExceptionHandler(
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
    new ConnectorTraverser.Builder()
        .setConnector(null)
        .setContext(getContextWithExceptionHandler(1, 1))
        .build();
  }

  @Test
  public void testConstructor() {
    setupConfig(Collections.emptyMap());
    ConnectorTraverser traverser =
        new ConnectorTraverser.Builder()
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
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
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
    IndexingConnectorContext context =
        new IndexingConnectorContextImpl.Builder().setIndexingService(mockIndexingService).build();
    NothingConnector connector = new NothingConnector(1);
    ConnectorTraverser traverser =
        new ConnectorTraverser.Builder()
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
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
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
    final CountDownLatch countDown = new CountDownLatch(1);
    IndexingConnector failedOnce =
        new AbstractConnector() {
          @Override
          public void traverse() throws IOException, InterruptedException {
            if (countDown.getCount() == 0) {
              fail("Unexpected traverse call.");
            }
            countDown.countDown();
            throw new IOException("Don't call me again.");
          }
        };
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
        .setConnector(failedOnce)
        .setContext(getContextWithExceptionHandler(1))
        .build();
    traverser.start();
    assertTrue(countDown.await(30, TimeUnit.SECONDS));
    traverser.stop();
  }

  @Test
  public void testTraverseRetryAndSuccess() throws Exception {
    setupConfig(Collections.emptyMap());
    final CountDownLatch counter = new CountDownLatch(4);
    final AtomicBoolean success = new AtomicBoolean(false);
    IndexingConnector fail3Times =
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
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
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
    IndexingConnector failedAlways =
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
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
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
    IndexingConnectorContext context =
        new IndexingConnectorContextImpl.Builder()
            .setIndexingService(mockIndexingService)
            .setIncrementalTraversalExceptionHandler(new RetryExceptionHandler(-1))
            .build();
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
        .setConnector(incremental)
        .setContext(context)
        .build();
    traverser.start();
    assertTrue(latch.await(30, TimeUnit.SECONDS));
    traverser.stop();
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
    IndexingConnectorContext context =
        new IndexingConnectorContextImpl.Builder()
            .setIndexingService(mockIndexingService)
            .setIncrementalTraversalExceptionHandler(new RetryExceptionHandler(4))
            .build();
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
        .setConnector(incremental)
        .setContext(context)
        .build();
    traverser.start();
    assertTrue(latch.await(30, TimeUnit.SECONDS));
    assertTrue(success.get());
    traverser.stop();
  }

  @Test
  // TODO: seems to be flaky..
  public void testItemRetriever() throws Exception {
    setupConfig(ImmutableMap.of("traverser.pollRequest.queue", "custom",
        "traverser.pollRequest.statuses", "ACCEPTED,MODIFIED"));
    final CountDownLatch countDown = new CountDownLatch(11);
    List<Item> polledEntries = new ArrayList<Item>();
    final Set<String> expectedIds = new HashSet<String>();
    for (int i = 1; i <= 10; i++) {
      String id = "Id-" + i;
      polledEntries.add(new Item().setName("datasources/sourceId/items/" + id).setQueue("custom"));
      expectedIds.add(id);
    }

    ItemRetrieverConnector connector =
        new ItemRetrieverConnector() {

          @Override
          public void process(Item entry) throws IOException, InterruptedException {
            assertTrue(expectedIds.remove(entry.getName()));
            countDown.countDown();
            if ("Id-3".equals(entry.getName())) {
              throw new RepositoryException.Builder()
                  .setErrorMessage("Repository Error with Id-3")
                  .build();
            }
          }

          @Override
          public void init(IndexingConnectorContext context) throws Exception {
            TraverserConfiguration traverserConfiguration =
                new TraverserConfiguration.Builder("default")
                    .name("test traverser")
                    .pollRequest(
                        new PollItemsRequest()
                            .setQueue("custom")
                            .setStatusCodes(Arrays.asList("ACCEPTED", "MODIFIED")))
                    .itemRetriever(this)
                    .build();

            context.registerTraverser(traverserConfiguration);
          }
        };

    IndexingServiceTransport transport = new IndexingServiceTransport();
    transport.registerPollRequest(
        new PollItemsRequest()
            .setStatusCodes(Arrays.asList("ACCEPTED", "MODIFIED"))
            .setQueue("custom")
            .setConnectorName("datasources/sourceId/connectors/unitTest"),
        new PollItemsResponse().setItems(polledEntries));
    transport.registerPollRequest(
        new PollItemsRequest()
            .setStatusCodes(Arrays.asList("ACCEPTED", "MODIFIED"))
            .setQueue("custom")
            .setConnectorName("datasources/sourceId/connectors/unitTest"),
        new PollItemsResponse().setItems(Collections.emptyList()));
    SettableFuture<GenericJson> pushResult = SettableFuture.create();
    PushItemRequest expectedRequest =
        new PushItemRequest()
            .setConnectorName("datasources/sourceId/connectors/unitTest")
            .setItem(
                new PushItem()
                    .setQueue("custom")
                    .setType("REPOSITORY_ERROR")
                    .setRepositoryError(
                        new RepositoryError().setErrorMessage("Repository Error with Id-3")))
                    .setDebugOptions(new DebugOptions().setEnableDebugging(false));
    doAnswer(
            invocation -> {
              Items.Push pushRequest = invocation.getArgument(0);
              assertEquals(expectedRequest, pushRequest.getJsonContent());
              pushResult.set(new GenericJson());
              countDown.countDown();
              return pushResult;
            })
        .when(batchingService)
        .pushItem(any());

    IndexingService indexingService = buildIndexingService(transport);
    IndexingConnectorContext context =
        new IndexingConnectorContextImpl.Builder().setIndexingService(indexingService).build();
    connector.init(context);
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
        .setConnector(connector)
        .setContext(context)
        .build();
    traverser.start();
    assertTrue(countDown.await(2, TimeUnit.SECONDS));
    assertThat(expectedIds, not(hasItem(anything())));
    traverser.stop();
    transport.validate();
  }

  @Test
  public void pollQueueIntervalSecs_succeeds() throws Exception {
    setupConfig(ImmutableMap.of(
            "schedule.pollQueueIntervalSecs", "3"
          ));
    ItemRetrieverConnector connector =
        new ItemRetrieverConnector() {

          @Override
          public void process(Item entry) throws IOException, InterruptedException {
          }

          @Override
          public void init(IndexingConnectorContext context) throws Exception {
          }
        };
    final CountDownLatch countDown = new CountDownLatch(2);
    doAnswer(
        invocation -> {
          // We can't hook into ParallelProcessingTraverserWorker's poll method, which is
          // what's being scheduled using pollQueueIntervalSecs, but it mainly just calls
          // IndexingService.poll, so use this to count invocations.
          countDown.countDown();
          // If we return nothing, ParallelProcessingTraverserWorker's poll method will
          // return and wait to be called again when scheduled. Otherwise,
          // IndexingService.poll is called repeatedly until the worker accumulates 1000
          // items.
          return Collections.emptyList();
        })
        .when(mockIndexingService)
        .poll(any());

    IndexingConnectorContext context = getContextWithExceptionHandler(-1);
    context.registerTraverser(
        new TraverserConfiguration.Builder().itemRetriever(connector).build());
    connector.init(context);
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
        .setConnector(connector)
        .setContext(context)
        .build();
    traverser.start();
    // Run once at start and then again 3 seconds later
    assertEquals(true, countDown.await(4, TimeUnit.SECONDS));
    traverser.stop();
  }

  private IndexingService buildIndexingService(IndexingServiceTransport transport) {
    try {
      when(batchingService.state()).thenReturn(State.NEW);
      when(contentUploadService.state()).thenReturn(State.NEW);
      doAnswer(invocation -> new ServiceManager(invocation.getArgument(0)))
          .when(serviceManagerHelper)
          .getServiceManager(Arrays.asList(batchingService, contentUploadService));
      IndexingService service =
          new IndexingServiceImpl.Builder()
              .setSourceId("sourceId")
              .setIdentitySourceId("identitySourceId")
              .setCredentialFactory(credentialFactory)
              .setJsonFactory(JSON_FACTORY)
              .setTransport(transport)
              .setRootUrl("")
              .setBatchingIndexingService(batchingService)
              .setContentUploadService(contentUploadService)
              .setServiceManagerHelper(serviceManagerHelper)
              .setRetryPolicy(new RetryPolicy.Builder().build())
              .setConnectorId("unitTest")
              .build();
      service.startAsync().awaitRunning();
      return service;
    } catch (IOException | GeneralSecurityException e) {
      fail("method should never go here");
      return null;
    }
  }

  @Test
  public void testBatchItemRetriever() throws Exception {
    setupConfig(ImmutableMap.of("traverser.pollRequest.queue", "custom",
        "traverser.pollRequest.statuses", "ACCEPTED,MODIFIED",
        "traverser.hostload", "1"
        ));
    final List<Item> polledEntries = new ArrayList<Item>();
    final CountDownLatch countDown = new CountDownLatch(1);
    final AtomicBoolean isMatched = new AtomicBoolean(false);
    final List<String> expectedIds = new ArrayList<String>();
    for (int i = 1; i <= 10; i++) {
      String id = "Id-" + i;
      polledEntries.add(new Item().setName("datasources/sourceId/items/" + id).setQueue("custom"));
      expectedIds.add(id);
    }

    BatchItemRetrieverConnector connector =
        new BatchItemRetrieverConnector() {

          @Override
          public void processBatch(List<Item> entries) throws IOException, InterruptedException {
            List<String> actualIds =
                entries.stream().map(e -> e.getName()).collect(Collectors.toList());
            isMatched.set(Objects.equals(expectedIds, actualIds));
            countDown.countDown();
            assertEquals(expectedIds, actualIds);
          }

          @Override
          public void init(IndexingConnectorContext context) throws Exception {
            TraverserConfiguration traverserConfiguration =
                new TraverserConfiguration.Builder()
                    .name("test traverser")
                    .pollRequest(
                        new PollItemsRequest()
                            .setQueue("custom")
                            .setStatusCodes(Arrays.asList("ACCEPTED", "MODIFIED")))
                    .itemRetriever(this)
                    .build();

            context.registerTraverser(traverserConfiguration);
          }
        };

    IndexingServiceTransport transport = new IndexingServiceTransport();
    transport.registerPollRequest(
        new PollItemsRequest()
            .setStatusCodes(Arrays.asList("ACCEPTED", "MODIFIED"))
            .setQueue("custom")
            .setConnectorName("datasources/sourceId/connectors/unitTest"),
        new PollItemsResponse().setItems(polledEntries));

    IndexingService indexingService = buildIndexingService(transport);

    IndexingConnectorContext context =
        new IndexingConnectorContextImpl.Builder().setIndexingService(indexingService).build();
    connector.init(context);
    ConnectorTraverser traverser = new ConnectorTraverser.Builder()
        .setConnector(connector)
        .setContext(context)
        .build();
    traverser.start();
    countDown.await(1L, TimeUnit.SECONDS);
    traverser.stop();
    transport.validate();
    assertTrue(isMatched.get());
  }

  private void setupConfig(Map<String, String> configuration) {
    Properties properties = new Properties();
    properties.putAll(configuration);
    properties.put("api.customerId", "customerId");
    properties.put("api.sourceId", "sourceId");
    setupConfig.initConfig(properties);
  }
}
