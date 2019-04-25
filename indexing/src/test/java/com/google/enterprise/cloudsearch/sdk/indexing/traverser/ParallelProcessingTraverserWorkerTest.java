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
package com.google.enterprise.cloudsearch.sdk.indexing.traverser;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import com.google.common.util.concurrent.Futures;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException.ErrorType;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

/** Tests for {@link ParallelProcessingTraverserWorker}. */

public class ParallelProcessingTraverserWorkerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock IndexingService indexingService;
  @Mock ItemRetriever itemRetriever;
  @Spy ExecutorService executorService = Executors.newCachedThreadPool();
  TraverserConfiguration conf;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    Properties config = new Properties();
    config.put("traverser.test.pollRequest.queue", "custom");
    config.put("traverser.test.pollRequest.statuses", "");
    config.put("traverser.test.hostload", "2");
    config.put("traverser.test.timeout", "60");
    config.put("traverser.test.timeunit", "SECONDS");
    setupConfig.initConfig(new Properties());
    conf = (new TraverserConfiguration.Builder("test"))
          .name("name")
          .itemRetriever(itemRetriever)
          .build();
  }

  /**
   * Test method for constructor.
   */
  @Test
  public void testParallelProcessingTraverserWorker(){
    new ParallelProcessingTraverserWorker(conf, indexingService, executorService);
  }

  /**
   * Test method for {@link ParallelProcessingTraverserWorker#poll()}.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testPoolAndProcess() throws IOException, InterruptedException{
    ParallelProcessingTraverserWorker worker = new ParallelProcessingTraverserWorker(
        conf, indexingService, executorService);
    List<Item> entries =
        Arrays.asList(
            (new Item()).setName("id-1").setQueue("custom"),
            (new Item()).setName("id-2").setQueue("custom"),
            (new Item()).setName("id-3").setQueue("custom"));
    final Set<String> expectedIds = new HashSet<String>(Arrays.asList("id-1", "id-2", "id-3"));
    final CountDownLatch countDown = new CountDownLatch(3);
    when(indexingService.poll(any(PollItemsRequest.class)))
        .thenReturn(entries)
        .thenReturn(Collections.emptyList());
    doCallRealMethod().when(executorService).execute(any());

    doAnswer(
            (invocation) -> {
              Object[] args = invocation.getArguments();
              Item entry = (Item) args[0];
              assertTrue(expectedIds.remove(entry.getName()));
              countDown.countDown();
              return null;
            })
        .when(itemRetriever)
        .process(any());

    worker.poll();
    countDown.await(1L, TimeUnit.SECONDS);
    assertTrue(expectedIds.isEmpty());
  }

  /**
   * Test method for {@link ParallelProcessingTraverserWorker#poll()} with {@link
   * ParallelProcessingTraverserWorker} handling {@link Error} thrown by {@link ItemRetriever}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testPollAndProcessHandleAssertionError() throws IOException, InterruptedException {
    conf =
        (new TraverserConfiguration.Builder("test"))
            .name("name")
            .itemRetriever(itemRetriever)
            .hostLoad(1)
            .build();
    ParallelProcessingTraverserWorker worker =
        new ParallelProcessingTraverserWorker(conf, indexingService, executorService);
    List<Item> entries =
        Arrays.asList(
            (new Item()).setName("id-1").setQueue("custom"),
            (new Item()).setName("id-2").setQueue("custom"),
            (new Item()).setName("id-3").setQueue("custom"));
    final Set<String> expectedIds = new HashSet<String>(Arrays.asList("id-1", "id-2", "id-3"));
    final CountDownLatch countDown = new CountDownLatch(3);
    when(indexingService.poll(any(PollItemsRequest.class)))
        .thenReturn(entries)
        .thenReturn(Collections.emptyList());
    doCallRealMethod().when(executorService).execute(any());

    doAnswer(
            (invocation) -> {
              Object[] args = invocation.getArguments();
              Item entry = (Item) args[0];
              assertTrue(expectedIds.remove(entry.getName()));
              countDown.countDown();
              if ("id-2".equals(entry.getName())) {
                throw new AssertionError();
              }
              return null;
            })
        .when(itemRetriever)
        .process(any());

    worker.poll();
    // If we skips one of the polled item (most likely due to bug), latch will never be 0. Timeout
    // here safeguards tests from being stuck.
    countDown.await(1L, TimeUnit.SECONDS);
    assertTrue(expectedIds.isEmpty());
  }

  /**
   * Test method for {@link ParallelProcessingTraverserWorker#poll()} with connector throwing {@link
   * RepositoryException}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRepositoryException() throws IOException, InterruptedException {
    ParallelProcessingTraverserWorker worker =
        new ParallelProcessingTraverserWorker(conf, indexingService, executorService);
    byte[] payload = "original".getBytes(StandardCharsets.UTF_8);
    Item queueItem = new Item().setName("id-1").setQueue("custom").encodePayload(payload);
    PushItem expectedPushItem =
        new PushItem()
            .setType("REPOSITORY_ERROR")
            .setQueue("custom")
            .encodePayload(payload)
            .setRepositoryError(
                new RepositoryError()
                    .setErrorMessage("Repository Error")
                    .setHttpStatusCode(500)
                    .setType(ErrorType.SERVER_ERROR.toString()));
    final CountDownLatch latch = new CountDownLatch(2);
    when(indexingService.poll(any(PollItemsRequest.class)))
        .thenReturn(Collections.singletonList(queueItem))
        .thenReturn(Collections.emptyList());
    doCallRealMethod().when(executorService).execute(any());

    doAnswer(
            (invocation) -> {
              latch.countDown();
              throw new RepositoryException.Builder()
                  .setErrorMessage("Repository Error")
                  .setErrorCode(500)
                  .setErrorType(ErrorType.SERVER_ERROR)
                  .build();
            })
        .when(itemRetriever)
        .process(eq(queueItem));
    doAnswer(
            invocation -> {
              latch.countDown();
              return Futures.immediateFuture(new Item());
            })
        .when(indexingService)
        .push(eq("id-1"), eq(expectedPushItem));

    worker.poll();
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  /**
   * Test method for {@link ParallelProcessingTraverserWorker#poll()} with connector throwing {@link
   * RepositoryException} wrapped in {@link IOException}.
   *
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testRepositoryExceptionWrappedInIOException()
      throws IOException, InterruptedException {
    ParallelProcessingTraverserWorker worker =
        new ParallelProcessingTraverserWorker(conf, indexingService, executorService);
    byte[] payload = "original".getBytes(StandardCharsets.UTF_8);
    Item queueItem = new Item().setName("id-1").setQueue("custom").encodePayload(payload);
    PushItem expectedPushItem =
        new PushItem()
            .setType("REPOSITORY_ERROR")
            .setQueue("custom")
            .encodePayload(payload)
            .setRepositoryError(
                new RepositoryError()
                    .setErrorMessage("Repository Error")
                    .setHttpStatusCode(500)
                    .setType(ErrorType.SERVER_ERROR.toString()));
    final CountDownLatch latch = new CountDownLatch(2);
    when(indexingService.poll(any(PollItemsRequest.class)))
        .thenReturn(Collections.singletonList(queueItem))
        .thenReturn(Collections.emptyList());
    doCallRealMethod().when(executorService).execute(any());

    doAnswer(
            (invocation) -> {
              latch.countDown();
              throw new IOException(
                  new RepositoryException.Builder()
                      .setErrorMessage("Repository Error")
                      .setErrorCode(500)
                      .setErrorType(ErrorType.SERVER_ERROR)
                      .build());
            })
        .when(itemRetriever)
        .process(eq(queueItem));
    doAnswer(
            invocation -> {
              latch.countDown();
              return Futures.immediateFuture(new Item());
            })
        .when(indexingService)
        .push(eq("id-1"), eq(expectedPushItem));

    worker.poll();
    assertTrue(latch.await(2, TimeUnit.SECONDS));
  }

  /**
   * Test method for {@link ParallelProcessingTraverserWorker#shutdown()}.
   * @throws InterruptedException
   */
  @Test
  public void testShutdown() throws InterruptedException{
    ParallelProcessingTraverserWorker worker = new ParallelProcessingTraverserWorker(
        conf, indexingService, executorService);
    when(executorService.isShutdown()).thenReturn(false);
    worker.shutdown();
    InOrder inOrderCheck = inOrder(executorService);
    inOrderCheck.verify(executorService).shutdown();
    inOrderCheck.verify(executorService).awaitTermination(10L, TimeUnit.SECONDS);
    inOrderCheck.verify(executorService).shutdownNow();
    inOrderCheck.verifyNoMoreInteractions();
  }

}
