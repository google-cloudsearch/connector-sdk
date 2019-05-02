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
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.util.BackOff;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.enterprise.cloudsearch.sdk.AsyncRequest.SettableFutureCallback;
import com.google.enterprise.cloudsearch.sdk.AsyncRequest.Status;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.BatchRequestHelper;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.ExecutorFactory;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.ScheduleFlushRunnable;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.SnapshotRunnable;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.TimeProvider;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.StatsManager.ResetStatsRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link BatchRequestService}. */

@RunWith(MockitoJUnitRunner.class)
public class BatchRequestServiceTest {
  @Mock private ExecutorFactory executorFactory;
  @Mock private ScheduledExecutorService scheduleExecutorService;
  @Mock private TimeProvider currentTimeProvider;
  @Mock private AbstractGoogleJsonClient service;
  @Mock private AsyncRequest<GenericJson> asyncRequest;
  @Mock private AbstractGoogleJsonClientRequest<GenericJson> testRequest;
  @Mock private HttpTransport httpTransport;
  @Mock private HttpRequestInitializer httpRequestInitializer;
  @Mock private BatchRequestHelper batchRequestHelper;
  @Mock private GoogleCredential credential;
  @Mock private BackOff backOff;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private OperationStats operationStats;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private RetryPolicy retryPolicy;

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetStatsRule resetStats = new ResetStatsRule();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  private BatchRequestService setupService() {
    when(executorFactory.getExecutor()).thenReturn(MoreExecutors.newDirectExecutorService());
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);
    when(retryPolicy.getBackOffFactory().createBackOffInstance()).thenReturn(backOff);
    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setRetryPolicy(retryPolicy)
            .build();
    return batchService;
  }

  @Test
  public void testDefaultBuilder() {
    BatchRequestService batchService =
        new BatchRequestService.Builder(service).setGoogleCredential(credential).build();
    checkNotNull(batchService);
  }

  @Test
  public void testNullBatchRequest() {
    thrown.expect(NullPointerException.class);
    new BatchRequestService.Builder(null).setExecutorFactory(executorFactory).build();
  }

  @Test
  public void testNullExecutors() {
    thrown.expect(NullPointerException.class);
    new BatchRequestService.Builder(service).setExecutorFactory(null).build();
  }

  @Test
  public void testNullBatchPolicy() {
    thrown.expect(NullPointerException.class);
    new BatchRequestService.Builder(service)
        .setExecutorFactory(executorFactory)
        .setBatchPolicy(null)
        .build();
  }

  @Test
  public void testNotStartedBatch() throws InterruptedException {
    BatchRequestService batchService =
        new BatchRequestService.Builder(service).setGoogleCredential(credential).build();
    assertFalse(batchService.isRunning());
    thrown.expect(IllegalStateException.class);
    batchService.add(asyncRequest);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFlushSetsHttpInterceptorOnBatchRequest() throws Exception {
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    ArgumentCaptor<HttpRequestInitializer> requestInitializerCaptor =
        ArgumentCaptor.forClass(HttpRequestInitializer.class);

    AsyncRequest<GenericJson> requestToBatch = Mockito.mock(AsyncRequest.class);
    AsyncRequest<GenericJson> requestToBatch2 = Mockito.mock(AsyncRequest.class);
    SettableFutureCallback<GenericJson> callback = Mockito.mock(SettableFutureCallback.class);
    SettableFutureCallback<GenericJson> callback2 = Mockito.mock(SettableFutureCallback.class);
    when(requestToBatch.getCallback()).thenReturn(callback);
    when(requestToBatch2.getCallback()).thenReturn(callback2);

    when(batchRequestHelper.createBatch(requestInitializerCaptor.capture()))
        .thenReturn(batchRequest);

    when(requestToBatch.getStatus()).thenReturn(Status.COMPLETED);
    when(requestToBatch2.getStatus()).thenReturn(Status.COMPLETED);

    batchService.add(requestToBatch);
    batchService.add(requestToBatch2);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();

    // verify the used requestInitializer
    HttpRequestInitializer requestInitializer = requestInitializerCaptor.getValue();
    assertThat(requestInitializer,
        instanceOf(BatchRequestService.EventLoggingRequestInitializer.class));
    HttpRequest testRequest =
        new MockHttpTransport().createRequestFactory().buildDeleteRequest(null);
    requestInitializer.initialize(testRequest);
    testRequest.getInterceptor().intercept(testRequest);
    verify(callback).onStart();
    verify(callback2).onStart();
    BatchPolicy defaultBatchPolicy = new BatchPolicy.Builder().build();
    assertEquals(
        defaultBatchPolicy.getBatchConnectTimeoutSeconds() * 1000,
        testRequest.getConnectTimeout());
    assertEquals(
        defaultBatchPolicy.getBatchReadTimeoutSeconds() * 1000,
        testRequest.getReadTimeout());
  }

  @Test
  public void testFlush() throws Exception {
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    doAnswer(
            invocation -> {
              requestToBatch.getCallback().onStart();
              requestToBatch.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    assertEquals(new GenericJson(), requestToBatch.getFuture().get());
    assertFalse(batchService.isRunning());
  }

  @Test
  public void testFlushOnShutdown() throws Exception {
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    doAnswer(
            invocation -> {
              requestToBatch.getCallback().onStart();
              requestToBatch.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.stopAsync().awaitTerminated();
    assertEquals(new GenericJson(), requestToBatch.getFuture().get());
    assertFalse(batchService.isRunning());
  }

  @Test
  public void testFailedResult() throws Exception {
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    GoogleJsonError error = new GoogleJsonError();
    error.setCode(403);
    error.setMessage("unauthorized");
    doAnswer(
            invocation -> {
              requestToBatch.getCallback().onFailure(error, new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    validateFailedResult(requestToBatch.getFuture());
    assertFalse(batchService.isRunning());
  }

  @Test
  public void testBatchRequestIOException() throws Exception {
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    GoogleJsonError error = new GoogleJsonError();
    error.setCode(403);
    error.setMessage("unauthorized");
    doAnswer(
            invocation -> {
              throw new IOException();
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    validateFailedResult(requestToBatch.getFuture());
    assertFalse(batchService.isRunning());
  }

  @Test
  public void testBatchRequestSocketTimeoutException() throws Exception {
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    when(retryPolicy.getMaxRetryLimit()).thenReturn(1);
    when(retryPolicy.isRetryableStatusCode(504)).thenReturn(true);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    AtomicInteger counter = new AtomicInteger();
    GenericJson successfulResult = new GenericJson();
    doAnswer(
            invocation -> {
              if (counter.incrementAndGet() == 1) {
                throw new SocketTimeoutException();
              }
              requestToBatch.getCallback().onStart();
              requestToBatch.getCallback().onSuccess(successfulResult, new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    assertEquals(successfulResult, requestToBatch.getFuture().get());
    assertEquals(Status.COMPLETED, requestToBatch.getStatus());
    assertEquals(1, requestToBatch.getRetries());
    assertFalse(batchService.isRunning());
  }

  @Test
  public void testGenericJsonFlush() throws Exception {
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    assertFalse(batchService.isRunning());
    verifyNoMoreInteractions(batchRequestHelper);
  }

  @Test
  public void testCancelOnShutdown() throws Exception {
    when(executorFactory.getExecutor()).thenReturn(MoreExecutors.newDirectExecutorService());
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);
    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setGoogleCredential(credential)
            .setBatchPolicy(new BatchPolicy.Builder().setFlushOnShutdown(false).build())
            .build();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    batchService.add(requestToBatch);
    batchService.stopAsync().awaitTerminated();
    assertTrue(requestToBatch.getFuture().isCancelled());
    assertEquals(Status.CANCELLED, requestToBatch.getStatus());
    assertFalse(batchService.isRunning());
    verifyNoMoreInteractions(service);
  }

  @Test
  public void testAutoFlushMaxSize() throws Exception {
    when(executorFactory.getExecutor()).thenReturn(MoreExecutors.newDirectExecutorService());
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);
    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(
                new BatchPolicy.Builder().setMaxBatchSize(2).setFlushOnShutdown(false).build())
            .build();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    AsyncRequest<GenericJson> requestToBatch1 =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    AsyncRequest<GenericJson> requestToBatch2 =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    AsyncRequest<GenericJson> requestToBatch3 =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    BatchRequest firstBatch = getMockBatchRequest();
    BatchRequest secondBatch = getMockBatchRequest();
    AtomicBoolean firstBatchExecuted = new AtomicBoolean();
    AtomicBoolean secondbatchExecuted = new AtomicBoolean();
    doAnswer(
            invocation -> {
              requestToBatch1.getCallback().onStart();
              requestToBatch1.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              requestToBatch2.getCallback().onStart();
              requestToBatch2.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              firstBatchExecuted.set(true);
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(firstBatch);
    doAnswer(
            invocation -> {
              requestToBatch3.getCallback().onStart();
              requestToBatch3.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              secondbatchExecuted.set(true);
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(secondBatch);
    when(batchRequestHelper.createBatch(any())).thenReturn(firstBatch, secondBatch);
    batchService.add(requestToBatch1);
    batchService.add(requestToBatch2);
    batchService.add(requestToBatch3);

    assertEquals(1, batchService.getCurrentBatchSize());
    assertTrue(firstBatchExecuted.get());
    assertFalse(secondbatchExecuted.get());
    ListenableFuture<Integer> remaining = batchService.flush();
    assertTrue(secondbatchExecuted.get());
    batchService.stopAsync().awaitTerminated();
    assertEquals(new GenericJson(), requestToBatch1.getFuture().get());
    assertEquals(new GenericJson(), requestToBatch2.getFuture().get());
    assertEquals(new GenericJson(), requestToBatch3.getFuture().get());
    assertEquals(Status.COMPLETED, requestToBatch1.getStatus());
    assertEquals(Status.COMPLETED, requestToBatch2.getStatus());
    assertEquals(Status.COMPLETED, requestToBatch3.getStatus());
    assertEquals((Integer) 1, remaining.get());
    assertFalse(batchService.isRunning());
    InOrder inOrder = inOrder(batchRequestHelper);
    inOrder.verify(batchRequestHelper).createBatch(any());
    inOrder
        .verify(batchRequestHelper)
        .queue(
            eq(firstBatch),
            eq(requestToBatch1.getRequest()),
            argThat(new EqualityMatcher<>(requestToBatch1.getCallback())));
    inOrder
        .verify(batchRequestHelper)
        .queue(
            eq(firstBatch),
            eq(requestToBatch2.getRequest()),
            argThat(new EqualityMatcher<>(requestToBatch2.getCallback())));
    inOrder.verify(batchRequestHelper).executeBatchRequest(firstBatch);
    inOrder.verify(batchRequestHelper).createBatch(any());
    inOrder
        .verify(batchRequestHelper)
        .queue(
            eq(secondBatch),
            eq(requestToBatch3.getRequest()),
            argThat(new EqualityMatcher<>(requestToBatch3.getCallback())));
    inOrder.verify(batchRequestHelper).executeBatchRequest(secondBatch);
    verifyNoMoreInteractions(batchRequestHelper);
    verifyNoMoreInteractions(backOff);
  }

  @Test
  public void testRejectedByExecutor() throws Exception {
    ExecutorService rejectExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(rejectExecutor);
    doAnswer(
            invocation -> {
              throw new RejectedExecutionException();
            })
        .when(rejectExecutor)
        .execute(any());
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);
    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setGoogleCredential(credential)
            .setBatchPolicy(new BatchPolicy.Builder().setFlushOnShutdown(false).build())
            .build();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    batchService.add(requestToBatch);
    try {
      batchService.flush();
      fail("missing RejectedExecutionException");
    } catch (RejectedExecutionException expected) {
    }
    batchService.stopAsync().awaitTerminated();
    validateFailedResult(requestToBatch.getFuture());
    assertFalse(batchService.isRunning());
  }

  @Test
  public void testScheduledAutoFlush() throws Exception {
    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);
    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setTimeProvider(currentTimeProvider)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(
                new BatchPolicy.Builder()
                    .setMaxBatchDelay(10, TimeUnit.SECONDS)
                    .setFlushOnShutdown(false)
                    .build())
            .build();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    long currentTimeMillis = System.currentTimeMillis();
    when(currentTimeProvider.currentTimeMillis()).thenReturn(currentTimeMillis);
    doAnswer(
            invocation -> {
              ((SnapshotRunnable) invocation.getArgument(0)).run();
              return null;
            })
        .when(batchExecutor)
        .execute(isA(SnapshotRunnable.class));
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    doAnswer(
            invocation -> {
              requestToBatch.getCallback().onStart();
              requestToBatch.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    ArgumentCaptor<ScheduleFlushRunnable> scheduleTaskCaptor =
        ArgumentCaptor.forClass(ScheduleFlushRunnable.class);
    verify(scheduleExecutorService)
        .schedule(scheduleTaskCaptor.capture(), eq(10L), eq(TimeUnit.SECONDS));
    scheduleTaskCaptor.getValue().run();
    assertEquals(new GenericJson(), requestToBatch.getFuture().get());
    verify(batchExecutor).execute(isA(SnapshotRunnable.class));
    batchService.stopAsync().awaitTerminated();
    assertFalse(batchService.isRunning());
    verify(batchExecutor).shutdown();
    verify(batchExecutor).awaitTermination(10L, TimeUnit.SECONDS);
    verify(batchExecutor).shutdownNow();
    verify(scheduleExecutorService).shutdown();
    verify(scheduleExecutorService).awaitTermination(10L, TimeUnit.SECONDS);
    verify(scheduleExecutorService).shutdownNow();
    verifyNoMoreInteractions(batchExecutor, scheduleExecutorService);
    verifyNoMoreInteractions(backOff);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAllRetriesFailed() throws Exception {
    int httpErrorCode = 503;
    when(retryPolicy.isRetryableStatusCode(httpErrorCode)).thenReturn(true);
    int retries = 3;
    when(retryPolicy.getMaxRetryLimit()).thenReturn(retries);
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    assertEquals(0, requestToBatch.getRetries());
    assertEquals(Status.NEW, requestToBatch.getStatus());
    GoogleJsonError error = new GoogleJsonError();
    error.setCode(httpErrorCode);
    error.setMessage("Service Unavailable");

    doAnswer(
            i -> {
              requestToBatch.getCallback().onFailure(error, new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    verify(retryPolicy, times(retries + 1)).isRetryableStatusCode(httpErrorCode);
    verify(retryPolicy, times(retries + 1)).getMaxRetryLimit();
    validateFailedResult(requestToBatch.getFuture());
    assertEquals(Status.FAILED, requestToBatch.getStatus());
    assertEquals(retries, requestToBatch.getRetries());
    assertFalse(batchService.isRunning());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNonRetryableErrorCode() throws Exception {
    int httpErrorCode = 65535;
    when(retryPolicy.isRetryableStatusCode(httpErrorCode)).thenReturn(false);
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    assertEquals(0, requestToBatch.getRetries());
    assertEquals(Status.NEW, requestToBatch.getStatus());
    GoogleJsonError error = new GoogleJsonError();
    error.setCode(httpErrorCode);
    error.setMessage("Unknown error code");

    doAnswer(
            i -> {
              requestToBatch.getCallback().onFailure(error, new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    verify(retryPolicy).isRetryableStatusCode(httpErrorCode);
    validateFailedResult(requestToBatch.getFuture());
    assertEquals(Status.FAILED, requestToBatch.getStatus());
    assertEquals(0, requestToBatch.getRetries());
    assertFalse(batchService.isRunning());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testNonRetryableException() throws Exception {
    when(retryPolicy.isRetryableStatusCode(0)).thenReturn(false);
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    assertEquals(0, requestToBatch.getRetries());
    assertEquals(Status.NEW, requestToBatch.getStatus());

    doAnswer(
            i -> {
              throw new IOException("Non-retryable exception");
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    verify(retryPolicy).isRetryableStatusCode(0);
    validateFailedResult(requestToBatch.getFuture());
    assertEquals(Status.FAILED, requestToBatch.getStatus());
    assertEquals(0, requestToBatch.getRetries());
    assertFalse(batchService.isRunning());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testInterruptedExceptionWhileRetryingRequests() throws Exception {
    when(retryPolicy.isRetryableStatusCode(0)).thenReturn(false);
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatchSuccessful =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    AsyncRequest<GenericJson> requestToBatchFailed =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    assertEquals(0, requestToBatchSuccessful.getRetries());
    assertEquals(Status.NEW, requestToBatchSuccessful.getStatus());

    doAnswer(
            invocation -> {
              requestToBatchSuccessful.getCallback().onStart();
              requestToBatchSuccessful
                  .getCallback()
                  .onSuccess(new GenericJson(), new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);
    doThrow(new InterruptedException()).doNothing().when(batchRequestHelper).sleep(any(Long.class));

    batchService.add(requestToBatchSuccessful);
    batchService.add(requestToBatchFailed);
    Future<Integer> result = batchService.flush();
    Thread.interrupted(); // Clear interrupted flag
    batchService.stopAsync().awaitTerminated();
    verify(retryPolicy).isRetryableStatusCode(0);
    assertEquals(Status.COMPLETED, requestToBatchSuccessful.getStatus());
    assertEquals(Status.FAILED, requestToBatchFailed.getStatus());
    assertEquals(0, requestToBatchSuccessful.getRetries());
    assertEquals(0, requestToBatchFailed.getRetries());
    assertFalse(batchService.isRunning());
    assertEquals(result.get().intValue(), 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMaximumBackOffTimeReachedWhileRetryingFailedRequests() throws Exception {
    int httpErrorCode = 503;
    when(backOff.nextBackOffMillis()).thenReturn(2L).thenReturn(BackOff.STOP);
    when(retryPolicy.isRetryableStatusCode(httpErrorCode)).thenReturn(true);
    when(retryPolicy.getMaxRetryLimit()).thenReturn(3);
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatchSuccessful =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    AsyncRequest<GenericJson> requestToBatchFailed =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    assertEquals(0, requestToBatchSuccessful.getRetries());
    assertEquals(Status.NEW, requestToBatchSuccessful.getStatus());
    GenericJson successfulResult = new GenericJson();
    GoogleJsonError error = new GoogleJsonError();
    error.setCode(httpErrorCode);
    error.setMessage("Service Unavailable");

    doAnswer(
            i -> {
              if (requestToBatchSuccessful.getRetries() >= 1) {
                requestToBatchSuccessful.getCallback().onStart();
                requestToBatchSuccessful
                    .getCallback()
                    .onSuccess(successfulResult, new HttpHeaders());
              } else {
                requestToBatchSuccessful.getCallback().onStart();
                requestToBatchSuccessful.getCallback().onFailure(error, new HttpHeaders());
              }
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatchSuccessful);
    batchService.add(requestToBatchFailed);
    Future<Integer> result = batchService.flush();
    batchService.stopAsync().awaitTerminated();
    verify(retryPolicy).isRetryableStatusCode(httpErrorCode);
    assertEquals(Status.COMPLETED, requestToBatchSuccessful.getStatus());
    assertEquals(Status.FAILED, requestToBatchFailed.getStatus());
    assertEquals(1, requestToBatchSuccessful.getRetries());
    assertEquals(0, requestToBatchFailed.getRetries());
    assertEquals(successfulResult, requestToBatchSuccessful.getFuture().get());
    validateFailedResult(requestToBatchFailed.getFuture());
    assertFalse(batchService.isRunning());
    assertEquals(result.get().intValue(), 1);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRetryableGoogleJSONException() throws Exception {
    int httpErrorCode = 503;
    String errorMessage = "Service Unavailable";
    when(retryPolicy.isRetryableStatusCode(httpErrorCode)).thenReturn(true);
    when(retryPolicy.getMaxRetryLimit()).thenReturn(3);
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    assertEquals(0, requestToBatch.getRetries());
    assertEquals(Status.NEW, requestToBatch.getStatus());
    GoogleJsonError error = new GoogleJsonError();
    error.setCode(httpErrorCode);
    error.setMessage(errorMessage);
    GoogleJsonResponseException exception =
        new GoogleJsonResponseException(
            new HttpResponseException.Builder(httpErrorCode, errorMessage, new HttpHeaders()),
            error);
    GenericJson successfulResult = new GenericJson();
    doAnswer(
            i -> {
              if (requestToBatch.getRetries() >= 2) {
                requestToBatch.getCallback().onStart();
                requestToBatch.getCallback().onSuccess(successfulResult, new HttpHeaders());
                return null;
              } else {
                throw exception;
              }
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    verify(retryPolicy, times(2)).isRetryableStatusCode(httpErrorCode);
    verify(retryPolicy, times(2)).getMaxRetryLimit();
    verify(backOff, times(2)).nextBackOffMillis();
    assertEquals(successfulResult, requestToBatch.getFuture().get());
    assertEquals(Status.COMPLETED, requestToBatch.getStatus());
    assertEquals(2, requestToBatch.getRetries());
    assertFalse(batchService.isRunning());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testFailedRequestSucceedsOnRetry() throws Exception {
    int httpErrorCode = 503;
    when(retryPolicy.isRetryableStatusCode(httpErrorCode)).thenReturn(true);
    when(retryPolicy.getMaxRetryLimit()).thenReturn(1);
    BatchRequestService batchService = setupService();
    batchService.startAsync().awaitRunning();
    assertTrue(batchService.isRunning());
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    GoogleJsonError error = new GoogleJsonError();
    error.setCode(httpErrorCode);
    error.setMessage("Service Unavailable");
    GenericJson successfulResult = new GenericJson();
    doAnswer(
            i -> {
              if (requestToBatch.getRetries() >= 1) {
                requestToBatch.getCallback().onStart();
                requestToBatch.getCallback().onSuccess(successfulResult, new HttpHeaders());
              } else {
                requestToBatch.getCallback().onFailure(error, new HttpHeaders());
              }
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    batchService.add(requestToBatch);
    batchService.flush();
    batchService.stopAsync().awaitTerminated();
    verify(retryPolicy).isRetryableStatusCode(httpErrorCode);
    verify(retryPolicy).getMaxRetryLimit();
    verify(backOff).nextBackOffMillis();
    assertEquals(successfulResult, requestToBatch.getFuture().get());
    assertEquals(Status.COMPLETED, requestToBatch.getStatus());
    assertEquals(1, requestToBatch.getRetries());
    assertFalse(batchService.isRunning());
  }

  @Test
  public void batchPolicy_fromConfiguration_batchSize_default() throws Exception {
    int batchCount = 2;
    setupConfig.initConfig(new Properties());
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();
    int batchSize = batchPolicy.getMaxBatchSize();

    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    for (int i = 0; i < batchSize * batchCount; i++) {
      batchService.add(asyncRequest);
    }
    batchService.stopAsync().awaitTerminated();

    ArgumentCaptor<BatchRequestService.SnapshotRunnable> captor =
        ArgumentCaptor.forClass(BatchRequestService.SnapshotRunnable.class);
    verify(batchExecutor, times(batchCount)).execute(captor.capture());
    for (BatchRequestService.SnapshotRunnable batch : captor.getAllValues()) {
      assertEquals(batchSize, batch.snapshotRequests.size());
    }
  }

  @Test
  public void batchPolicy_fromConfiguration_batchSize_custom() throws Exception {
    int batchCount = 2;
    int batchSize = 4;
    Properties config = new Properties();
    config.setProperty("batch.batchSize", String.valueOf(batchSize));
    setupConfig.initConfig(config);
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();
    assertEquals(batchSize, batchPolicy.getMaxBatchSize());

    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    for (int i = 0; i < batchSize * batchCount; i++) {
      batchService.add(asyncRequest);
    }
    batchService.stopAsync().awaitTerminated();

    ArgumentCaptor<BatchRequestService.SnapshotRunnable> captor =
        ArgumentCaptor.forClass(BatchRequestService.SnapshotRunnable.class);
    verify(batchExecutor, times(batchCount)).execute(captor.capture());
    for (BatchRequestService.SnapshotRunnable batch : captor.getAllValues()) {
      assertEquals(batchSize, batch.snapshotRequests.size());
    }
  }

  @Test
  public void batchPolicy_fromConfiguration_flushOnShutdown_default() throws Exception {
    int batchCount = 2;
    int batchSize = 4; // Greater than 1 for this test
    Properties config = new Properties();
    config.setProperty("batch.batchSize", String.valueOf(batchSize));
    setupConfig.initConfig(config);
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();
    assertEquals(true, batchPolicy.isFlushOnShutdown());

    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    // Create an extra request that won't be sent in a batch.
    for (int i = 0; i < (batchSize * batchCount) + 1; i++) {
      batchService.add(asyncRequest);
    }
    batchService.stopAsync().awaitTerminated();

    // With flushOnShutdown set to true (the default), execute should be called one more
    // time to handle the extra request.
    ArgumentCaptor<BatchRequestService.SnapshotRunnable> captor =
        ArgumentCaptor.forClass(BatchRequestService.SnapshotRunnable.class);
    verify(batchExecutor, times(batchCount + 1)).execute(captor.capture());
  }

  @Test
  public void batchPolicy_fromConfiguration_flushOnShutdown_true() throws Exception {
    int batchCount = 2;
    int batchSize = 4;
    Properties config = new Properties();
    config.setProperty("batch.batchSize", String.valueOf(batchSize));
    config.setProperty("batch.flushOnShutdown", "true");
    setupConfig.initConfig(config);
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();
    assertEquals(true, batchPolicy.isFlushOnShutdown());

    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    // Create an extra request that won't be sent in a batch.
    for (int i = 0; i < (batchSize * batchCount) + 1; i++) {
      batchService.add(asyncRequest);
    }
    batchService.stopAsync().awaitTerminated();

    // With flushOnShutdown set to true, execute should be called one more time to handle
    // the extra request.
    ArgumentCaptor<BatchRequestService.SnapshotRunnable> captor =
        ArgumentCaptor.forClass(BatchRequestService.SnapshotRunnable.class);
    verify(batchExecutor, times(batchCount + 1)).execute(captor.capture());
  }

  @Test
  public void batchPolicy_fromConfiguration_flushOnShutdown_false() throws Exception {
    int batchCount = 2;
    int batchSize = 4;
    Properties config = new Properties();
    config.setProperty("batch.batchSize", String.valueOf(batchSize));
    config.setProperty("batch.flushOnShutdown", "false");
    setupConfig.initConfig(config);
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();
    assertEquals(false, batchPolicy.isFlushOnShutdown());

    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    // Create an extra request that won't be sent in a batch.
    for (int i = 0; i < (batchSize * batchCount) + 1; i++) {
      batchService.add(asyncRequest);
    }
    batchService.stopAsync().awaitTerminated();

    // With flushOnShutdown set to false, execute should only be called for the two batches.
    ArgumentCaptor<BatchRequestService.SnapshotRunnable> captor =
        ArgumentCaptor.forClass(BatchRequestService.SnapshotRunnable.class);
    verify(batchExecutor, times(batchCount)).execute(captor.capture());
  }

  // See also testScheduledAutoFlush()
  // There are two elements we can test for maxBatchDelay: whether the value from the
  // BatchPolicy is used in scheduling the calls to flush(), and whether the call happens
  // if we wait that amount of time. testScheduledAutoFlush() tests the parameters to
  // schedule() using a mock; this test checks the BatchPolicy values as set using
  // configuration parameters and checks that a request is flushed after the configured
  // time even though a batch size wasn't reached.
  @Test
  public void batchPolicy_fromConfiguration_maxBatchDelay_default() throws Exception {
    Properties config = new Properties();
    // Don't flush on shutdown; the test is checking for the scheduled flush.
    config.setProperty("batch.flushOnShutdown", "false");
    setupConfig.initConfig(config);
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();
    Integer batchDelay = batchPolicy.getMaxBatchDelay();
    // Check for a change in the default that will increase test run time.
    if (batchDelay > 20) {
      fail("Running this test with default batchDelay=" + batchDelay
          + " will make the test run time too long; remove this test?");
    }
    // Use a real factory so we get a working object for the ScheduledExecutorService
    ExecutorFactory executorFactory = spy(new BatchRequestService.ExecutorFactoryImpl());
    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    doAnswer(
            invocation -> {
              ((SnapshotRunnable) invocation.getArgument(0)).run();
              return null;
            })
        .when(batchExecutor)
        .execute(isA(SnapshotRunnable.class));
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    doAnswer(
            invocation -> {
              requestToBatch.getCallback().onStart();
              requestToBatch.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    batchService.add(requestToBatch);
    // Wait out the configured delay.
    Thread.sleep((batchDelay + 1) * 1000);
    batchService.stopAsync().awaitTerminated();

    // If the flush doesn't happen, the request will be cancelled and get() will throw an
    // exception.
    assertEquals(new GenericJson(), requestToBatch.getFuture().get());
    verify(batchExecutor).execute(isA(SnapshotRunnable.class));
  }

  @Test
  public void batchPolicy_fromConfiguration_maxBatchDelay_custom() throws Exception {
    Integer batchDelay = 3;
    Properties config = new Properties();
    config.setProperty("batch.maxBatchDelaySeconds", String.valueOf(batchDelay));
    // Don't flush on shutdown; the test is checking for the scheduled flush.
    config.setProperty("batch.flushOnShutdown", "false");
    setupConfig.initConfig(config);
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();
    assertEquals(batchDelay.intValue(), batchPolicy.getMaxBatchDelay());

    // Use a real factory so we get a working object for the ScheduledExecutorService
    ExecutorFactory executorFactory = spy(new BatchRequestService.ExecutorFactoryImpl());
    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    doAnswer(
            invocation -> {
              ((SnapshotRunnable) invocation.getArgument(0)).run();
              return null;
            })
        .when(batchExecutor)
        .execute(isA(SnapshotRunnable.class));
    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    BatchRequest batchRequest = getMockBatchRequest();
    when(batchRequestHelper.createBatch(any())).thenReturn(batchRequest);
    doAnswer(
            invocation -> {
              requestToBatch.getCallback().onStart();
              requestToBatch.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(batchRequest);

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    batchService.add(requestToBatch);
    // Wait out the configured delay.
    Thread.sleep((batchDelay + 1) * 1000);
    batchService.stopAsync().awaitTerminated();

    // If the flush doesn't happen, the request will be cancelled and get() will throw an
    // exception.
    assertEquals(new GenericJson(), requestToBatch.getFuture().get());
    verify(batchExecutor).execute(isA(SnapshotRunnable.class));
  }

  @Test
  public void batchPolicy_fromConfiguration_maxActiveBatches() throws Exception {
    int maxActiveBatches = 5;
    int maxQueueLength = 500;
    Properties config = new Properties();
    config.setProperty("batch.maxActiveBatches", String.valueOf(maxActiveBatches));
    config.setProperty("batch.maxQueueLength", String.valueOf(maxQueueLength));
    setupConfig.initConfig(config);
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();

    AsyncRequest<GenericJson> requestToBatch =
        new AsyncRequest<GenericJson>(testRequest, retryPolicy, operationStats);
    doAnswer(
            invocation -> {
              requestToBatch.getCallback().onStart();
              requestToBatch.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
              return null;
            })
        .when(batchRequestHelper)
        .executeBatchRequest(any());
    ExecutorService mockBatchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(mockBatchExecutor);
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);
    doAnswer(
        invocation -> {
          // Actually run the batch; this is how the active batches semaphore is released.
          ((SnapshotRunnable) invocation.getArgument(0)).run();
          return null;
        })
        .when(mockBatchExecutor)
        .execute(isA(SnapshotRunnable.class));

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    // Send more requests than can be active (in batches) at once, but less than
    // maxQueueLength (we're not testing that here).
    int numRequests = (maxActiveBatches + 1) * batchPolicy.getMaxBatchSize();
    assertTrue(numRequests < maxQueueLength);
    for (int i = 0; i < numRequests; i++) {
      batchService.add(requestToBatch);
    }
    batchService.stopAsync().awaitTerminated();

    ArgumentCaptor<BatchRequestService.SnapshotRunnable> captor =
        ArgumentCaptor.forClass(BatchRequestService.SnapshotRunnable.class);
    verify(mockBatchExecutor, Mockito.atLeastOnce()).execute(captor.capture());
    int requestCount = 0;
    for (BatchRequestService.SnapshotRunnable batch : captor.getAllValues()) {
      requestCount += batch.snapshotRequests.size();
    }
    assertEquals(numRequests, requestCount);
  }

  @Test
  public void batchPolicy_fromConfiguration_maxQueueSize() throws Exception {
    // If maxQueueLength is less than maxBatchSize, a scheduled executor is required, or
    // flush will never be called and the test will hang.
    int maxQueueLength = 15;
    int batchSize = 10;
    Properties config = new Properties();
    config.setProperty("batch.maxQueueLength", String.valueOf(maxQueueLength));
    config.setProperty("batch.batchSize", String.valueOf(batchSize));
    setupConfig.initConfig(config);
    BatchPolicy batchPolicy = BatchPolicy.fromConfiguration();

    ExecutorService batchExecutor = Mockito.mock(ExecutorService.class);
    when(executorFactory.getExecutor()).thenReturn(batchExecutor);
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);

    BatchRequestService batchService =
        new BatchRequestService.Builder(service)
            .setExecutorFactory(executorFactory)
            .setBatchRequestHelper(batchRequestHelper)
            .setGoogleCredential(credential)
            .setBatchPolicy(batchPolicy)
            .build();
    batchService.startAsync().awaitRunning();
    // Send more requests than can fit in the request queue. Don't let the number of
    // batches exceed maxActiveBatches or the batches will hang (see
    // batchPolicy_fromConfiguration_maxActiveBatches).
    int numRequests = maxQueueLength * 3;
    for (int i = 0; i < numRequests; i++) {
      batchService.add(asyncRequest);
    }
    batchService.stopAsync().awaitTerminated();

    ArgumentCaptor<BatchRequestService.SnapshotRunnable> captor =
        ArgumentCaptor.forClass(BatchRequestService.SnapshotRunnable.class);
    verify(batchExecutor, Mockito.atLeastOnce()).execute(captor.capture());
    int requestCount = 0;
    for (BatchRequestService.SnapshotRunnable batch : captor.getAllValues()) {
      requestCount += batch.snapshotRequests.size();
    }
    assertEquals(numRequests, requestCount);
  }

  private <T> void validateFailedResult(ListenableFuture<T> failed) throws InterruptedException {
    try {
      failed.get();
      fail("missing ExecutionException");
    } catch (ExecutionException ignore) {
      assertThat(ignore.getCause(), instanceOf(IOException.class));
    }
  }

  private BatchRequest getMockBatchRequest() {
    return new BatchRequest(new MockHttpTransport(), null);
  }

  private static class EqualityMatcher<T> implements ArgumentMatcher<T> {
    private final T self;

    public EqualityMatcher(T self) {
      this.self = self;
    }

    @Override
    public boolean matches(Object argument) {
      return argument == null ? false : argument.equals(self);
    }
  }
}
