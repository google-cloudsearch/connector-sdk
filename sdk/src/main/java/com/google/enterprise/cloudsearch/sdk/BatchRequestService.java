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
import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.util.BackOff;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.enterprise.cloudsearch.sdk.AsyncRequest.EventStartCallback;
import com.google.enterprise.cloudsearch.sdk.AsyncRequest.Status;
import com.google.enterprise.cloudsearch.sdk.RetryPolicy.BackOffFactory;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.net.ssl.SSLHandshakeException;

/**
 * Batch multiple requests. Use {@link BatchRequestService#add} to batch requests. Caller can use
 * returned {@link ListenableFuture} to get result of batched request. Use {@link
 * BatchRequestService#isRunning()} to determine if more requests can be batched. Set {@link
 * BatchPolicy} to specify max batch request size or batching delay. Requests will be automatically
 * flushed once flush policy is satisfied. Use {@link BatchRequestService#flush} to flush current
 * batched requests.
 *
 * <p>TODO(tvartak) : Use executor service to set values or exceptions on settable future with
 * timeout. Caller can potentially add long running callback or listener with
 * {@link MoreExecutors#newDirectExecutorService()}. These should not block other threads.
 */
public class BatchRequestService extends AbstractIdleService {
  private static final Logger logger = Logger.getLogger(BatchRequestService.class.getName());
  private final ArrayBlockingQueue<AsyncRequest<?>> requests;
  private final ExecutorService batchExecutor;
  private final ScheduledExecutorService scheduledExecutor;
  private final BatchPolicy flushPolicy;
  private final AtomicLong lastFlush = new AtomicLong();
  private final TimeProvider currentTimeProvider;
  private final AtomicBoolean needToScheduleFlush = new AtomicBoolean(true);
  private final BatchRequestHelper batchRequestHelper;
  private final Semaphore batchingSemaphore;
  private final BatchRequestInitializer batchRequestInitializer;
  private final BackOffFactory backOffFactory;

  /**
   * Create an instance of {@link BatchRequestService}
   *
   * @param builder for creating {@link BatchRequestService}
   */
  public BatchRequestService(Builder builder) {
    this.batchRequestHelper = builder.batchRequestHelper;
    this.batchExecutor = checkNotNull(builder.executorFactory.getExecutor());
    this.scheduledExecutor = checkNotNull(builder.executorFactory.getScheduledExecutor());
    this.flushPolicy = builder.batchPolicy;
    this.backOffFactory = checkNotNull(builder.retryPolicy.getBackOffFactory());
    this.currentTimeProvider = builder.timeProvider;
    this.requests = new ArrayBlockingQueue<>(builder.batchPolicy.getQueueLength(), true);
    this.batchingSemaphore = new Semaphore(builder.batchPolicy.getMaxActiveBatches());
    this.batchRequestInitializer =
        new BatchRequestInitializer(builder.credential, builder.batchPolicy);
  }

  /**
   * Adds an request to batch request. If current batch size is greater than or equal to {@link
   * BatchPolicy#getMaxBatchSize()}, current batched requests will be automatically flushed.
   * Operation might block if {@link BatchRequestService} can not accept more request immediately.
   *
   * @param request to be batched
   * @throws InterruptedException
   */
  public <T> void add(AsyncRequest<T> request) throws InterruptedException {
    checkNotNull(request, "can not batch null request");
    checkState(isRunning(), "can not batch elements if service is not running.");
    requests.put(request);
    boolean flushed = flushIfRequired();
    if (!flushed && needToScheduleFlush.get()) {
      scheduleAutoFlush();
    }
  }

  private void scheduleAutoFlush() {
    boolean needToScheduleAutoFlush = needToScheduleFlush.compareAndSet(true, false);
    if (needToScheduleAutoFlush) {
      scheduledExecutor.schedule(
          new ScheduleFlushRunnable(currentTimeProvider.currentTimeMillis()),
          flushPolicy.getMaxBatchDelay(),
          flushPolicy.getMaxBatchDelayUnit());
    }
  }

  private boolean flushIfRequired() throws InterruptedException {
    if (getCurrentBatchSize() < flushPolicy.getMaxBatchSize()) {
      return false;
    }
    synchronized (requests) {
      if (getCurrentBatchSize() < flushPolicy.getMaxBatchSize()) {
        return false;
      }
      logger.info("flushing batched requests as max size reached");
      flush();
    }
    return true;
  }

  /**
   * Returns number of elements enqueued for batched execution.
   *
   * @return number of elements enqueued for batched execution.
   */
  public int getCurrentBatchSize() {
    return requests.size();
  }

  /**
   * Flushes all enqueued requests for batched execution.
   *
   * @return number of requests flushed for batched execution.
   * @throws InterruptedException if interrupted while executing batch requests.
   */
  public ListenableFuture<Integer> flush() throws InterruptedException {
    return flush(false);
  }

  private ListenableFuture<Integer> flush(boolean isShutdown) throws InterruptedException {
    checkState(isShutdown || isRunning(), "service not running to flush batched requests.");
    SnapshotRunnable snapshot;
    synchronized (requests) {
      if (requests.isEmpty()) {
        SettableFuture<Integer> result = SettableFuture.create();
        result.set(0);
        return result;
      }
      final List<AsyncRequest<?>> snapshotRequests = new ArrayList<>();
      requests.drainTo(snapshotRequests, flushPolicy.getMaxBatchSize());
      snapshot = new SnapshotRunnable(snapshotRequests, backOffFactory.createBackOffInstance());
      lastFlush.set(currentTimeProvider.currentTimeMillis());
      needToScheduleFlush.set(true);
    }
    try {
      batchingSemaphore.acquire();
      batchExecutor.execute(snapshot);
    } catch (RejectedExecutionException e) {
      // Mark current set of Futures in batch as rejected.
      snapshot.rejectSnapshot(e);
      throw e;
    }
    return snapshot.result;
  }

  /**
   * {@link Runnable} task to flush batched requests once {@link BatchPolicy#getMaxBatchDelay()} is
   * expired. This is no-op if batch is already flushed. {@link ScheduleFlushRunnable#run} compares
   * expected time stamp with next flush to determine if current flush is required.
   */
  @VisibleForTesting
  class ScheduleFlushRunnable implements Runnable {
    final long expectedLastFlush;

    ScheduleFlushRunnable(long expectedLastFlush) {
      this.expectedLastFlush = expectedLastFlush;
    }

    @Override
    public void run() {
      if (lastFlush.get() > expectedLastFlush) {
        return;
      }
      try {
        flush();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @VisibleForTesting
  class SnapshotRunnable implements Runnable {
    private final BackOff backOff;
    final List<AsyncRequest<?>> snapshotRequests;
    final SettableFuture<Integer> result;

    SnapshotRunnable(List<AsyncRequest<?>> snapshotRequests, BackOff backOff) {
      this.snapshotRequests = Collections.unmodifiableList(snapshotRequests);
      this.result = SettableFuture.create();
      this.backOff = backOff;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void run() {
      try {
        List<AsyncRequest<?>> pendingTasks = new ArrayList<>(snapshotRequests);
        do {
          try {
            execute(pendingTasks);
          } catch (IOException e) {
            // may be retryable
            pendingTasks.forEach(t -> onFailure(t, e));
          } finally {
            // back off
            pendingTasks = filterActiveTasks(pendingTasks);
            if (pendingTasks.size() > 0) {
              boolean succeeded = backOff(pendingTasks);
              if (!succeeded) {
                break;
              }
            }
          }
        } while (!pendingTasks.isEmpty());
        result.set(
            (int) snapshotRequests.stream().filter(t -> t.getStatus() != Status.FAILED).count());
      } finally {
        batchingSemaphore.release();
      }
    }

    private boolean backOff(List<AsyncRequest<?>> pendingTasks) {
      long backOffMillis = getBackOffTime();
      if (backOffMillis == BackOff.STOP) {
        pendingTasks.forEach(
            t ->
                onFailure(
                    t, new ExecutionException(new Exception("Maximum back-off cycle reached."))));
        return false;
      }
      try {
        batchRequestHelper.sleep(backOffMillis);
      } catch (InterruptedException e) {
        logger.log(Level.WARNING, "Task interrupted while processing batch requests, stopping.", e);
        pendingTasks.forEach(t -> onFailure(t, e));
        Thread.currentThread().interrupt();
        return false;
      }
      return true;
    }

    private long getBackOffTime() {
      long backOffMillis = BackOff.STOP;
      try {
        backOffMillis = backOff.nextBackOffMillis();
      } catch (IOException e) {
        // Use the default.
      }
      return backOffMillis;
    }

    private List<AsyncRequest<?>> filterActiveTasks(List<AsyncRequest<?>> inputList) {
      return inputList
          .stream()
          .filter(t -> (t.getStatus() == Status.NEW) || (t.getStatus() == Status.RETRYING))
          .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private void execute(List<AsyncRequest<?>> toBeProcessed) throws IOException {
      List<? extends EventStartCallback> eventCallbackList =
          ImmutableList.copyOf(
              toBeProcessed.stream().map(r -> r.getCallback()).collect(Collectors.toList()));
      BatchRequest batchRequest =
          batchRequestHelper.createBatch(
              new EventLoggingRequestInitializer(batchRequestInitializer, eventCallbackList));
      for (@SuppressWarnings("rawtypes") AsyncRequest req : toBeProcessed) {
        batchRequestHelper.queue(batchRequest, req.getRequest(), req.getCallback());
      }
      batchRequestHelper.executeBatchRequest(batchRequest);
    }

    @SuppressWarnings("unchecked")
    private void rejectSnapshot(RejectedExecutionException rejectedException) {
      logger.log(Level.WARNING, "snapshot rejected", rejectedException);
      snapshotRequests.forEach(k -> onFailure(k, rejectedException));
      result.setException(rejectedException);
      batchingSemaphore.release();
    }

    /** Helper method makes the onFailure callback without throwing IOException */
    private <T> void onFailure(AsyncRequest<T> entry, Exception e) {
      GoogleJsonError error = getGoogleJsonError(e);
      logger.log(Level.WARNING, String.format("Batched request failed with error %s", error), e);
      entry.getCallback().onFailure(error, new HttpHeaders());
    }

    private GoogleJsonError getGoogleJsonError(Exception e) {
      if (e instanceof GoogleJsonResponseException) {
        return ((GoogleJsonResponseException) e).getDetails();
      }
      boolean retryableException =
          (e instanceof SSLHandshakeException || e instanceof SocketTimeoutException);
      if (retryableException) {
        logger.log(Level.WARNING, "Retrying request failed with exception:", e);
      }
      // Using retryable 504 Gateway Timeout error code.
      int errorCode = retryableException ? 504 : 0;
      GoogleJsonError error = new GoogleJsonError();
      error.setMessage(e.getMessage());
      error.setCode(errorCode);
      return error;
    }
  }

  @Override
  protected void startUp() throws Exception {}

  @Override
  protected void shutDown() throws Exception {
    logger.log(
        Level.INFO,
        "Shutting down batching service. flush on shutdown: {0}",
        flushPolicy.isFlushOnShutdown());
    if (flushPolicy.isFlushOnShutdown()) {
      flush(true);
    } else {
      // cancel batched requests if flushOnShutdown is false.
      requests.stream().forEach(k -> k.cancel());
    }
    // TODO(tvartak) Use MoreExecutors.shutdownAndAwaitTermination for shutdown
    shutdownExecutor(batchExecutor);
    shutdownExecutor(scheduledExecutor);
  }

  private synchronized void shutdownExecutor(ExecutorService executor) {
    executor.shutdown();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
    executor.shutdownNow();
  }

  /** Interface for classes that can provide the current time. */
  public interface TimeProvider {
    /** Returns the current time in milliseconds. */
    public long currentTimeMillis();
  }

  /** {@link TimeProvider} implementation to get current system time. */
  public static class SystemTimeProvider implements TimeProvider {
    /** Returns the current time in milliseconds. */
    @Override
    public long currentTimeMillis() {
      return System.currentTimeMillis();
    }
  }

  /** Factory for obtaining {@link ExecutorService} instance. */
  public interface ExecutorFactory {
    /**
     * Returns {@link ExecutorService} to execute batch requests.
     *
     * @return {@link ExecutorService}
     */
    ExecutorService getExecutor();
    /**
     * Returns {@link ScheduledExecutorService} to flush batched requests periodically.
     *
     * @return {@link ScheduledExecutorService}
     */
    ScheduledExecutorService getScheduledExecutor();
  }

  /**
   * {@link ExecutorFactory} implementation to get {@link ExecutorService} and {@link
   * ScheduledExecutorService} instances used by {@link BatchRequestService}.
   */
  public static class ExecutorFactoryImpl implements ExecutorFactory {
    /** Gets an instance of {@link ExecutorService} */
    @Override
    public ExecutorService getExecutor() {
      return Executors.newCachedThreadPool(
          new ThreadFactoryBuilder().setDaemon(false).setNameFormat("batching").build());
    }

    /** Gets an instance of {@link ScheduledExecutorService}. */
    @Override
    public ScheduledExecutorService getScheduledExecutor() {
      return Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder().setDaemon(false).setNameFormat("batching-schedule").build());
    }
  }

  /** Helper wrapping calls to BatchRequest which is final and cannot be mocked. */
  @VisibleForTesting
  static class BatchRequestHelper {
    private final AbstractGoogleJsonClient service;

    BatchRequestHelper(AbstractGoogleJsonClient service) {
      this.service = service;
    }

    BatchRequest createBatch(HttpRequestInitializer requestInitializer) {
      return service.batch(requestInitializer);
    }

    <T> void queue(
        BatchRequest batchRequest,
        AbstractGoogleJsonClientRequest<T> request,
        JsonBatchCallback<T> jsonCallback)
        throws IOException {
      request.queue(batchRequest, jsonCallback);
    }

    void executeBatchRequest(BatchRequest batchRequest) throws IOException {
      batchRequest.execute();
    }

    void sleep(long millis) throws InterruptedException {
      Thread.sleep(millis);
    }
  }

  @VisibleForTesting
  static class EventLoggingRequestInitializer implements HttpRequestInitializer {
    private final HttpRequestInitializer chainedRequestInitializer;
    private final List<? extends EventStartCallback> eventList;

    public EventLoggingRequestInitializer(
        final HttpRequestInitializer chainedRequestInitializer,
        final List<? extends EventStartCallback> eventList) {
      this.chainedRequestInitializer = chainedRequestInitializer;
      this.eventList = eventList;
    }

    @Override
    public void initialize(HttpRequest request) throws IOException {
      chainedRequestInitializer.initialize(request);
      request.setInterceptor(req -> eventList.stream().forEach(e -> e.onStart()));
    }
  }

  private static class BatchRequestInitializer implements HttpRequestInitializer {
    private final GoogleCredential credential;
    private final HttpRequestInitializer requestTimeoutInitializer;

    public BatchRequestInitializer(GoogleCredential credential, BatchPolicy batchPolicy) {
      this.credential = checkNotNull(credential, "batch request requires authentication");
      this.requestTimeoutInitializer = getRequestTimeoutInitializer(
          batchPolicy.getBatchConnectTimeoutSeconds(), batchPolicy.getBatchReadTimeoutSeconds());
    }

    @Override
    public void initialize(HttpRequest request) throws IOException {
      credential.initialize(request);
      requestTimeoutInitializer.initialize(request);
      // TODO(tvartak) : Make request logging configurable.
      request.setLoggingEnabled(true);
    }

    private static HttpRequestInitializer getRequestTimeoutInitializer(
        int connectTimeout, int readTimeout) {
      return request -> {
        request.setConnectTimeout(connectTimeout * 1000);
        request.setReadTimeout(readTimeout * 1000);
      };
    }
  }

  /** Builder object for creating an instance of {@link BatchRequestService} */
  public static class Builder {
    private ExecutorFactory executorFactory = new ExecutorFactoryImpl();
    private BatchPolicy batchPolicy = new BatchPolicy.Builder().build();
    private RetryPolicy retryPolicy = new RetryPolicy.Builder().build();
    private TimeProvider timeProvider = new SystemTimeProvider();
    private BatchRequestHelper batchRequestHelper;
    private GoogleCredential credential;

    /**
     * Creates Builder to construct {@link BatchRequestService} for batching requests for {@link
     * AbstractGoogleJsonClient} service.
     *
     * @param service instance to create batch request for.
     */
    @SuppressWarnings("javadoc")
    public Builder(AbstractGoogleJsonClient service) {
      this.batchRequestHelper = new BatchRequestHelper(service);
    }

    /**
     * Sets {@link ExecutorFactory} to be used to create instance of {@link ExecutorService} to be
     * used to execute batched requests asynchronously.
     *
     * @param executorFactory {@link ExecutorFactory} to be used
     */
    public Builder setExecutorFactory(ExecutorFactory executorFactory) {
      this.executorFactory = executorFactory;
      return this;
    }

    /**
     * Sets {@link BatchPolicy} for request batching.
     *
     * @param flushPolicy {@link BatchPolicy} for request batching.
     */
    public Builder setBatchPolicy(BatchPolicy flushPolicy) {
      this.batchPolicy = flushPolicy;
      return this;
    }

    /**
     * Sets {@link RetryPolicy} for exponential back off and error handling for failed requests.
     *
     * @param retryPolicy {@link RetryPolicy} for exponential back off and error handling for failed
     *     requests
     */
    public Builder setRetryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    /**
     * Sets {@link TimeProvider} to compute current time used for auto flushing batch requests.
     *
     * @param timeProvider {@link TimeProvider} to get current time.
     */
    public Builder setTimeProvider(TimeProvider timeProvider) {
      this.timeProvider = timeProvider;
      return this;
    }

    /**
     * Sets helper object which allows dependency injection for unit tests.
     *
     * @param batchRequestHelper helper object which allows dependency injection for unit tests.
     */
    public Builder setBatchRequestHelper(BatchRequestHelper batchRequestHelper) {
      this.batchRequestHelper = batchRequestHelper;
      return this;
    }

    /**
     * Sets credentials to be used for executing {@link BatchRequest}
     *
     * @param credential to be used for executing {@link BatchRequest}
     */
    public Builder setGoogleCredential(GoogleCredential credential) {
      this.credential = credential;
      return this;
    }

    /**
     * Creates an instance of {@link BatchRequestService}.
     *
     * @return an instance of {@link BatchRequestService}.
     */
    public BatchRequestService build() {
      checkNotNull(executorFactory, "executorFactory can not be null");
      checkNotNull(batchPolicy, "batchFlushPolicy can not be null");
      checkNotNull(timeProvider, "timeProvider can not be null");
      checkNotNull(batchRequestHelper, "batchRequestHelper can not be null");
      checkNotNull(credential, "credential can not be null");
      return new BatchRequestService(this);
    }
  }
}
