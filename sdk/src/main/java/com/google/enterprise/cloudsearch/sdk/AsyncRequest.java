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

import com.google.api.client.googleapis.batch.json.JsonBatchCallback;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException.Builder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats.Event;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Individual batched request wrapper as consumed by {@link BatchRequestService#add} */
public class AsyncRequest<T> {
  private static final Logger logger = Logger.getLogger(AsyncRequest.class.getName());
  private final AbstractGoogleJsonClientRequest<T> requestToExecute;
  private final SettableFutureCallback<T> callback;

  private RetryPolicy retryPolicy;
  private int retries = 0;
  private Status status = Status.NEW;

  enum Status {
    NEW,
    RETRYING,
    COMPLETED,
    FAILED,
    CANCELLED
  }

  public AsyncRequest(
      AbstractGoogleJsonClientRequest<T> requestToExecute,
      RetryPolicy retryPolicy,
      OperationStats operationStats) {
    this.requestToExecute = checkNotNull(requestToExecute, "request can not be null");
    this.callback = new SettableFutureCallback<>(this, operationStats);
    this.retryPolicy = checkNotNull(retryPolicy, "retry policy cannot be null!");
  }

  /** Cancel this request. */
  public void cancel() {
    status = Status.CANCELLED;
    callback.cancel();
  }

  /**
   * Gets request to be batched.
   *
   * @return request to be batched.
   */
  public AbstractGoogleJsonClientRequest<T> getRequest() {
    return requestToExecute;
  }

  /**
   * Gets {@link ListenableFuture} instance representing pending result from {@link AsyncRequest}
   * execution.
   *
   * @return {@link ListenableFuture} instance representing pending result for {@link AsyncRequest}
   */
  public ListenableFuture<T> getFuture() {
    return callback.settable;
  }

  /**
   * Gets {@link SettableFutureCallback} instance associated with batched request.
   *
   * @return {@link SettableFutureCallback} instance associated with batched request.
   */
  public SettableFutureCallback<T> getCallback() {
    return callback;
  }

  /**
   * Gets current number of retries for {@link AsyncRequest}.
   *
   * @return current number of retries for {@link AsyncRequest}.
   */
  public int getRetries() {
    return retries;
  }

  /** Increments retry count if batched request is retried. */
  public void incrementRetries() {
    retries++;
  }

  /**
   * Gets {@link Status} for batched request.
   *
   * @return status for batched request.
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Sets status for batched request.
   *
   * @param newStatus updated status for batched request.
   */
  public void setStatus(Status newStatus) {
    this.status = newStatus;
  }

  /**
   * {@link JsonBatchCallback} wrapper which updates {@link ListenableFuture} associated with
   * individual batched request.
   */
  public static class SettableFutureCallback<T> extends JsonBatchCallback<T>
      implements EventStartCallback {
    private final AsyncRequest<T> request;
    private final SettableFuture<T> settable;
    private final OperationStats operationStats;
    private Event event;

    SettableFutureCallback(AsyncRequest<T> request, OperationStats operationStats) {
      this.request = request;
      this.settable = SettableFuture.create();
      this.operationStats = operationStats;
    }

    /**
     * Wrapper on {@link JsonBatchCallback#onSuccess} to record successful execution of batched
     * request.
     */
    @Override
    public void onSuccess(T t, HttpHeaders responseHeaders) throws IOException {
      settable.set(t);
      checkState(
          event != null, "Invalid state: onStart() must be called before calling this method!");
      event.success();
      request.setStatus(Status.COMPLETED);
    }

    /**
     * Wrapper on {@link JsonBatchCallback#onFailure} to record failure while executing batched
     * request.
     */
    @Override
    public void onFailure(GoogleJsonError error, HttpHeaders responseHeaders) {
      if (event != null) {
        event.failure();
        event = null;
      } else {
        operationStats.event(request.requestToExecute.getClass().getName()).failure();
      }
      logger.log(Level.WARNING, "Request failed with error {0}", error);
      if (request.retryPolicy.isRetryableStatusCode(error.getCode())) {
        if (request.getRetries() < request.retryPolicy.getMaxRetryLimit()) {
          request.setStatus(Status.RETRYING);
          request.incrementRetries();
          return;
        }
      }
      GoogleJsonResponseException exception =
          new GoogleJsonResponseException(
              new Builder(error.getCode(), error.getMessage(), responseHeaders), error);
      fail(exception);
    }

    private void fail(Exception ex) {
      settable.setException(ex);
      request.setStatus(Status.FAILED);
    }

    private void cancel() {
      if (!settable.isDone()) {
        settable.cancel(true);
      }
    }

    void setEvent(Event event) {
      this.event = event;
    }

    /** Record start of processing / execution for batched request. */
    @Override
    public void onStart() {
      event = operationStats.event(request.requestToExecute.getClass().getName());
      event.start();
    }
  }

  /** Interface to record start of an event such as execution of batched request. */
  public static interface EventStartCallback {
    /** Record start of an event. */
    void onStart();
  }
}
