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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.batch.BatchRequest;
import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.concurrent.TimeUnit;

/**
 * Policy for flushing batched requests.
 *
 * <p>The trigger for uploading a batch of requests is by either the number of requests or the
 * timeout, whichever comes first. For example, if the batch delay time has expired without the
 * batch size being reached, or if the batch size number of documents is reached before the delay
 * time expires, then the batch upload is triggered.
 */
public class BatchPolicy {
  private static final int DEFAULT_BATCH_DELAY_SECONDS = 5;
  private static final int DEFAULT_BATCH_SIZE = 10;
  private static final int DEFAULT_MAX_QUEUE_LENGTH = 1000;
  private static final int DEFAULT_MAX_ACTIVE_BATCHES = 20;
  private static final int DEFAULT_BATCH_REQUEST_TIMEOUT_SECONDS = 120;

  @VisibleForTesting static final String CONFIG_BATCH_FLUSH_ON_SHUTDOWN = "batch.flushOnShutdown";
  static final String CONFIG_BATCH_SIZE = "batch.batchSize";
  static final String CONFIG_BATCH_MAX_DELAY_SECONDS = "batch.maxBatchDelaySeconds";
  static final String CONFIG_BATCH_MAX_QUEUE_LENGTH = "batch.maxQueueLength";
  static final String CONFIG_BATCH_MAX_ACTIVE_BATCHES = "batch.maxActiveBatches";
  static final String CONFIG_BATCH_READ_TIMEOUT_SECONDS = "batch.readTimeoutSeconds";
  static final String CONFIG_BATCH_CONNECT_TIMEOUT_SECONDS = "batch.connectTimeoutSeconds";


  private final int maxBatchSize;
  private final int maxBatchDelay;
  private final TimeUnit maxBatchDelayUnit;
  private final boolean flushOnShutdown;
  private final int queueLength;
  private final int maxActiveBatches;
  private final int batchReadTimeout;
  private final int batchConnectTimeout;

  private BatchPolicy(Builder builder) {
    maxBatchSize = builder.maxBatchSize;
    maxBatchDelay = builder.maxBatchDelay;
    maxBatchDelayUnit = builder.maxBatchDelayUnit;
    flushOnShutdown = builder.flushOnShutdown;
    queueLength = builder.queueLength;
    maxActiveBatches = builder.maxActiveBatches;
    batchReadTimeout = builder.batchReadTimeout;
    batchConnectTimeout = builder.batchConnectTimeout;
  }

  /**
   * Creates a batch policy from configuration file parameters.
   *
   * <p>The configuration file should have parameters in the format of:
   *
   * <ul>
   *   <li>batch.flushOnShutdown = true to flush batched request during service shutdown
   *   <li>batch.batchSize = 10 number of requests to batch together
   *   <li>batch.maxBatchDelaySeconds = 5 number of seconds to wait before batched requests are
   *       flushed automatically
   *   <li>batch.maxQueueLength = 1000 maximum number of requests in batch queue for execution
   *   <li>batch.maxActiveBatches = 20 number of allowable concurrently executing batches
   *   <li>batch.readTimeoutSeconds = 120 read timeout in seconds for batch request
   *   <li>batch.connectTimeoutSeconds = 120 connect timeout in seconds for batch request
   * </ul>
   */
  public static BatchPolicy fromConfiguration() {
    checkState(Configuration.isInitialized(), "config not initialized");
    return new BatchPolicy.Builder()
        .setFlushOnShutdown(Configuration.getBoolean(CONFIG_BATCH_FLUSH_ON_SHUTDOWN, true).get())
        .setMaxBatchDelay(
            Configuration.getInteger(CONFIG_BATCH_MAX_DELAY_SECONDS, DEFAULT_BATCH_DELAY_SECONDS)
                .get(),
            TimeUnit.SECONDS)
        .setMaxBatchSize(Configuration.getInteger(CONFIG_BATCH_SIZE, DEFAULT_BATCH_SIZE).get())
        .setQueueLength(
            Configuration.getInteger(CONFIG_BATCH_MAX_QUEUE_LENGTH, DEFAULT_MAX_QUEUE_LENGTH).get())
        .setMaxActiveBatches(
            Configuration.getInteger(CONFIG_BATCH_MAX_ACTIVE_BATCHES, DEFAULT_MAX_ACTIVE_BATCHES)
                .get())
        .setBatchReadTimeoutSeconds(
            Configuration.getInteger(
                CONFIG_BATCH_READ_TIMEOUT_SECONDS, DEFAULT_BATCH_REQUEST_TIMEOUT_SECONDS).get())
        .setBatchConnectTimeoutSeconds(
            Configuration.getInteger(
                CONFIG_BATCH_CONNECT_TIMEOUT_SECONDS, DEFAULT_BATCH_REQUEST_TIMEOUT_SECONDS).get())
        .build();
  }

  /**
   * Gets maximum number of requests allowed in single batch request.
   *
   * @return maximum number of requests allowed in single batch request
   */
  public int getMaxBatchSize() {
    return maxBatchSize;
  }

  /**
   * Gets maximum batch auto flush delay.
   *
   * @return maximum duration to wait before auto flushing batch.
   */
  public int getMaxBatchDelay() {
    return maxBatchDelay;
  }

  /**
   * Gets {@link TimeUnit} for batch auto flush delay.
   *
   * @return {@link TimeUnit} for batch auto flush delay
   */
  public TimeUnit getMaxBatchDelayUnit() {
    return maxBatchDelayUnit;
  }

  /**
   * Gets flag indicating if already enqueued requests would be executed once shutdown is initiated
   * or already enqueued requests would be marked as cancelled.
   *
   * @return true implies already enqueued requests would be executed once shutdown is initiated,
   *     false implies already enqueued requests would be marked as cancelled.
   */
  public boolean isFlushOnShutdown() {
    return flushOnShutdown;
  }

  /**
   * Gets total number of requests allowed to be batched before blocking batching of additional
   * requests.
   *
   * @return Total number of requests allowed to be batched before blocking batching of additional
   *     requests.
   */
  public int getQueueLength() {
    return queueLength;
  }

  /**
   * Gets maximum number of allowed active batch requests under processing at a given instance.
   *
   * @return Maximum number of allowed active batch requests under processing at a given instance.
   */
  public int getMaxActiveBatches() {
    return maxActiveBatches;
  }

  /**
   * Gets read timeout in seconds for {@link BatchRequest} execution.
   *
   * @return read timeout in seconds for {@link BatchRequest} execution.
   */
  public int getBatchReadTimeoutSeconds() {
    return batchReadTimeout;
  }

  /**
   * Gets connect timeout in seconds for {@link BatchRequest} execution.
   *
   * @return connect timeout in seconds for {@link BatchRequest} execution.
   */
  public int getBatchConnectTimeoutSeconds() {
    return batchConnectTimeout;
  }

  /** Builder object for creating an instance of {@link BatchRequest}. */
  public static final class Builder {
    private boolean flushOnShutdown = true;
    private int maxBatchSize = DEFAULT_BATCH_SIZE;
    private int maxBatchDelay = DEFAULT_BATCH_DELAY_SECONDS;
    private TimeUnit maxBatchDelayUnit = TimeUnit.SECONDS;
    private int queueLength = DEFAULT_MAX_QUEUE_LENGTH;
    private int maxActiveBatches = DEFAULT_MAX_ACTIVE_BATCHES;
    private int batchReadTimeout = DEFAULT_BATCH_REQUEST_TIMEOUT_SECONDS;
    private int batchConnectTimeout = DEFAULT_BATCH_REQUEST_TIMEOUT_SECONDS;

    /**
     * Sets maximum number of requests to be batched together.
     *
     * @param maxBatchSize maximum number of requests to be batched together
     * @return this Builder instance
     */
    public Builder setMaxBatchSize(int maxBatchSize) {
      this.maxBatchSize = maxBatchSize;
      return this;
    }

    /**
     * Sets auto flush delay for batched request.
     *
     * @param maxBatchDelay auto flush delay.
     * @param maxBatchDelayUnit TimeUnit for auto flush delay.
     * @return this Builder instance
     */
    public Builder setMaxBatchDelay(int maxBatchDelay, TimeUnit maxBatchDelayUnit) {
      this.maxBatchDelay = maxBatchDelay;
      this.maxBatchDelayUnit = maxBatchDelayUnit;
      return this;
    }

    /**
     * Sets flag to indicate if {@link BatchRequestService} should execute already enqueued requests
     * once {@link BatchRequestService} shutdown is initiated or mark such requests as cancelled.
     *
     * @param flushOnShutdown Set to {@code true} if already enqueued requests should be executed
     *     once shutdown is initiated. Set to {@code false} to mark already enqueued requests as
     *     cancelled once shutdown is initiated.
     * @return this Builder instance
     */
    public Builder setFlushOnShutdown(boolean flushOnShutdown) {
      this.flushOnShutdown = flushOnShutdown;
      return this;
    }

    /**
     * Sets total length of active batch request queue.
     *
     * @param queueLength total length of active batch request queue.
     * @return this Builder instance
     */
    public Builder setQueueLength(int queueLength) {
      this.queueLength = queueLength;
      return this;
    }

    /**
     * Sets maximum concurrent batch requests allowed to be executed.
     *
     * @param maxActiveBatches maximum concurrent batch requests allowed to be executed
     *     simultaneously.
     * @return this Builder instance
     */
    public Builder setMaxActiveBatches(int maxActiveBatches) {
      this.maxActiveBatches = maxActiveBatches;
      return this;
    }

    /**
     * Sets read timeout in seconds for entire {@link BatchRequest}
     *
     * @param batchReadTimeout read timeout for entire {@link BatchRequest}
     * @return this Builder instance
     */
    public Builder setBatchReadTimeoutSeconds(int batchReadTimeout) {
      this.batchReadTimeout = batchReadTimeout;
      return this;
    }

    /**
     * Sets connect timeout in seconds for entire {@link BatchRequest}
     *
     * @param batchConnectTimeout connect timeout for entire {@link BatchRequest}
     * @return this Builder instance
     */
    public Builder setBatchConnectTimeoutSeconds(int batchConnectTimeout) {
      this.batchConnectTimeout = batchConnectTimeout;
      return this;
    }

    /**
     * Builds an instance of {@link BatchPolicy}.
     *
     * @return an instance of {@link BatchPolicy}.
     */
    public BatchPolicy build() {
      checkArgument(maxBatchSize > 0, "maxBatchSize should be greater than 0");
      checkArgument(maxBatchDelay > 0, "maxBatchDelay should be greater than 0");
      checkNotNull(maxBatchDelayUnit, "maxBatchDelayUnit can not be null");
      checkArgument(queueLength > 0, "queueLength should be greater than 0");
      checkArgument(maxActiveBatches > 0, "maxActiveBatches should be greater than 0");
      checkArgument(batchReadTimeout > 0, "batchReadTimeout should be greater than 0");
      checkArgument(batchReadTimeout > 0, "batchReadTimeout should be greater than 0");
      return new BatchPolicy(this);
    }
  }
}
