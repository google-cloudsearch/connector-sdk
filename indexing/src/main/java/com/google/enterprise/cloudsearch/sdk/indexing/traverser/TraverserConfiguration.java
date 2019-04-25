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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.Connector;
import com.google.enterprise.cloudsearch.sdk.config.ConfigValue;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.BatchItemRetriever;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl.PollItemStatus;
import com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Object that defines a Cloud Search queue polling task.
 *
 * <p>The {@link Connector} uses this method when it implements the {@link ItemRetriever} interface.
 * Each instance defines the parameters of a polling request.
 */
public class TraverserConfiguration {
  private final String name;
  private final PollItemsRequest pollRequest;
  private final ItemRetriever itemRetriever;
  private final BatchItemRetriever batchItemRetriever;
  private final long timeout;
  private final int hostload;
  private final TimeUnit timeunit;

  /**
   * Returns the traverser configuration name that is used for logging.
   *
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * Returns the {@link PollItemsRequest} object that contains all the parameters that define the
   * polling task.
   *
   * @return the pollRequest
   */
  public PollItemsRequest getPollRequest() {
    return pollRequest;
  }

  /**
   * Returns the {@link ItemRetriever} object that is used for issuing
   * {@link ItemRetriever#process(Item)} calls.
   *
   * <p>This is typically a {@link Connector} object that implements the {@link ItemRetriever}
   * interface.
   *
   * @return the item retriever object
   */
  public ItemRetriever getItemRetriever() {
    return itemRetriever;
  }

  /**
   * Returns the {@link BatchItemRetriever} object that is used for issuing {@link
   * BatchItemRetriever#processBatch(List)} calls.
   *
   * <p>This is similar to the item retriever object, except that multiple queued items are polled
   * instead of one at a time. Exactly one of either an item retriever or batch item retriever
   * object must be defined.
   *
   * @return the batch item retriever object
   */
  public BatchItemRetriever getBatchItemRetriever() {
    return batchItemRetriever;
  }

  /**
   * Returns the timeout value that specifies when the polling task interrupts its request.
   *
   * @return the timeout value
   */
  public long getTimeout() {
    return timeout;
  }

  /**
   * Returns the timeout unit value that specifies when the polling task interrupts its request.
   *
   * @return the time unit of the {@code timeout} parameter
   */
  public TimeUnit getTimeunit() {
    return timeunit;
  }

  /**
   * Returns the number of polling threads allowed to run in parallel.
   *
   * <p>Each thread works on a polled document in parallel with up to the defined maximum setting,
   * returned from this method. A higher maximum host load value processes more documents in
   * parallel, with a similar increase in system resources used.
   *
   * <p>If multiple {@link TraverserConfiguration} instances are defined within a connector,
   * defining differing host load values for each configuration sets the relative resource
   * priorities of each configuration. If very few documents are polled at any given time, it is
   * likely that increasing the host load would have no effect on processing throughput.
   *
   * @return the maximum number of allowable active polling threads for this configuration
   */
  public int getHostload() {
    return hostload;
  }

  private TraverserConfiguration(Builder builder) {
    this.name = builder.name;
    this.pollRequest = builder.pollRequest;
    this.itemRetriever = builder.itemRetriever;
    this.batchItemRetriever = builder.batchItemRetriever;
    this.timeout = builder.timeout;
    this.timeunit = builder.timeunit;
    this.hostload = builder.hostload;
  }

  /**
   * Builder for {@link TraverserConfiguration} class.
   *
   * <p>Optional configuration parameters:
   * <ul>
   * <li>{@code traverser.pollRequest.queue} - Specifies the queue names that
   *     this traverser polls. Default: Empty string (implies "default")
   * <li>{@code traverser.pollRequest.statuses} - Specifies the specific item
   *     status' that this traverser polls. Default: Empty string (implies all status values)
   * <li>{@code traverser.pollRequest.limit} - Specifies the maximum number of
   *     items to return from a polling request. Default: 0 (implies API maximum)
   * <li>{@code traverser.timeout} - Specifies the timeout value for interrupting
   *     this traverser poll attempt. Default: {@code 60}
   * <li>{@code traverser.timeunit} - Specifies the timeout units.
   *     Default: SECONDS
   * <li>{@code traverser.hostload} - Specifies the maximum number of active
   *     parallel threads available for polling. Default: {@code 5}
   * </ul>
   */
  public static class Builder {

    public static final String TRAVERSER = "traverser";
    public static final String CONFIG_POLL_REQUEST_QUEUE = ".pollRequest.queue";
    public static final String CONFIG_POLL_REQUEST_STATUSES = ".pollRequest.statuses";
    public static final String CONFIG_POLL_REQUEST_LIMIT = ".pollRequest.limit";
    public static final String CONFIG_TIMEUNIT = ".timeunit";
    public static final String CONFIG_TIMEOUT = ".timeout";
    public static final String CONFIG_HOSTLOAD = ".hostload";

    public static final int CONFIG_HOSTLOAD_DEFAULT = 5;
    public static final int CONFIG_TIMEOUT_DEFAULT = 60;

    private final ConfigValue<Integer> defaultTimeout =
        Configuration.getInteger(TRAVERSER + CONFIG_TIMEOUT, CONFIG_TIMEOUT_DEFAULT);
    private final ConfigValue<TimeUnit> defaultTimeunit =
        Configuration
            .getValue(TRAVERSER + CONFIG_TIMEUNIT, TimeUnit.SECONDS, TimeUnit::valueOf);
    private final ConfigValue<Integer> defaultHostload =
        Configuration.getInteger(TRAVERSER + CONFIG_HOSTLOAD, CONFIG_HOSTLOAD_DEFAULT);
    private final ConfigValue<String> defaultQueueName =
        Configuration.getString(TRAVERSER + CONFIG_POLL_REQUEST_QUEUE, "default");
    private final ConfigValue<List<String>> defaultRequestStatuses =
        Configuration.getMultiValue(
                TRAVERSER + CONFIG_POLL_REQUEST_STATUSES,
                Collections.emptyList(),
                Configuration.STRING_PARSER);
    private final ConfigValue<Integer> defaultPollLimit =
        Configuration.getInteger(TRAVERSER + CONFIG_POLL_REQUEST_LIMIT, 0);

    private String name;
    private String queueName;
    private List<String> requestStatuses;
    private PollItemsRequest pollRequest;
    private ItemRetriever itemRetriever;
    private BatchItemRetriever batchItemRetriever;
    private long timeout;
    private int hostload;
    private TimeUnit timeunit;
    private int pollRequestLimit;

    /**
     * Creates a builder instance with default configuration properties.
     *
     * <p>The default configuration properties can be overridden:
     * <pre>
     *   {@code traverser.pollRequest.queue=default}
     *   {@code traverser.pollRequest.statuses=}
     *   {@code traverser.pollRequest.limit=0}
     *   {@code traverser.tiemout=60}
     *   {@code traverser.timeunit=SECONDS}
     *   {@code traverser.hostload=5}
     * </pre>
     */
    public Builder() {
      checkState(Configuration.isInitialized(), "Configuration should be initialized before using");
      this.name = null;
      this.timeout = defaultTimeout.get();
      this.timeunit = defaultTimeunit.get();
      this.hostload = defaultHostload.get();
      this.queueName = defaultQueueName.get();
      this.requestStatuses = defaultRequestStatuses.get();
      this.pollRequestLimit = defaultPollLimit.get();
    }

    // TODO(normang): use enum strings for status values in examples below

    /**
     * Creates the builder instance and populates the configuration properties using
     * {@code configKey} as follows:
     *
     * <pre>
     *   {@code traverser.[configKey].pollRequest.queue=QueueName}
     *   {@code traverser.[configKey].pollRequest.statuses=NEW_ITEM,MODIFIED}
     *   {@code traverser.[configKey].pollRequest.limit=30}
     *   {@code traverser.[configKey].timeout=20}
     *   {@code traverser.[configKey].timeunit=SECONDS}
     *   {@code traverser.[configKey].hostload=10}
     * </pre>
     *
     * <p>This example indicates that we should scale the traverser up to 10 parallel polling
     * threads and set the timeout to 20 seconds. Worker threads use the queue "QueueName" and poll
     * requests with statuses of only "NEW_ITEM" or "MODIFIED".
     *
     * <p>If configuration parameters with [configKey] are missing, the default parameters are
     * applied:
     *
     * <pre>
     *   {@code traverser.pollRequest.queue=default}
     *   {@code traverser.pollRequest.statuses=}
     *   {@code traverser.pollRequest.limit=0}
     *   {@code traverser.tiemout=60}
     *   {@code traverser.timeunit=SECONDS}
     *   {@code traverser.hostload=5}
     * </pre>
     *
     * <p>The default configuration properties can also be overridden.
     *
     * @param configKey the custom configuration key name (see example above)
     */
    public Builder(String configKey) {
      checkState(Configuration.isInitialized(), "Configuration should be initialized before using");
      String prefix = TRAVERSER + "." + configKey;
      this.name = configKey;
      this.timeout = Configuration.getOverriden(prefix + CONFIG_TIMEOUT, defaultTimeout).get();
      this.timeunit = Configuration.getOverriden(prefix + CONFIG_TIMEUNIT, defaultTimeunit).get();
      this.hostload = Configuration.getOverriden(prefix + CONFIG_HOSTLOAD, defaultHostload).get();
      this.queueName =
          Configuration.getOverriden(prefix + CONFIG_POLL_REQUEST_QUEUE, defaultQueueName).get();
      this.requestStatuses =
          Configuration.getOverriden(prefix + CONFIG_POLL_REQUEST_STATUSES, defaultRequestStatuses)
              .get();
      this.pollRequestLimit =
          Configuration.getOverriden(prefix + CONFIG_POLL_REQUEST_LIMIT, defaultPollLimit).get();
    }

    /**
     * Sets the name of the worker thread that can be used for logging.
     *
     * @param name the name of the worker thread
     * @return this builder
     */
    public Builder name(String name) {
      this.name = name;
      return this;
    }

    /**
     * Sets the {@link PollItemsRequest} that is used to poll queue items.
     *
     * @param pollRequest the pollRequest object
     * @return this builder
     */
    public Builder pollRequest(PollItemsRequest pollRequest) {
      this.pollRequest = pollRequest;
      return this;
    }

    /**
     * Sets the {@link ItemRetriever} to use for processing poll queue items.
     *
     * <p>Only one of {@link ItemRetriever} or {@link BatchItemRetriever} can be defined in a
     * {@link TraverserConfiguration} instance.
     *
     * @param itemRetriever the item retriever object
     * @return this builder
     */
    public Builder itemRetriever(ItemRetriever itemRetriever) {
      this.itemRetriever = itemRetriever;
      return this;
    }

    /**
     * Sets {@link BatchItemRetriever} to use for processing poll queue items.
     *
     * <p>Only one of {@link ItemRetriever} or {@link BatchItemRetriever} can be defined in a
     * {@link TraverserConfiguration} instance.
     *
     * @param batchItemRetriever the item retriever object
     * @return this builder
     */
    public Builder itemRetriever(BatchItemRetriever batchItemRetriever) {
      this.batchItemRetriever = batchItemRetriever;
      return this;
    }

    /**
     * Sets hostload (number of worker threads) used by {@link TraverserWorker}
     *
     * @param hostLoad number of worker threads ised by traverser worker
     * @return this builder
     */
    public Builder hostLoad(int hostLoad) {
      this.hostload = hostLoad;
      return this;
    }

    /**
     * Builds a {@link TraverserConfiguration} instance.
     *
     * @return the fully formed {@link TraverserConfiguration} instance
     */
    public TraverserConfiguration build() {
      checkArgument(
          (itemRetriever != null) ^ (batchItemRetriever != null),
          "one and only one itemRetriever or batchItemRetriever should be defined");
      checkArgument((timeout > 0) && (timeunit != null), "timeout should be greater than 0");
      checkArgument(hostload > 0, "hostload should be greater than 0");
      checkArgument(
          pollRequestLimit >= 0, "poll request limit should be greater than or equal to 0");

      pollRequest = new PollItemsRequest();
      if (!Strings.isNullOrEmpty(queueName)) {
        pollRequest.setQueue(queueName);
      }
      if ((requestStatuses != null) && (requestStatuses.size() > 0)) {
        List<String> badStatus = PollItemStatus.getBadStatus(requestStatuses);
        checkArgument(badStatus.isEmpty(), "Invalid PollItemStatus: " + badStatus.toString());
        pollRequest.setStatusCodes(requestStatuses);
      }
      if (pollRequestLimit > 0) {
        pollRequest.setLimit(pollRequestLimit);
      }
      return new TraverserConfiguration(this);
    }
  }
}
