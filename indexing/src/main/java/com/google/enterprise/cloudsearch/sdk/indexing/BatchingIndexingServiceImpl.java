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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Delete;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Index;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Push;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Unreserve;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.enterprise.cloudsearch.sdk.AsyncRequest;
import com.google.enterprise.cloudsearch.sdk.BatchPolicy;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.ExecutorFactory;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.SystemTimeProvider;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.TimeProvider;
import com.google.enterprise.cloudsearch.sdk.RetryPolicy;
import com.google.enterprise.cloudsearch.sdk.StatsManager;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.concurrent.ExecutorService;

/**
 * Concrete class implementing {@link BatchingIndexingService}.
 */
public class BatchingIndexingServiceImpl extends AbstractIdleService
    implements BatchingIndexingService {

  private final BatchRequestService batchService;
  private final RetryPolicy retryPolicy;
  private final OperationStats operationStats =
      StatsManager.getComponent(BatchingIndexingServiceImpl.class.getName());

  private BatchingIndexingServiceImpl(Builder builder) {
    this.retryPolicy = checkNotNull(builder.retryPolicy, "retry policy cannot be null!");
    this.batchService =
        new BatchRequestService.Builder(builder.service)
            .setBatchPolicy(builder.batchPolicy)
            .setRetryPolicy(builder.retryPolicy)
            .setExecutorFactory(builder.executorFactory)
            .setTimeProvider(builder.currentTimeProvider)
            .setGoogleCredential(builder.credential)
            .build();
  }

  @Override
  public ListenableFuture<Operation> indexItem(Index indexItem) throws InterruptedException {
    AsyncRequest<Operation> itemUpdate = new AsyncRequest<>(indexItem, retryPolicy, operationStats);
    batchService.add(itemUpdate);
    return itemUpdate.getFuture();
  }

  @Override
  public ListenableFuture<Item> pushItem(Push pushItem) throws InterruptedException {
    AsyncRequest<Item> itemPush = new AsyncRequest<>(pushItem, retryPolicy, operationStats);
    batchService.add(itemPush);
    return itemPush.getFuture();
  }

  @Override
  public ListenableFuture<Operation> deleteItem(Delete deleteItem) throws InterruptedException {
    AsyncRequest<Operation> itemDelete =
        new AsyncRequest<>(deleteItem, retryPolicy, operationStats);
    batchService.add(itemDelete);
    return itemDelete.getFuture();
  }

  @Override
  public ListenableFuture<Operation> unreserveItem(Unreserve unreserveItem)
      throws InterruptedException {
    AsyncRequest<Operation> itemUnreserve =
        new AsyncRequest<>(unreserveItem, retryPolicy, operationStats);
    batchService.add(itemUnreserve);
    return itemUnreserve.getFuture();
  }

  @Override
  protected void startUp() throws Exception {
    batchService.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    batchService.stopAsync().awaitTerminated();
  }

  public static BatchingIndexingService fromConfiguration(
      CloudSearch service, GoogleCredential credential) {
    checkState(Configuration.isInitialized(), "config not initialized");
    return new Builder()
        .setService(service)
        .setBatchPolicy(BatchPolicy.fromConfiguration())
        .setCredential(credential)
        .build();
  }

  /** Builder for creating an instance of {@link BatchingIndexingServiceImpl} */
  public static final class Builder {
    private CloudSearch service;
    private ExecutorFactory executorFactory = new BatchRequestService.ExecutorFactoryImpl();
    private BatchPolicy batchPolicy = new BatchPolicy.Builder().build();
    private RetryPolicy retryPolicy = new RetryPolicy.Builder().build();
    private TimeProvider currentTimeProvider = new SystemTimeProvider();
    private GoogleCredential credential;

    /**
     * Sets {@link CloudSearch} service client to be used for creating batch requests
     *
     * @param service {@link CloudSearch} service client to be used for creating batch requests
     * @return this builder instance
     */
    public Builder setService(CloudSearch service) {
      this.service = service;
      return this;
    }

    /**
     * Sets {@link ExecutorFactory} to be used for creating instances of {@link ExecutorService}
     * used by batching service
     *
     * @param executorFactory to be used for creating instances of {@link ExecutorService}
     * @return this builder instance
     */
    public Builder setExecutorFactory(ExecutorFactory executorFactory) {
      this.executorFactory = executorFactory;
      return this;
    }

    /**
     * Sets {@link BatchPolicy} to be used for batching requests
     *
     * @param batchPolicy to be used for batching requests
     * @return this builder instance
     */
    public Builder setBatchPolicy(BatchPolicy batchPolicy) {
      this.batchPolicy = batchPolicy;
      return this;
    }

    /**
     * Sets {@link RetryPolicy} to be used for batching requests
     *
     * @param retryPolicy to be used for batching requests
     * @return this builder instance
     */
    public Builder setRetryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return this;
    }

    /**
     * Sets {@link GoogleCredential} to be used for batching requests
     *
     * @param credential to be used for batching requests
     * @return this builder instance
     */
    public Builder setCredential(GoogleCredential credential) {
      this.credential = credential;
      return this;
    }

    /**
     * Builds an instance of {@link BatchingIndexingServiceImpl}
     *
     * @return an instance of {@link BatchingIndexingServiceImpl}
     */
    public BatchingIndexingServiceImpl build() {
      checkNotNull(service, "service can not be null");
      checkNotNull(credential, "credential can not be null");
      checkNotNull(executorFactory, "executorFactory can not be null");
      checkNotNull(batchPolicy, "batchPolicy can not be null");
      return new BatchingIndexingServiceImpl(this);
    }
  }
}
