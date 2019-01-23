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

import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import com.google.enterprise.cloudsearch.sdk.BatchPolicy;
import java.util.concurrent.Executor;

/**
 * Indexing Service for batching multiple API requests.
 *
 * <p>This service provides the ability to group a set of API requests together in order to send
 * them to Cloud Search in a single submission. The submission triggers are configurable using a
 * {@link BatchPolicy} object. This is commonly used by {@link IndexingServiceImpl} for API requests
 * that return empty response objects, such as push and delete.
 *
 * <p>Batched API operations are executed asynchronously. The caller can use the
 * {@link ListenableFuture#get()} method to obtain the result of a batched request. The caller can
 * also use {@link ListenableFuture#addListener(Runnable, Executor)} or
 * {@link Futures#addCallback(ListenableFuture, FutureCallback)}
 * to register callback methods to retrieve results from the batched operation.
 */
public interface BatchingIndexingService extends Service {
  /** Adds an index item request to the batch. */
  ListenableFuture<Operation> indexItem(Items.Index indexItem) throws InterruptedException;

  /** Adds a push item request to the batch. */
  ListenableFuture<Item> pushItem(Items.Push pushItem) throws InterruptedException;

  /** Adds a delete item request to the batch. */
  ListenableFuture<Operation> deleteItem(Items.Delete deleteItem) throws InterruptedException;

  /** Adds an unreserve queue request to the batch. */
  ListenableFuture<Operation> unreserveItem(Items.Unreserve unreserveItem)
      throws InterruptedException;
}
