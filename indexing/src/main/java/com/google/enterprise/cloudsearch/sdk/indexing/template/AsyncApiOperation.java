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
package com.google.enterprise.cloudsearch.sdk.indexing.template;

import com.google.api.client.json.GenericJson;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import java.util.List;

/**
 * An {@link ApiOperation} that the {@link Repository} pushes asynchronously.
 *
 * <p>The {@link Repository} can push {@link AsyncApiOperation} objects using the {@link EventBus}
 * passed in from its {@link Repository#init(RepositoryContext)} method. Typically, the
 * {@link Repository} performs on-demand API operations whenever it implements a scheduled or
 * event-driven change-detection mechanism.
 *
 * <p>The {@link ListenableFuture} result represents the future result of the operation accessible
 * from {@link AsyncApiOperation#getResult()}.
 *
 * <p>Sample usage:
 *
 * <pre><code>
 * public MyRepository implements Repository {
 *  {@literal @}Override
 *   public void init(RepositoryContext context) {
 *     this.context = context;
 *   }
 *
 *   private void onDocumentRemoved(String docId) {
 *     AsyncApiOperation operation = new AsyncApiOperation(ApiOperations.deleteItem(docId));
 *     this.context.postAsyncOperation(operation);
 *   }
 *
 *   // other implemented methods
 * }
 * </code></pre>
 */
public class AsyncApiOperation {

  private final ApiOperation operation;
  private SettableFuture<List<GenericJson>> result = SettableFuture.create();

  /**
   * Constructs {@link AsyncApiOperation} to be posted on {@link
   * RepositoryContext#postAsyncOperation}.
   *
   * @param operation {@link ApiOperation} to be executed asynchronously.
   */
  public AsyncApiOperation(ApiOperation operation) {
    this.operation = operation;
  }
  /**
   * Gets {@link ApiOperation} to be executed.
   *
   * @return operation to be executed.
   */
  public ApiOperation getOperation() {
    return operation;
  }

  /**
   * Gets result for {@link ApiOperation} operation execution.
   *
   * @return result for {@link ApiOperation} operation execution.
   */
  public ListenableFuture<List<GenericJson>> getResult() {
    return result;
  }

  /** Sets result for {@link ApiOperation} operation execution. */
  void setResult(ListenableFuture<List<GenericJson>> futureResult) {
    this.result.setFuture(futureResult);
  }
}
