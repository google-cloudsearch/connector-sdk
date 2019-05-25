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

import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.json.GenericJson;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import java.util.List;

/**
 * Context object to enable the {@link Repository} to make asynchronous calls to the Cloud Search
 * SDK.
 *
 * <p>Normally the SDK initiates all data repository access based on its traversal scheduling
 * methods. If the data repository supports automatic document modification detection, then this
 * context object is used by the {@link Repository} to perform updates asynchronously to the
 * scheduled traversal calls.
 *
 * <p>This context object is created by the template traversal connectors
 * {@link FullTraversalConnector} and {@link ListingConnector} during initialization. The connector
 * passes the context to the {@link Repository#init(RepositoryContext)} method. The context is then
 * saved for use when the data repository reports a document change.
 */
public class RepositoryContext {

  private final EventBus eventBus;
  private final DefaultAclMode defaultAclMode;

  private RepositoryContext(Builder builder) {
    this.eventBus = builder.eventBus;
    this.defaultAclMode = builder.defaultAclMode;
  }

  /**
   * Posts an {@link AsyncApiOperation} from the {@link Repository}.
   *
   * <p>This is the call back method for data repository document modification detection. Sample
   * usage is detailed in the {@link AsyncApiOperation} documentation.
   *
   * @deprecated Use {@link #postApiOperationAsync(ApiOperation)}
   */
  @Deprecated()
  public void postAsyncOperation(AsyncApiOperation operation) {
    eventBus.post(operation);
  }

  /**
   * Posts an {@link ApiOperation} from the {@link Repository} asynchronously.
   *
   * <p>This is the call back method for data repository document modification detection.
   * <p>Sample usage:
   * <pre><code>
   * public MyRepository implements Repository {
   *   {@literal @}Override
   *   public void init(RepositoryContext context) {
   *     this.context = context;
   *   }
   *
   *   private void onDocumentRemoved(String docId) {
   *     ApiOperation operation = ApiOperations.deleteItem(docId);
   *
   *     Futures.addCallback(
   *         this.context.postApiOperationAsync(operation),
   *         new FutureCallback&lt;List&lt;GenericJson&gt;&gt;() {
   *           {@literal @}Override
   *           public void onSuccess(@Nullable List&lt;GenericJson&gt; result) {
   *             // Acking logic.
   *           }
   *
   *           {@literal @}Override
   *           public void onFailure(Throwable t) {
   *             // Error handling.
   *           }
   *         },
   *         MoreExecutors.directExecutor());
   *   }
   *
   *   // other implemented methods
   * }
   * </code></pre>
   */
  public ListenableFuture<List<GenericJson>> postApiOperationAsync(ApiOperation operation) {
    AsyncApiOperation asyncApiOperation = new AsyncApiOperation(operation);
    eventBus.post(asyncApiOperation);
    return asyncApiOperation.getResult();
  }

  EventBus getEventBus() {
    return eventBus;
  }

  /**
   * Gets {@link DefaultAclMode} as configured in connector configuration.
   *
   * @return {@link DefaultAclMode} as configured in connector configuration.
   */
  public DefaultAclMode getDefaultAclMode() {
    return defaultAclMode;
  }

  static class Builder {

    private EventBus eventBus;
    private DefaultAclMode defaultAclMode;

    Builder setEventBus(EventBus eventBus) {
      this.eventBus = eventBus;
      return this;
    }

    Builder setDefaultAclMode(DefaultAclMode defaultAclMode) {
      this.defaultAclMode = defaultAclMode;
      return this;
    }

    RepositoryContext build() {
      checkState(eventBus != null, "EventBus must be set!");
      return new RepositoryContext(this);
    }
  }
}
