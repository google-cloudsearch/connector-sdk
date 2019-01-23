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

import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import java.util.Iterator;

/**
 * A factory for creating {@link ApiOperation} instances.
 *
 * <p>This class consists exclusively of static methods that operate on or return specific
 * {@link ApiOperation} instances used in template classes (such as {@link Repository}).
 */
public class ApiOperations {

  // Suppresses default constructor, ensuring non-instantiability.
  private ApiOperations() {
  }

  /**
   * Creates a {@link DeleteItem} instance.
   *
   * @param id to delete
   * @return {@link DeleteItem}
   */
  public static ApiOperation deleteItem(String id) {
    return new DeleteItem(id);
  }

  /**
   * Creates a {@link DeleteItem} instance with a custom version.
   *
   * @param id to delete
   * @param version item version for delete
   * @param requestMode mode for request
   * @return {@link DeleteItem}
   */
  public static ApiOperation deleteItem(String id, byte[] version, RequestMode requestMode) {
    return new DeleteItem(id, version, requestMode);
  }

  /**
   * Creates a {@link DeleteQueueItems} instance.
   *
   * @param queueName items with this queue name will be deleted
   * @return {@link DeleteQueueItems}
   */
  public static ApiOperation deleteQueueItems(String queueName) {
    return new DeleteQueueItems(queueName);
  }

  /**
   * Creates a {@link BatchApiOperation} instance.
   *
   * <p>Use this method to batch multiple {@link ApiOperation} instances in a single operation.
   *
   * @param operations to batch
   * @return {@link BatchApiOperation}
   */
  public static ApiOperation batch(Iterator<ApiOperation> operations) {
    return new BatchApiOperation(operations);
  }
}
