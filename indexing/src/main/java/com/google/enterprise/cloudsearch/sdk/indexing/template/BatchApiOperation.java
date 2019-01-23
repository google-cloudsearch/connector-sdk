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
import com.google.common.collect.Iterators;
import com.google.enterprise.cloudsearch.sdk.CloseableIterableOnce;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Container for a sequence of {@link ApiOperation} objects.
 *
 * <p>The {@link Repository} can return this object to perform multiple operations when required by
 * the connector. This object calls the {@link IndexingService} object for each of its contained
 * operations.
 *
 * <p>For example, if a {@link Repository#getDoc(com.google.api.services.cloudsearch.v1.model.Item)}
 * method call determines that the requested document has some of its child documents missing from
 * the data repository. A delete operation for each of these children can be concurrently sent in a
 * batch operation along with the requested document's update operation.
 *
 * <p><strong>This class implements {@code Iterable}, but the {@code iterator} and {@code equals}
 * methods both exhaust the iterator and should only be used by tests.</strong>
 */
public class BatchApiOperation implements ApiOperation, Iterable<ApiOperation> {

  private final CloseableIterableOnce<ApiOperation> operations;

  BatchApiOperation(Iterator<ApiOperation> operations) {
    this.operations = new CloseableIterableOnce<>(operations);
  }

  @Override
  public List<GenericJson> execute(IndexingService service)
      throws IOException, InterruptedException {
    return execute(service, Optional.empty());
  }

  @Override
  public List<GenericJson> execute(IndexingService service,
      Optional<Consumer<ApiOperation>> operationModifier)
      throws IOException, InterruptedException {
    List<GenericJson> res = new ArrayList<>();
    for (ApiOperation operation : operations) {
      res.addAll(operation.execute(service, operationModifier));
    }
    return res;
  }

  /**
   * Gets an iterator over the {@code ApiOperation}s. This implementation exhausts the
   * iterator, and should only be used by tests.
   *
   * @return an iterator on the first call
   * @throws IllegalStateException if this method has been called on this instance before
   */
  @Override
  public Iterator<ApiOperation> iterator() {
    return operations.iterator();
  }

  /**
   * Indicates whether another object is also a {@code BatchApiOperation} and iterates
   * over the same objects. This implementation exhausts the iterators of both objects,
   * and should only be used by tests.
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof BatchApiOperation)) {
      return false;
    }
    BatchApiOperation other = (BatchApiOperation) o;
    return Iterators.elementsEqual(this.iterator(), other.iterator());
  }

  /**
   * Gets a hash code for this object. This implementation always returns the same value,
   * in order to remain consistent with {@link #equals} without exhausting the iterator.
   *
   * @return a fixed hash code
   */
  @Override
  public int hashCode() {
    return 13;
  }
}
