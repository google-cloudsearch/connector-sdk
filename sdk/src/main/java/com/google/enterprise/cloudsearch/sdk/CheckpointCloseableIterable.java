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

/**
 * A {@link CloseableIterable} that supports pagination while traversing a repository or incremental
 * changes. An implementation can optionally break a large traversal process into smaller pieces,
 * using checkpoints to track progress, and returning {@code true} from {@link
 * CheckpointCloseableIterable#hasMore} to indicate more objects are available.
 *
 * <p>Refer to {@link CheckpointCloseableIterableImpl} for an implementation.
 */
public interface CheckpointCloseableIterable<T> extends CloseableIterable<T> {
  /**
   * Get current checkpoint value. Framework is expected to call this method only after iterating
   * through all objects available.
   *
   * @return checkpoint value to save
   * @throws RuntimeException if computation of checkpoint value fails.
   */
  public byte[] getCheckpoint();

  /**
   * Flag to indicate if more items are available to traverse beyond current set of items returned
   * as part of {@link Iterable}. Framework is expected to call this method only after iterating
   * through all objects available.
   *
   * @return True if more objects are available for processing after current batch. False if no more
   *     objects available for traversal.
   */
  public boolean hasMore();
}
