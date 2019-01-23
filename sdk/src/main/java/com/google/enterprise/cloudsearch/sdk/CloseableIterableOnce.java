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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link CloseableIterable} that can be iterated over at most once. Additional calls to
 * {@code iterator()} will throw an {@code IllegalStateException}.
 * <p>
 * This class does not override {@code Object.equals}.
 */
public class CloseableIterableOnce<T> implements CloseableIterable<T> {
  private AtomicReference<Iterator<T>> iterator;

  /**
   * Constructs an instance wrapping the given iterator.
   *
   * @param iterator the iterator to wrap
   * @throws NullPointerException if {@code iterator} is {@code null}
   */
  public CloseableIterableOnce(Iterator<T> iterator) {
    this.iterator = new AtomicReference<>(checkNotNull(iterator));
  }

  /**
   * Gets the wrapped iterator on the first call, and throws an exception on all subsequent calls.
   *
   * @return the wrapped iterator on the first call
   * @throws IllegalStateException if this method has been called on this instance before
   */
  @Override
  public Iterator<T> iterator() {
    Iterator<T> temp = iterator.getAndSet(null);
    if (temp == null) {
      throw new IllegalStateException("iterator is exhausted");
    }
    return temp;
  }

  /**
   * Closes any resources associated with this iterator. This
   * implementation invalidates the iterator.
   */
  @Override
  public void close() {
    iterator.set(null);
  }
}
