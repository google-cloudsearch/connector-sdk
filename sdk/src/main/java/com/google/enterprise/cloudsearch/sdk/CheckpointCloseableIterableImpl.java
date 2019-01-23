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

import com.google.common.collect.Iterators;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * {@link CheckpointCloseableIterable} implementation which supports lazy initialization of {@link
 * CheckpointCloseableIterable#getCheckpoint} value and {@link CheckpointCloseableIterable#hasMore}.
 */
public class CheckpointCloseableIterableImpl<T> implements CheckpointCloseableIterable<T> {

  private final CloseableIterable<T> delegate;
  private final Supplier<byte[]> checkpoint;
  private final Supplier<Boolean> hasMore;

  private CheckpointCloseableIterableImpl(Builder<T> builder) {
    this.delegate = checkNotNull(builder.delegate);
    this.checkpoint = checkNotNull(builder.checkpoint);
    this.hasMore = checkNotNull(builder.hasMore);
  }

  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public byte[] getCheckpoint() {
    return checkpoint.get();
  }

  @Override
  public boolean hasMore() {
    return hasMore.get();
  }

  /** Builder object for {@link CheckpointCloseableIterableImpl} */
  public static class Builder<T> {
    private final CloseableIterable<T> delegate;
    private Supplier<byte[]> checkpoint = () -> null;
    private Supplier<Boolean> hasMore = () -> false;

    /**
     * Constructs a builder that clones the given {@code CheckpointCloseableIterable}. This
     * constructor should rarely be needed, but exists to avoid hard-to-debug errors if the {@link
     * CloseableIterable} overload were used instead, since that would ignore the checkpoint and
     * more items flag.
     *
     * <p>Changes to {@code delegate} are reflected in the constructed {@code
     * CheckpointCloseableIterable} by default, unless the {@link #setCheckpoint} or {@link
     * #setHasMore} methods are called on this builder.
     */
    public Builder(CheckpointCloseableIterable<T> delegate) {
      this.delegate = delegate;
      this.checkpoint = () -> delegate.getCheckpoint();
      this.hasMore = () -> delegate.hasMore();
    }

    /**
     * Constructs a {@link CheckpointCloseableIterableImpl.Builder} that wraps given {@link
     * CloseableIterable}
     */
    public Builder(CloseableIterable<T> delegate) {
      this.delegate = delegate;
    }

    /**
     * Constructs a {@link CheckpointCloseableIterableImpl.Builder} that wraps given {@link
     * Iterator}
     */
    public Builder(Iterator<T> delegate) {
      this.delegate = new CloseableIterableOnce<T>(delegate);
    }

    /** Constructs a {@link CheckpointCloseableIterableImpl.Builder} that wraps given collection. */
    public Builder(Collection<T> operations) {
      this.delegate = new CloseableIterableOnce<T>(operations.iterator());
   }

    /**
     * Sets checkpoint to be committed after processing of all items returned in current iterable.
     *
     * @param checkpoint to be committed after processing of all items.
     */
    public Builder<T> setCheckpoint(byte[] checkpoint) {
      this.checkpoint = () -> checkpoint;
      return this;
    }

    /**
     * Sets {@link Supplier} for checkpoint to be committed after processing of all items. {@link
     * Supplier#get} is invoked only after iterating over all items.
     *
     * @param checkpoint to be committed after processing of all items.
     */
    public Builder<T> setCheckpoint(Supplier<byte[]> checkpoint) {
      this.checkpoint = checkpoint;
      return this;
    }

    /**
     * Sets flag to indicate if more items are available for processing beyond items returned in
     * current {@link Iterable}.
     *
     * @param hasMore flag to indicate if more items are available for processing.
     */
    public Builder<T> setHasMore(boolean hasMore) {
      this.hasMore = () -> hasMore;
      return this;
    }

    /**
     * Sets {@link Supplier} for flag to indicate if more items are available for processing beyond
     * items returned in current {@link Iterable}. {@link Supplier#get} is invoked only after
     * iterating over all items.
     *
     * @param hasMore flag to indicate if more items are available for processing.
     */
    public Builder<T> setHasMore(Supplier<Boolean> hasMore) {
      this.hasMore = hasMore;
      return this;
    }

    /** Builds an instance of {@link CheckpointCloseableIterableImpl} */
    public CheckpointCloseableIterableImpl<T> build() {
      return new CheckpointCloseableIterableImpl<T>(this);
    }
  }

  /**
   * A {@link TestRule} to compare two instances of {@link CheckpointCloseableIterable}s. {@code
   * null} CheckpointCloseableIterable are considered equal. This rule iterates over the {@code
   * ApiOperation}s before comparing the checkpoint and pagination values.
   */
  public static class CompareCheckpointCloseableIterableRule<T> implements TestRule {

    private CompareCheckpointCloseableIterableRule() {}

    @Override
    public Statement apply(Statement base, Description description) {
      return base;
    }

    /**
     * Gets instance of {@link
     * CheckpointCloseableIterableImpl.CompareCheckpointCloseableIterableRule}.
     */
    public static <T> CompareCheckpointCloseableIterableRule<T> getCompareRule() {
      return new CompareCheckpointCloseableIterableRule<T>();
    }

    /**
     * Compares instances of {@link CheckpointCloseableIterable}. The comparison exhausts the
     * supplied iterable before comparing checkpoint and hasMore flag.
     *
     * @param items1 to compare
     * @param items2 to compare
     * @return true if supplied CheckpointCloseableIterable match, false otherwise.
     */
    public boolean compare(
        CheckpointCloseableIterable<T> items1, CheckpointCloseableIterable<T> items2) {
      if (items1 == items2) {
        return true;
      }
      if ((items1 == null) || (items2 == null)) {
        return false;
      }
      // The iterators must be compared first to cause iteration through the items. This allows
      // the repository to process each item and optionally update the checkpoint with data from
      // the item (e.g. database connector might compare a last modified field to get the latest).
      // Only after the changes are processed can the correct checkpoint and pagination be compared.
      return Iterators.elementsEqual(items1.iterator(), items2.iterator())
          && Arrays.equals(items1.getCheckpoint(), items2.getCheckpoint())
          && (items1.hasMore() == items2.hasMore());
    }
  }
}
