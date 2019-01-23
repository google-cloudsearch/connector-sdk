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

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/** {@link Iterable} which handles pagination for iterating over set of objects. */
public abstract class PaginationIterable<T, Q> implements Iterable<T> {

  private final Iterable<T> delegate;

  /** Represents set of items and optional continuation token to fetch additional items. */
  public static class Page<T, Q> {
    private final List<T> results;
    private final Optional<Q> nextPageToken;

    /**
     * Creates {@link PaginationIterable.Page} with given set of items and continuation token
     *
     * @param results set of items part of current page
     * @param nextPageToken optional continuation token to fetch next page
     */
    public Page(List<T> results, Optional<Q> nextPageToken) {
      this.results = checkNotNull(results);
      this.nextPageToken = checkNotNull(nextPageToken);
    }
  }

  /** Creates an instance of {@link PaginationIterable} */
  public PaginationIterable(Optional<Q> startPage) {
    delegate = Iterables.concat(new ListPaginationIterable<T, Q>(startPage, this::fetchPage));
  }

  /**
   * Return next set of objects based on optional pagination token.
   *
   * @param nextPage pagination token
   * @return {@link Page} containing next set of objects and optional continuation token.
   * @throws IOException if fetching next set of objects fails
   */
  public abstract Page<T, Q> getPage(Optional<Q> nextPage) throws IOException;

  @Override
  public Iterator<T> iterator() {
    return delegate.iterator();
  }

  private Page<T, Q> fetchPage(Optional<Q> paginationToken) {
    try {
      return getPage(paginationToken);
    } catch (IOException e) {
      throw new RuntimeException("Error fetching page", e);
    }
  }

  private static class ListPaginationIterable<T, Q> implements Iterable<List<T>> {
    private final Optional<Q> startPageToken;
    private final Function<Optional<Q>, Page<T, Q>> pageFetcher;

    private ListPaginationIterable(
        Optional<Q> startPage, Function<Optional<Q>, Page<T, Q>> pageFetcher) {
      this.startPageToken = checkNotNull(startPage);
      this.pageFetcher = checkNotNull(pageFetcher);
    }

    @Override
    public Iterator<List<T>> iterator() {
      return new ListPaginationIterator(new Page<>(Collections.emptyList(), startPageToken));
    }

    private class ListPaginationIterator implements Iterator<List<T>> {
      private final AtomicReference<Page<T, Q>> currentPage;
      private final AtomicBoolean firstPageLoaded = new AtomicBoolean();

      ListPaginationIterator(Page<T, Q> startPage) {
        currentPage = new AtomicReference<>(startPage);
      }

      @Override
      public boolean hasNext() {
        while (currentPage.get().results.isEmpty()
            && (currentPage.get().nextPageToken.isPresent() || !firstPageLoaded.get())) {
          currentPage.set(checkNotNull(pageFetcher.apply(currentPage.get().nextPageToken)));
          firstPageLoaded.set(true);
        }
        return !currentPage.get().results.isEmpty();
      }

      @Override
      public List<T> next() {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
        List<T> results = currentPage.get().results;
        currentPage.set(new Page<>(Collections.emptyList(), currentPage.get().nextPageToken));
        return results;
      }
    }
  }
}
