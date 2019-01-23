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

import static java.lang.String.format;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.Repository;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * An indexing template {@link Repository} allows to list items in "pages".
 * Following is an example of canonical usage on this repository:
 *
 * <pre>
 *   FakeIndexingRepository repo = new FakeIndexingRepository.Builder()
 *      .addFullTraversalPage(page1)
 *      .addFullTraversalPage(page2).build();
 * </pre>
 * Each page contains {@code List<MockItem>}.
 */
public class FakeIndexingRepository implements Repository {
  private static final Logger logger = Logger.getLogger(FakeIndexingRepository.class.getName());
  private final CountDownLatch closeLatch = new CountDownLatch(1);
  private final ImmutableList<Page> fullTraversalPages;

  private FakeIndexingRepository(List<Page> fullTraversalPages) {
    this.fullTraversalPages = ImmutableList.copyOf(fullTraversalPages);
  }

  @Override
  public void init(RepositoryContext context) throws RepositoryException {}

  @Override
  public CheckpointCloseableIterable<ApiOperation> getIds(@Nullable byte[] checkpoint)
      throws RepositoryException {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public CheckpointCloseableIterable<ApiOperation> getChanges(@Nullable byte[] checkpoint)
      throws RepositoryException {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public CheckpointCloseableIterable<ApiOperation> getAllDocs(@Nullable byte[] checkpoint)
      throws RepositoryException {
    String parsed = checkpoint == null ? "" : new String(checkpoint, StandardCharsets.UTF_8);
    java.util.Optional<Page> page =
        fullTraversalPages
        .stream()
        .filter(p -> Objects.equals(p.forCheckpoint, parsed))
        .findFirst();
    if (!page.isPresent()) {
      throw new RepositoryException.Builder()
      .setErrorMessage(format("unexpected checkpoint %s", parsed))
      .build();
    }
    Page currentPage = page.get();
    List<MockItem> currentPageItems = currentPage.items;
    logger.log(Level.FINE, "Number of items in current page during full traversal: {0}",
        currentPageItems.size());
    List<ApiOperation> operations =
        currentPageItems
        .stream()
        .map(i -> getItem(i))
        .collect(Collectors.toList());
    return new CheckpointCloseableIterableImpl.Builder<>(operations)
        .setCheckpoint(currentPage.nextCheckpoint.getBytes())
        .setHasMore(currentPage.hasMore)
        .build();
  }

  @Override
  public ApiOperation getDoc(Item item) throws RepositoryException {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public boolean exists(Item item) throws RepositoryException {
    return false;
  }

  @Override
  public void close() {
    closeLatch.countDown();
  }

  public void awaitForClose(long timeout, TimeUnit unit) throws InterruptedException {
    closeLatch.await(timeout, unit);
  }

  private static class Page {
    final String forCheckpoint;
    final String nextCheckpoint;
    final List<MockItem> items;
    final boolean hasMore;

    private Page(String forCheckpoint, String nextCheckpoint, List<MockItem> items,
        boolean hasMore) {
      this.forCheckpoint = forCheckpoint;
      this.nextCheckpoint = nextCheckpoint;
      this.items = items;
      this.hasMore = hasMore;
    }
  }

  static class Builder {
    private final List<List<MockItem>> pages = new ArrayList<>();

    public Builder addPage(List<MockItem> items) {
      pages.add(items);
      return this;
    }

    public FakeIndexingRepository build() {
      int counter = 0;
      String currentCheckpoint = "";
      Iterator<List<MockItem>> allPages = pages.iterator();
      List<Page> fullTraversalPages = new ArrayList<>();
      while (allPages.hasNext()) {
        List<MockItem> current = allPages.next();
        boolean hasNext = allPages.hasNext();
        String nextCheckpoint = hasNext ? Integer.toString(++counter) : "";
        fullTraversalPages.add(new Page(currentCheckpoint, nextCheckpoint, current, hasNext));
        logger.log(Level.FINE, "Current Checkpoint {0}, next checkpoint {1}. hasMore {2}",
            new Object[] {currentCheckpoint, nextCheckpoint, hasNext});
        currentCheckpoint = nextCheckpoint;
      }
      return new FakeIndexingRepository(fullTraversalPages);
    }
  }

  private RepositoryDoc getItem(MockItem createItem) {
    return new RepositoryDoc.Builder()
        .setItem(createItem.getItem())
        .build();
  }
}