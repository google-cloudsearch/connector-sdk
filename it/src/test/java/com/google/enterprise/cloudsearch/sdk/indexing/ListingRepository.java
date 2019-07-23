/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.enterprise.cloudsearch.sdk.indexing;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperations;
import com.google.enterprise.cloudsearch.sdk.indexing.template.PushItems;
import com.google.enterprise.cloudsearch.sdk.indexing.template.Repository;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Repository implementation for testing ListingConnector.
 */
public class ListingRepository implements Repository {
  private static final Logger logger = Logger.getLogger(ListingRepository.class.getName());

  private Map<String, MockItem> rootItems = new HashMap<>();
  private Map<String, MockItem> childItems = new HashMap<>();
  private List<String> changedIds = new ArrayList<>();
  private CountDownLatch getDocLatch;
  private CountDownLatch getChangesLatch = new CountDownLatch(1);
  private Runnable getDocCallback;

  ListingRepository() {
  }

  ListingRepository addRootItems(MockItem... rootItems) {
    for (MockItem mockItem : rootItems) {
      this.rootItems.put(mockItem.getName(), mockItem);
    }
    return this;
  }

  ListingRepository addChildItems(MockItem... childItems) {
    for (MockItem mockItem : childItems) {
      this.childItems.put(mockItem.getName(), mockItem);
    }
    return this;
  }

  ListingRepository addChangedItems(MockItem... changedItems) {
    for (MockItem mockItem : changedItems) {
      childItems.put(mockItem.getName(), mockItem);
      changedIds.add(mockItem.getName());
    }
    return this;
  }

  Optional<MockItem> findMockItem(String name) {
    return Stream.concat(rootItems.values().stream(), childItems.values().stream())
        .filter(mockItem -> mockItem.getName().equals(name))
        .findFirst();
  }

  List<String> findChildren(String name) {
    return childItems.values().stream()
        .filter(mockItem -> name.equals(mockItem.getContainerName()))
        .map(MockItem::getName)
        .collect(Collectors.toList());
  }

  @Override
  public void init(RepositoryContext context) throws RepositoryException {
    getDocLatch = new CountDownLatch(rootItems.size() + childItems.size());
  }

  @Override
  public CheckpointCloseableIterable<ApiOperation> getIds(@Nullable byte[] checkpoint)
      throws RepositoryException {
    PushItems.Builder builder = new PushItems.Builder();
    for (String name : rootItems.keySet()) {
      builder.addPushItem(name, new PushItem());
    }
    return new CheckpointCloseableIterableImpl.Builder<ApiOperation>(
        Arrays.asList(builder.build())).build();
  }

  @Override
  public CheckpointCloseableIterable<ApiOperation> getChanges(@Nullable byte[] checkpoint)
      throws RepositoryException {
    try {
      PushItems.Builder builder = new PushItems.Builder();
      for (String id : changedIds) {
        builder.addPushItem(id, new PushItem().setType("MODIFIED"));
      }
      return new CheckpointCloseableIterableImpl.Builder<ApiOperation>(
          Arrays.asList(builder.build())).build();

    } finally {
      changedIds.clear();
      getChangesLatch.countDown();
    }
  }

  @Override
  public CheckpointCloseableIterable<ApiOperation> getAllDocs(@Nullable byte[] checkpoint)
      throws RepositoryException {
    throw new UnsupportedOperationException("Method not supported.");
  }

  @Override
  public ApiOperation getDoc(Item polledItem) throws RepositoryException {
    Optional<MockItem> mockItem = findMockItem(polledItem.getName());
    if (!mockItem.isPresent()) {
      logger.log(Level.FINE, "No such item: " + polledItem.getName());
      return ApiOperations.deleteItem(polledItem.getName());
    }
    try {
      logger.log(Level.FINE, "Sending: " + polledItem.getName());
      Item item = mockItem.get().getItem();
      RepositoryDoc.Builder builder = new RepositoryDoc.Builder().setItem(item);
      for (String childName : findChildren(item.getName())) {
        builder.addChildId(childName, new PushItem());
      }
      return builder.build();
    } finally {
      getDocLatch.countDown();
      if (getDocCallback != null) {
        getDocCallback.run();
      }
    }
  }

  @Override
  public boolean exists(Item item) throws RepositoryException {
    return false;
  }

  @Override
  public void close() {
  }

  public void awaitGetDoc(long timeout, TimeUnit unit) throws InterruptedException {
    getDocLatch.await(timeout, unit);
  }

  public void awaitGetChanges(long timeout, TimeUnit unit) throws InterruptedException {
    getChangesLatch.await(timeout, unit);
  }

  public void notifyGetDoc(Runnable callback) throws InterruptedException {
    getDocCallback = callback;
  }
}
