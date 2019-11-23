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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.json.GenericJson;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * {@link ApiOperation} to push {@link com.google.api.services.cloudsearch.v1.model.Item} objects to
 * the Cloud Search queue.
 *
 * <p>This object calls {@link IndexingService#push(String, PushItem)}. The {@link Repository} can
 * return this operation to push items from the data repository.
 */
public class PushItems implements ApiOperation {

  private final List<PushItemResource> items;

  private PushItems(List<PushItemResource> items) {
    this.items = Collections.unmodifiableList(new ArrayList<PushItemResource>(checkNotNull(items)));
  }

  /** Builder for creating an instance of {@link PushItems} operation */
  public static class Builder {
    private final List<PushItemResource> items = new ArrayList<>();

    /**
     * Adds given {@link PushItem} in list of items to be pushed as part of {@link ApiOperation}
     * execution.
     *
     * @param id for item to be pushed
     * @param item to be pushed
     * @return this instance of {@link PushItems.Builder}
     */
    public Builder addPushItem(String id, PushItem item) {
      items.add(new PushItemResource(id, item));
      return this;
    }

    /**
     * Builds an instance of {@link PushItems} operation.
     *
     * @return an instance of {@link PushItems} operation.
     */
    public PushItems build() {
      return new PushItems(items);
    }
  }

  @Override
  public List<GenericJson> execute(IndexingService service)
      throws IOException, InterruptedException {
    List<ListenableFuture<? extends GenericJson>> futures = new ArrayList<>();
    for (PushItemResource pushItem : items) {
      futures.add(service.push(pushItem.getId(), pushItem.getItem()));
    }

    try {
      return Futures.allAsList(futures).get();
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  public List<PushItemResource> getPushItemResources() {
    return ImmutableList.copyOf(items);
  }

  @Override
  public String toString() {
    return "PushItems [items=" + items.toString() + "]";
  }

  @Override
  public int hashCode() {
    return items.hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof PushItems)) {
      return false;
    }
    PushItems otherEntries = (PushItems) other;
    return this.items.equals(otherEntries.items);
  }
}
