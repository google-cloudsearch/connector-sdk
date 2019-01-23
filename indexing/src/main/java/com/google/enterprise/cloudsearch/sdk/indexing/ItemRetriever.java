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

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemContent;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.ItemStructuredData;
import com.google.api.services.cloudsearch.v1.model.PollItemsResponse;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.enterprise.cloudsearch.sdk.Connector;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import java.io.IOException;

/**
 * Connector specific implementation for handling {@link Item} from {@link PollItemsResponse}.
 *
 * <p>This interface is used by connectors implementing a listing traversal strategy, including the
 * template connector
 * {@link com.google.enterprise.cloudsearch.sdk.indexing.template.ListingConnector}.
 *
 * <p>During {@link Connector#init}, the listing connector typically registers one or more
 * {@link ItemRetriever} instances using {@link IndexingConnectorContext#registerTraverser}. After
 * {@link Connector#init} is executed, the SDK initializes worker threads to handle polled items.
 */
public interface ItemRetriever {
  /**
   * Retrieves content and metadata for the polled {@link Item}.
   *
   * <p>This method should provide the document's content, structured data, and metadata. The item's
   * {@link ItemContent#getHash}, {@link ItemStructuredData#getHash()}, and/or
   * {@link ItemMetadata#getHash} may be provided to allow the Cloud Search queue to automatically
   * track document changes during the next traversal.
   *
   * <p>If the connector implements a graph traversal strategy to navigate a hierarchical
   * repository, this method should also push {@link PushItem} objects for any child documents when
   * the item is a repository container.
   *
   * <p>This method should be highly parallelizable and support ten or more concurrent calls.
   *
   * @param item {@link Item} object representing the document being sought
   * @throws IOException on errors retrieving the document's data, typically a
   *     {@link RepositoryException}
   * @throws InterruptedException on IO operation errors
   */
  void process(Item item) throws IOException, InterruptedException;
}
