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

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.UploadItemRef;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nullable;

/**
 * Access point between the connector developer and the indexing service API backend.
 */
public interface IndexingService extends Service {

  public enum ContentFormat {
    UNSPECIFIED,
    HTML,
    TEXT,
    RAW
  }

  /** Request mode for {@link Item} index and delete requests. */
  public enum RequestMode {
    /**
     * Priority is not specified in the update request. Leaving priority unspecified results in an
     * update failure.
     */
    UNSPECIFIED,
    /**
     * For real-time updates.
     */
    SYNCHRONOUS,
    /** For changes that are executed after the response is sent back to the caller. */
    ASYNCHRONOUS
  }

  /**
   * Deletes an {@link Item}.
   *
   * @param id the item id.
   * @param version the item version to compare against the previously stored item update version
   * @param requestMode mode for delete request
   * @return {@link ListenableFuture} that the caller uses to obtain the result of a delete
   *     operation (using {@link ListenableFuture#get()}).
   * @throws IOException when service throws an exception.
   */
  ListenableFuture<Operation> deleteItem(String id, byte[] version, RequestMode requestMode)
      throws IOException;

  /**
   * Deletes items from a queue.
   *
   * @param name the queue name
   * @return {@link ListenableFuture} that the caller uses to obtain the result of a
   *     delete queue items operation (using {@link ListenableFuture#get()}).
   * @throws IOException when the service throws an exception
   */
  ListenableFuture<Operation> deleteQueueItems(String name) throws IOException;

  /**
   * Gets an {@link Item}.
   *
   * @param id the item id
   * @return the item or {@code null} if not found
   * @throws IOException when service throws exception
   */
  Item getItem(String id) throws IOException;

  /**
   * Fetches the first of what may be many sets of {@link Item}.
   *
   * @param brief {@code true} to shorten {@link Item} metadata, default: {@code true}
   * @return an iterator for the returned set of {@link Item}
   * @throws IOException when service throws exception
   */
  Iterable<Item> listItem(boolean brief) throws IOException;

  /**
   * Sends an {@link Item} for indexing.
   *
   * @param item the item
   * @param requestMode {@link RequestMode} for {@link Item} index request
   * @return {@link ListenableFuture} that the caller uses to obtain the result of an update
   *     operation (using {@link ListenableFuture#get()})
   * @throws IOException when service throws exception
   */
  ListenableFuture<Operation> indexItem(Item item, RequestMode requestMode) throws IOException;

  /**
   * Sends an {@link Item} and associated content for indexing.
   *
   * @param item the item
   * @param content the item's content
   * @param contentHash the hash of the item's content
   * @param requestMode {@link RequestMode} for {@link Item} index request
   * @return {@link ListenableFuture} that the caller uses to obtain the result of an update
   *     operation (using {@link ListenableFuture#get()})
   * @throws IOException when service throws exception
   */
  ListenableFuture<Operation> indexItemAndContent(
      Item item,
      AbstractInputStreamContent content,
      @Nullable String contentHash,
      ContentFormat contentFormat,
      RequestMode requestMode)
      throws IOException;

  /**
   * Fetches {@link Item} entries from the queue using custom API parameters.
   *
   * @param pollQueueRequest the user created and populated poll request
   * @return items from the queue
   * @throws IOException when service throws exception
   */
  List<Item> poll(PollItemsRequest pollQueueRequest) throws IOException;

  /**
   * Fetches all of the {@link Item} entries repeatedly from the queue until the entire queue is
   * exhausted.
   *
   * @param pollQueueRequest the user created and populated poll request
   * @return an iterator for items returned from the queue
   * @throws IOException when service throws exception
   */
  Iterable<Item> pollAll(PollItemsRequest pollQueueRequest) throws IOException;

  /**
   * Pushes a {@link PushItem} object to indexing API Queue.
   *
   * @param id the item id
   * @param pushItem the item to push
   * @return {@link ListenableFuture} that the caller uses to obtain the result of a push operation
   *     (using {@link ListenableFuture#get()})
   * @throws IOException when service throws exception
   */
  ListenableFuture<Item> push(String id, PushItem pushItem) throws IOException;

  /**
   * Unreserves previously polled {@link Item} entries in a specific queue.
   *
   * <p>When a connector issues a {@link #poll(PollItemsRequest)} of the indexing queue, the
   * returned {@link Item} entries are marked internally as <em>reserved</em> so that they are
   * unavailable for a future {@link #poll(PollItemsRequest)} request. This prevents two possibly
   * different threads from processing the same {@link Item}. This method allows the connector to
   * reset the queue to make all of its entries available again.
   *
   * @param queue the queue to unreserve, ({@code null} for default queue)
   * @return {@link ListenableFuture} that the caller uses to obtain the result of an unreserve
   *     operation (using {@link ListenableFuture#get()})
   * @throws IOException when service throws exception
   */
  ListenableFuture<Operation> unreserve(String queue) throws IOException;

  /**
   * Creates {@link UploadItemRef} for uploading media content.
   *
   * @param itemId for which upload reference to be created.
   * @return {@link UploadItemRef} for uploading media content
   * @throws IOException when service throws exception
   */
  UploadItemRef startUpload(String itemId) throws IOException;

  /**
   * Gets the {@link Schema} defined within the connected data source.
   *
   * <p>Each data source may have at most one schema defined within it. This method extracts the
   * schema definition to use with the current connector's data repository.
   *
   * @return {@link Schema} defined within the connected data source
   * @throws IOException when service throws exception
   */
  Schema getSchema() throws IOException;

  /**
   * Returns the {@link Operation} with the given name.
   *
   * @param name the operation name
   * @return the Operation object describing the current state of the long-running operation
   * @throws IOException when service throws exception
   */
  Operation getOperation(String name) throws IOException;
}
