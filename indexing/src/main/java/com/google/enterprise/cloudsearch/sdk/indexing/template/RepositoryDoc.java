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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.json.GenericJson;
import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.*;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Generic object for a single document in a repository.
 *
 * <p>This is a type of {@link ApiOperation} that performs an {@link IndexingService#indexItem(Item,
 * RequestMode)} request. This single request can actually contain multiple requests if the data
 * repository is hierarchical.
 *
 * <p>Sample usage:
 *
 * <pre>{@code
 * Item item = ... // create an Item object
 * ByteArrayContent content = ... // create item content, using HTML content for this example
 * String contentHash = ... // optional content hash value
 * RepositoryDoc.Builder builder = new RepositoryDoc.Builder()
 *     .setItem(item)
 *     .setContent(content, contentHash, ContentFormat.HTML)
 *     .setRequestMode(RequestMode.SYNCHRONOUS);
 * // if hierarchical, add children of this document
 * List<String> childIds = ... // retrieve all child IDs
 * for (childId : childIds) {
 *   PushItem pushItem = ... // populate a push item for this ID
 *   builder.addChildId(childId, pushItem);
 * }
 * RepositoryDoc document = builder.build();
 * // now the document is ready for use, typically as a return value from a Repository method
 * }</pre>
 */
public class RepositoryDoc implements ApiOperation {
  private final Logger logger = Logger.getLogger(RepositoryDoc.class.getName());

  private final Item item;
  private final AbstractInputStreamContent content;
  private final ContentFormat contentFormat;
  private final String contentHash;
  private final Map<String, PushItem> childIds;
  private final Map<String, Acl> fragments;
  private final RequestMode requestMode;
  private final FutureCallback<GenericJson> callback;

  private RepositoryDoc(Builder builder) {
    this.item = builder.item;
    this.content = builder.content;
    this.contentHash = builder.contentHash;
    this.contentFormat = builder.contentFormat;
    this.childIds = builder.childIds;
    this.fragments = builder.fragments;
    this.requestMode = builder.requestMode;
    this.callback = builder.callback;
  }

  public static class Builder {
    Item item;
    AbstractInputStreamContent content;
    ContentFormat contentFormat = ContentFormat.RAW;
    String contentHash;
    Map<String, PushItem> childIds = new HashMap<>();
    Map<String, Acl> fragments;
    RequestMode requestMode = RequestMode.UNSPECIFIED;
    FutureCallback<GenericJson> callback;

    /** Creates an instance of {@link RepositoryDoc.Builder} */
    public Builder() {}

    /**
     * Sets item to be indexed.
     *
     * @param item to be indexed.
     */
    public Builder setItem(Item item) {
      this.item = item;
      return this;
    }

    /**
     * Sets the content and content format.
     *
     * <p>The {@code content} parameter should use a concrete implementation of {@code
     * AbstractInputStreamContent} based on the natural source object:
     *
     * <ul>
     *   <li>For {@code InputStream}, use {@code InputStreamContent}. For best results, if the
     *       length of the content (in bytes) is known without reading the stream, call {@code
     *       setLength} on the {@code InputStreamContent}.
     *   <li>For {@code String} or {@code byte[]}, use {@code ByteArrayContent}.
     *   <li>For existing files, use {@code FileContent}.
     * </ul>
     *
     * <p>Use this method when the content hash is not being used.
     */
    public Builder setContent(AbstractInputStreamContent content, ContentFormat contentFormat) {
      return setContent(content, null, contentFormat);
    }

    /**
     * Sets the content, content hash, and content format.
     *
     * <p>The {@code content} parameter should use a concrete implementation of {@code
     * AbstractInputStreamContent} based on the natural source object:
     *
     * <ul>
     *   <li>For {@code InputStream}, use {@code InputStreamContent}. For best results, if the
     *       length of the content (in bytes) is known without reading the stream, call {@code
     *       setLength} on the {@code InputStreamContent}.
     *   <li>For {@code String} or {@code byte[]}, use {@code ByteArrayContent}.
     *   <li>For existing files, use {@code FileContent}.
     * </ul>
     *
     * <p>Use this method when the content hash is being used. The content hash allows the Cloud
     * Search queue to determine whether a document's content has been modified during a subsequent
     * push of the document. This allows the document's queue status to automatically change to a
     * modified state.
     */
    public Builder setContent(
        AbstractInputStreamContent content,
        @Nullable String contentHash,
        ContentFormat contentFormat) {
      this.content = content;
      this.contentHash = contentHash;
      this.contentFormat = contentFormat;
      return this;
    }

    /**
     * Adds a child item to be pushed
     *
     * @param id for item to be pushed
     * @param item {@link PushItem} to be pushed
     */
    public Builder addChildId(String id, PushItem item) {
      checkArgument(!Strings.isNullOrEmpty(id), "id can not be null");
      this.childIds.put(id, checkNotNull(item, "item can not be null"));
      return this;
    }

    /**
     * Sets {@link Acl} fragments to be created.
     *
     * @param fragments to be created.
     */
    public Builder setAclFragments(Map<String, Acl> fragments) {
      this.fragments = new HashMap<>(fragments);
      return this;
    }

    /**
     * Sets {@link RequestMode} to be used for indexing requests.
     *
     * @param requestMode to be used for indexing requests.
     */
    public Builder setRequestMode(RequestMode requestMode) {
      this.requestMode = requestMode;
      return this;
    }

    /**
     * Sets {@link FutureCallback<GenericJson>} to be executed after RepositoryDoc execution is finished.
     *
     * @param callback to be executed after RepositoryDoc execution is finished.
     */
    public Builder setCallback(FutureCallback<GenericJson> callback) {
      this.callback = callback;
      return this;
    }

    /** Builds an instance of {@link RepositoryDoc} */
    public RepositoryDoc build() {
      checkNotNull(item);
      childIds = childIds != null ? Collections.unmodifiableMap(childIds) : Collections.emptyMap();
      fragments =
          fragments != null ? Collections.unmodifiableMap(fragments) : Collections.emptyMap();
      return new RepositoryDoc(this);
    }
  }

  /**
   * Gets the repository document converted into an {@link Item}.
   *
   * @return a unique document ID
   */
  public Item getItem() {
    return this.item;
  }

  /**
   * Gets the content of this repository document.
   *
   * @return document content
   */
  public AbstractInputStreamContent getContent() {
    return content;
  }

  /**
   * Gets the hash value of this repository document's content.
   *
   * @return document's content hash value
   */
  public String getContentHash() {
    return contentHash;
  }

  /**
   * Gets {@link ContentFormat} for indexable content.
   *
   * @return {@link ContentFormat} for indexable content.
   */
  public ContentFormat getContentFormat() {
    return contentFormat;
  }

  /**
   * Gets child items to be pushed.
   *
   * @return child items to be pushed.
   */
  public Map<String, PushItem> getChildIds() {
    return ImmutableMap.copyOf(Maps.transformValues(childIds, PushItem::clone));
  }

  /**
   * Gets additional {@link Acl} fragments to be created.
   *
   * @return additional {@link Acl} fragments to be created.
   */
  public Map<String, Acl> getFragments() {
    return ImmutableMap.copyOf(fragments);
  }

  /**
   * Gets {@link RequestMode} to be used for executing indexing requests.
   *
   * @return {@link RequestMode} to be used for executing indexing requests.
   */
  public RequestMode getRequestMode() {
    return requestMode;
  }

  /**
   * Gets {@link FutureCallback<GenericJson>} to be executed after RepositoryDoc execution is finished.
   *
   * @return {@link FutureCallback<GenericJson>} to be executed after RepositoryDoc execution is finished.
   */
  public FutureCallback<GenericJson> getCallback() {
    return callback;
  }

  /**
   * Performs the indexing service request to index the document.
   *
   * <p>In addition to updating the {@link Item}, the children (if any) are pushed and ACL fragments
   * (possibly representing a directory in a hierarchical data repository) are created and updated.
   *
   * @param service the indexing service used to execute requests
   * @return results of the requests
   * @throws IOException on SDK upload errors
   * @throws InterruptedException on program interruption
   */
  @Override
  public List<GenericJson> execute(IndexingService service)
      throws IOException, InterruptedException {
    List<ListenableFuture<? extends GenericJson>> futures = new ArrayList<>();
    // Extract original item name here. service.indexItem() call below mutates item.name value by
    // adding data source prefix as well as escaping unsupported chars. Use original item name to
    // create fragments below to ensure consistent encoding is applied to fragments as well.
    String originalItemName = item.getName();
    ListenableFuture<Operation> operation =
        content == null
            ? service.indexItem(item, requestMode)
            : service.indexItemAndContent(item, content, contentHash, contentFormat, requestMode);

    ListenableFuture<GenericJson> future = Futures.catchingAsync(
            operation,
            IOException.class,
            /**
             * Push failed index request into the queue if a backend error is returned, a failed
             * future is always returned so that this operation is not treated as if it succeeded.
             */
            (AsyncFunction<IOException, Item>)
                    ex -> {
                      logger.log(Level.WARNING, "Error indexing the item " + item, ex);
                      Optional<RepositoryError> error = getRepositoryErrorForResponseException(ex);
                      if (!error.isPresent()) {
                        return Futures.immediateFailedFuture(ex);
                      }
                      logger.log(Level.INFO, "Pushing this failed item to queue " + item);
                      return Futures.transformAsync(
                              service.push(
                                      item.getName(),
                                      new PushItem()
                                              .setQueue(item.getQueue())
                                              .setType("REPOSITORY_ERROR")
                                              .setRepositoryError(error.get())
                                              .encodePayload(item.decodePayload())),
                              input -> Futures.<Item>immediateFailedFuture(ex),
                              MoreExecutors.directExecutor());
                    },
            MoreExecutors.directExecutor());

    if(callback != null) {
      Futures.addCallback(future, callback, MoreExecutors.directExecutor());
    }

    futures.add(future);


    for (Map.Entry<String, PushItem> entry : childIds.entrySet()) {
      futures.add(service.push(entry.getKey(), entry.getValue()));
    }

    if (!fragments.isEmpty()) {
      for (Map.Entry<String, Acl> fragment : fragments.entrySet()) {
        Acl fragmentAcl = fragment.getValue();
        Item fragmentItem = fragmentAcl.createFragmentItemOf(originalItemName, fragment.getKey());
        futures.add(service.indexItem(fragmentItem, requestMode));
      }
    }

    try {
      return Futures.allAsList(futures).get();
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(item, content, contentHash, childIds, fragments);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof RepositoryDoc)) {
      return false;
    }
    RepositoryDoc otherDoc = (RepositoryDoc) other;
    return Objects.equals(item, otherDoc.item)
        && Objects.equals(requestMode, otherDoc.requestMode)
        && Objects.equals(content, otherDoc.content)
        && Objects.equals(contentHash, otherDoc.contentHash)
        && Objects.equals(contentFormat, otherDoc.contentFormat)
        && Objects.equals(childIds, otherDoc.childIds)
        && Objects.equals(fragments, otherDoc.fragments);
  }

  @Override
  public String toString() {
    return "RepositoryDoc [item="
        + item
        + ", content="
        + content
        + ", contentFormat="
        + contentFormat
        + ", contentHash="
        + contentHash
        + ", childIds="
        + childIds
        + ", fragments="
        + fragments
        + ", requestMode="
        + requestMode
        + "]";
  }

  private static Optional<RepositoryError> getRepositoryErrorForResponseException(
      IOException exception) {
    if (!(exception instanceof GoogleJsonResponseException)) {
      return Optional.empty();
    }
    GoogleJsonResponseException responseException = (GoogleJsonResponseException) exception;

    if (responseException.getStatusCode() == HTTP_NOT_FOUND
        || responseException.getStatusCode() == HTTP_BAD_REQUEST) {
      return Optional.empty();
    }
    return Optional.of(
        new RepositoryError()
            .setErrorMessage(responseException.getMessage())
            .setType("SERVER_ERROR")
            .setHttpStatusCode(responseException.getStatusCode()));
  }
}
