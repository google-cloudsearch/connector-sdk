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

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.Connector;
import com.google.enterprise.cloudsearch.sdk.ConnectorContext;
import com.google.enterprise.cloudsearch.sdk.IncrementalChangeHandler;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import javax.annotation.Nullable;

/**
 * An object that provides the necessary methods to access a data repository.
 *
 * <p>Depending on the traversal strategy being used, only a subset of these methods need to be
 * implemented. The methods are typically called from a {@link Connector} object such as the
 * {@link ListingConnector} or {@link FullTraversalConnector}.
 */
public interface Repository {

  /**
   * Performs data repository set-up and initialization.
   *
   * <p>This is the first access call from {@link Connector#init(ConnectorContext)}. It indicates
   * that the {@link Configuration} is initialized and is ready for use.
   *
   * @param context the {@link RepositoryContext}
   * @throws RepositoryException when repository initialization fails
   */
  void init(RepositoryContext context) throws RepositoryException;

  /**
   * Fetches all the document ids from the data repository.
   *
   * <p>This method is typically used by a list or graph traversal connector such as the {@link
   * ListingConnector} to push document ids to the Cloud Search queue. These ids are then polled
   * individually for uploading.
   *
   * @param checkpoint encoded checkpoint bytes
   * @return {@link CheckpointCloseableIterable} object containing list of {@link ApiOperation} to
   *     execute with new traversal checkpoint value
   * @throws RepositoryException when fetching document ids from the data repository fails
   */
  CheckpointCloseableIterable<ApiOperation> getIds(@Nullable byte[] checkpoint)
      throws RepositoryException;

  /**
   * Fetches all changed documents since the last traversal.
   *
   * <p>This method is only called if the data repository supports document change detection and the
   * {@link Connector} implements the {@link IncrementalChangeHandler} interface.
   *
   * <p>The {@code checkpoint} is defined and maintained within the {@link Repository} for
   * determining and saving the state from the previous traversal. The Cloud Search SDK stores and
   * retrieves the checkpoint from its queue so the {@link Repository} doesn't have to manage its
   * state between traversals or connector invocations.
   *
   * @param checkpoint encoded checkpoint bytes
   * @return {@link CheckpointCloseableIterable} object containing list of {@link ApiOperation} to
   *     execute with new traversal checkpoint value
   * @throws RepositoryException when change detection fails
   */
  CheckpointCloseableIterable<ApiOperation> getChanges(@Nullable byte[] checkpoint)
      throws RepositoryException;

  /**
   * Fetches all the documents from the data repository.
   *
   * <p>This method typically returns a {@link RepositoryDoc} object for each document that exists
   * in the repository. However depending on the data repository's capabilities, there might be
   * delete document operations also returned.
   *
   * @param checkpoint encoded checkpoint bytes
   * @return a list of {@link ApiOperation} objects to execute
   * @throws RepositoryException when fetching documents from the data repository fails
   */
  CheckpointCloseableIterable<ApiOperation> getAllDocs(@Nullable byte[] checkpoint)
      throws RepositoryException;

  /**
   * Fetches a single document from the data repository.
   *
   * <p>This method typically returns a {@link RepositoryDoc} object corresponding to passed
   * {@link Item}. However, if the requested document is no longer in the data repository, then a
   * {@link DeleteItem} operation might be returned instead.
   *
   * @param item the {@link Item} to process
   * @return a {@link ApiOperation} corresponding to the current state of the requested {@link Item}
   * @throws RepositoryException when the processing of the {@link Item} fails
   */
  ApiOperation getDoc(Item item) throws RepositoryException;

  /**
   * Checks whether the document corresponding to {@link Item} exists in the data repository.
   *
   * @return {@code true} if the document exists in the data repository
   * @throws RepositoryException when processing the requested document fails
   */
  boolean exists(Item item) throws RepositoryException;

  /**
   * Closes the data repository and releases resources such as connections or executors.
   */
  void close();
}
