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
package com.google.enterprise.cloudsearch.sdk.indexing.traverser;

import com.google.api.services.cloudsearch.v1.model.Item;
import java.util.List;

/**
 * An interface that performs the method call to poll the Cloud Search queue.
 *
 * <p>The Cloud Search SDK uses this interface to create polling threads whenever the
 * {@link com.google.enterprise.cloudsearch.sdk.Connector} implements the
 * {@link com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever} or
 * {@link com.google.enterprise.cloudsearch.sdk.indexing.BatchItemRetriever} interfaces.
 */
public interface TraverserWorker {

  /**
   * Returns the name of the worker thread for use in logging.
   *
   * @return name of worker thread
   */
  public String getName();

  /**
   * Performs the Cloud Search queue poll.
   *
   * <p>This method calls the
   * {@link com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever#process(Item)} or
   * {@link com.google.enterprise.cloudsearch.sdk.indexing.BatchItemRetriever#processBatch(List)}
   * method to deliver the queue item to the {@link com.google.enterprise.cloudsearch.sdk.Connector}
   * instance.
   */
  public void poll();

  /**
   * Shuts down this worker thread.
   */
  public void shutdown();
}
