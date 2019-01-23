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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.util.concurrent.ExecutorService;
import javax.annotation.Nullable;

/**
 * Factory object for creating {@link TraverserWorker} instances.
 */
public class TraverserWorkerManager {

  private TraverserWorkerManager() {}

  /**
   * Creates a {@link TraverserWorker} instance based on the {@link TraverserConfiguration}.
   *
   * <p>Each instance creates its own {@link ExecutorService}.
   *
   * @param conf configuration of the worker thread
   * @param indexingService {@link IndexingService} instance used for polling items
   * @return {@link TraverserWorker} instance
   */
  public static TraverserWorker newWorker(TraverserConfiguration conf,
      IndexingService indexingService) {
    return TraverserWorkerManager.newWorker(conf, indexingService, null);
  }

  /**
   * Creates a {@link TraverserWorker} instance based on the {@link TraverserConfiguration}.
   *
   * <p>An external, shared {@link ExecutorService} can provide better resource utilization.
   * However, if you use one, the {@link TraverserWorker} does not handle the executor's shutdown
   * method.
   *
   * @param conf configuration of the worker thread
   * @param indexingService {@link IndexingService} instance used for polling items
   * @param executor the {@link ExecutorService} that is used by the worker thread but if
   * <code>null</code>, the {@link TraverserWorker} creates its own {@link ExecutorService}
   *
   * @return {@link TraverserWorker} instance
   */
  public static TraverserWorker newWorker(TraverserConfiguration conf,
        IndexingService indexingService,  @Nullable ExecutorService executor) {
    checkNotNull(conf, "configuration should be defined");
    checkNotNull(indexingService, "indexingService should be defined");

    if (conf.getBatchItemRetriever() != null) {
      return new BatchProcessingTraverserWorker(conf, indexingService);
    } else {
      checkArgument(conf.getHostload() > 0, "hostload should be greater than 0");
      return new ParallelProcessingTraverserWorker(conf, indexingService, executor);
    }
  }
}
