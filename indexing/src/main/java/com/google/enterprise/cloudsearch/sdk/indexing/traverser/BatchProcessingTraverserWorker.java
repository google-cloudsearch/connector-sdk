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
import com.google.enterprise.cloudsearch.sdk.indexing.BatchItemRetriever;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of the single-thread Traverser that use BatchItemRetriever
 */
class BatchProcessingTraverserWorker extends AbstractTraverserWorker implements TraverserWorker {
  private final Logger logger = Logger.getLogger(BatchProcessingTraverserWorker.class.getName());

  private final BatchItemRetriever batchItemRetriever;

  public BatchProcessingTraverserWorker(
      TraverserConfiguration conf, IndexingService indexingService) {
    super(conf, indexingService);
    this.batchItemRetriever = conf.getBatchItemRetriever();
  }

  @Override
  public void poll() {
    logger.info("polling entries from queue " + pollRequest.getQueue());
    try {
      timeLimiter.callWithTimeout(new ProcessingFunction(), timeout, timeunit);
    } catch (Exception e) {
      logger.log(Level.WARNING, "[" + name + "] Error executing poll request " + pollRequest, e);
    }
  }

  @Override
  void shutdownWorker() {}

  private final class ProcessingFunction implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      List<Item> polledItems = null;
      try {
        polledItems = indexingService.poll(pollRequest);
      } catch (IOException e) {
        logger.log(Level.WARNING, "[" + name + "] Error executing poll request " + pollRequest, e);
      }

      try {
        batchItemRetriever.processBatch(polledItems);
      } catch (IOException io) {
        // TODO(tvartak) : Allow connector to throw RepositoryException and
        // catch RepositoryException here instead of {@link IOException}.
        logger.log(Level.WARNING, "Error processing queue entries batch " + polledItems, io);
        //TODO(imysak): should we handle errors here ?
      } catch (InterruptedException e) {
        logger.log(Level.WARNING, "Interrupted. Aborted processing batch.", e);
        Thread.currentThread().interrupt();
      }
      return null;
    }
  }

}