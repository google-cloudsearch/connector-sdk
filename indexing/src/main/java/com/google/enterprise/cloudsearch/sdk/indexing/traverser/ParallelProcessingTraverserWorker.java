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
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Implementation of the multiprocess Traverser. This implementation uses CachedThreadpool, which
 * can use maximum of <i>hostload</i> threads to process periodically polled items.
 */
class ParallelProcessingTraverserWorker extends AbstractTraverserWorker implements TraverserWorker {
  private final Logger logger = Logger.getLogger(ParallelProcessingTraverserWorker.class.getName());

  private static final int MIN_QUEUE_LOAD_TO_SKIP = 1000;

  private final ItemRetriever itemRetriever;
  private final int hostload;

  private final boolean sharedExecutor;
  private final ExecutorService executor;
  private final AtomicInteger currentLoad = new AtomicInteger(0);

  private final AtomicBoolean isShutdown = new AtomicBoolean(false);
  private final ConcurrentLinkedQueue<Item> queue;

  public ParallelProcessingTraverserWorker(
      TraverserConfiguration conf,
      IndexingService indexingService,
      @Nullable ExecutorService executor) {
    super(conf, indexingService);
    this.itemRetriever = conf.getItemRetriever();
    this.hostload = conf.getHostload();
    this.sharedExecutor = executor != null;
    this.executor = sharedExecutor ? executor : Executors.newCachedThreadPool();
    queue = new ConcurrentLinkedQueue<>();
  }

  @Override
  public void poll() {
    while (!isShutdown.get()) {
      if (queue.size() >= MIN_QUEUE_LOAD_TO_SKIP) {
        logger.info("skipping poll request since queue is full");
        break;
      }
      logger.info("polling entries with request " + pollRequest);
      if (!pollNextBatch()) {
        logger.info("Empty poll response. Try after sometime");
        break;
      }
    }
  }

  private boolean pollNextBatch() {
    List<Item> entries;
    try {
      entries = timeLimiter.callWithTimeout(new PollingFunction(), timeout, timeunit);
    } catch (TimeoutException | ExecutionException e) {
      logger.log(
          Level.WARNING,
          String.format("[%s] Error executing poll request %s", name, pollRequest),
          e);
      return false;
    } catch (InterruptedException e) {
      logger.log(
          Level.WARNING,
          String.format("[%s] Interrupted executing poll request %s", name, pollRequest),
          e);
      Thread.currentThread().interrupt();
      return false;
    }
    if (entries.isEmpty()) {
      return false;
    }
    queue.addAll(entries);

    while (currentLoad.get() < hostload) {
      executor.execute(new PollAndProcessRunnable());
      currentLoad.incrementAndGet();
    }
    return true;
  }

  private final class PollingFunction implements Callable<List<Item>> {
    @Override
    public List<Item> call() throws Exception {
      return indexingService.poll(pollRequest);
    }
  }

  @Override
  void shutdownWorker() {
    isShutdown.set(true);
    if (sharedExecutor) {
      if ((executor == null) || executor.isShutdown()) {
        return;
      }
      executor.shutdown();
      try {
        executor.awaitTermination(10L, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        logger.log(Level.WARNING, "Interrupted during executor termination.", e);
        Thread.currentThread().interrupt();
      }
      executor.shutdownNow();
    } else {
      logger.log(Level.FINE, "skip");
    }
  }

  class PollAndProcessRunnable implements Runnable {

    @Override
    public void run() {
      try {
        Thread.currentThread()
            .setName("TraverserRunner-" + name + "-" + Thread.currentThread().getId());
        Item polledItem = null;
        while ((polledItem = queue.poll()) != null) {
          try {
            timeLimiter.callWithTimeout(new ProcessingFunction(polledItem), timeout, timeunit);
          } catch (InterruptedException e) {
            logger.log(
                Level.WARNING,
                String.format("Interrupted while processing queue entry %s", polledItem),
                e);
            Thread.currentThread().interrupt();
          } catch (TimeoutException e) {
            logger.log(
                Level.WARNING,
                String.format(
                    "Processing queue entry %s timed out, limit %d %s",
                    polledItem, timeout, timeunit),
                e);
          } catch (Throwable t) {
            logger.log(
                Level.WARNING,
                String.format("Exception while processing queue entry %s", polledItem),
                t);
          }
        }
      } finally {
        currentLoad.decrementAndGet();
      }
    }

    private final class ProcessingFunction implements Callable<Void> {
      private final Item queueItem;

      private ProcessingFunction(Item queueItem) {
        this.queueItem = queueItem;
      }

      @Override
      public Void call() throws Exception {
        PushItem errorEntry = null;
        try {
          logger.log(Level.INFO, "processing queue item {0}", queueItem);
          itemRetriever.process(queueItem);
        } catch (IOException io) {
          // TODO(tvartak) : Allow connector to throw RepositoryException and
          // catch RepositoryException here instead of {@link IOException}.
          logger.log(Level.WARNING, "Error processing queue item " + queueItem, io);
          Optional<RepositoryError> error = getRepositoryError(io);
          if (error.isPresent()) {
            errorEntry =
                new PushItem()
                    .setQueue(queueItem.getQueue())
                    .setType("REPOSITORY_ERROR")
                    .setRepositoryError(error.get())
                    .encodePayload(queueItem.decodePayload());
          }
        }

        if (errorEntry != null) {
          logger.log(Level.INFO, "Pushing repository errors for item {0}", queueItem.getName());
          try {
            indexingService.push(queueItem.getName(), errorEntry);
          } catch (IOException e) {
            logger.log(Level.WARNING, "Error pushing item", e);
          }
        }
        return null;
      }
    }
  }

  private static Optional<RepositoryError> getRepositoryError(IOException exception) {
    boolean isRepositoryException = (exception instanceof RepositoryException);
    boolean isCauseRepositoryException =
        !isRepositoryException && (exception.getCause() instanceof RepositoryException);
    if (!isRepositoryException && !isCauseRepositoryException) {
      return Optional.empty();
    }
    if (isRepositoryException) {
      return Optional.of(((RepositoryException) exception).getRepositoryError());
    }
    if (isCauseRepositoryException) {
      return Optional.of(((RepositoryException) (exception.getCause())).getRepositoryError());
    }
    return Optional.empty();
  }
}
