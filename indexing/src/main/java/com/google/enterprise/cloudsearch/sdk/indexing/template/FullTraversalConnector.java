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
import static com.google.common.base.Preconditions.checkState;

import com.google.api.client.json.GenericJson;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.IncrementalChangeHandler;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingConnector;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingConnectorContext;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Template connector that performs a full repository traversal and uploads every document.
 *
 * <p>Use this connector type for relatively static or small data sets that the connector
 * can upload quickly. This connector uploads every document without pushing documents to
 * the Cloud Search queue. Use the {@link ListingConnector} for a list or graph traversal
 * strategy.
 *
 * <p>Sample usage:
 *
 * <pre>{@code
 * public static void main(String[] args) throws IOException, InterruptedException {
 *   Application application = new Application.Builder(
 *       new FullTraversalConnector(new MyRepository()), args).build();
 *   application.start();
 * }
 * }</pre>
 *
 * <p>If the repository supports document change detection, the connector can perform an incremental
 * traversal, which reads and re-indexes just the newly modified documents. Synchronization enables
 * an incremental traversal ({@link #handleIncrementalChanges()}) to run in parallel with a full
 * traversal ({@link #traverse()}), allowing the shorter incremental traversal to complete without
 * waiting for the longer full traversal to do so. When the two traversal types run simultaneously,
 * the full traversal holds off its start of execution until the currently running incremental
 * traversal has completed.
 *
 * <p>For example, if a full traversal on a large repository might take days to complete, then
 * configure several smaller incremental traversal to run every few hours or so. Each {@link Item}
 * gets a version timestamp to prevent a late update from the full traversal from overwriting a more
 * recent incremental update.
 *
 * <p>Optional configuration parameters:
 *
 * <ul>
 *   <li>{@value #NUM_THREADS} - Specifies the number of threads the connector is going to create to
 *       allow for parallel processing. A single iterator fetches operations serially (typically
 *       {@link RepositoryDoc} objects), but the API calls process in parallel using this number of
 *       threads.
 * </ul>
 */
public class FullTraversalConnector implements IndexingConnector, IncrementalChangeHandler {

  // define all relevant configuration parameters (public for Javadocs)
  /** Configuration key for setting number of worker threads for traversal */
  public static final String NUM_THREADS = "traverse.threadPoolSize";
  /** Configuration key to indicate if connector uses queue toggle logic for delete detection. */
  public static final String TRAVERSE_USE_QUEUES = "traverse.useQueues";
  /**
   * Configuration key to define number of {@link ApiOperation}s to be processed in batches before
   * fetching additional {@link ApiOperation}s.
   */
  public static final String TRAVERSE_PARTITION_SIZE = "traverse.partitionSize";

  /** Configuration key to define queue name prefix used by connector. */
  public static final String TRAVERSE_QUEUE_TAG = "traverse.queueTag";

  /** Default full traversal checkpoint name. */
  public static final String CHECKPOINT_FULL = "checkpoint_full";

  /** Default incremental traversal checkpoint name. */
  public static final String CHECKPOINT_INCREMENTAL = "checkpoint_incremental";

  /** Default queue checkpoint name. */
  public static final String CHECKPOINT_QUEUE = "checkpoint_queue";

  /** Default queue name prefix used by connector. */
  public static final String QUEUE_NAME = "FullTraversal||";

  static final int DEFAULT_THREAD_NUM = 50;
  static final int DEFAULT_PARTITION_SIZE = 50;
  static final String IGNORE_FAILURE = "ignore";
  private static final boolean DEFAULT_USE_QUEUES = true;
  private static final Logger logger = Logger.getLogger(FullTraversalConnector.class.getName());
  private final Repository repository;
  private DefaultAcl defaultAcl;
  private IndexingService indexingService;
  private Integer numThreads;
  private Long numToAbort;
  private ThreadPoolExecutor threadPoolExecutor;
  private ListeningExecutorService listeningExecutorService;
  private RepositoryContext repositoryContext;
  private CheckpointHandler checkpointHandler;
  private boolean useQueues;
  @VisibleForTesting QueueCheckpoint queueCheckpoint;
  private int partitionSize;

  /**
   * Creates an instance of {@link FullTraversalConnector} for performing full traversal over given
   * {@link Repository}
   *
   * @param repository implementation to fetch indexable items from
   */
  public FullTraversalConnector(Repository repository) {
    this(repository, null);
  }

  /**
   * Creates an instance of {@link FullTraversalConnector} for performing full traversal over given
   * {@link Repository} with ability to manage traversal checkpoints using supplied instance of
   * {@link CheckpointHandler}
   *
   * @param repository implementation to fetch indexable items from
   * @param checkpointHandler to manage traversal checkpoints
   */
  public FullTraversalConnector(Repository repository, CheckpointHandler checkpointHandler) {
    this.repository = checkNotNull(repository, "Repository cannot be null.");
    this.checkpointHandler = checkpointHandler;
  }

  /** Use the repository class name for the default ID, rather than this template class name. */
  @Override
  public String getDefaultId() {
    return repository.getClass().getName();
  }

  /**
   * Creates all objects needed for a traversal.
   *
   * @param context the context used to get the configuration
   * @throws Exception if configuration parameters are invalid
   */
  @Override
  public void init(IndexingConnectorContext context) throws Exception {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    indexingService = checkNotNull(context.getIndexingService());
    defaultAcl = DefaultAcl.fromConfiguration(indexingService);
    repositoryContext =
        new RepositoryContext.Builder()
            .setEventBus(new EventBus("EventBus-" + getClass().getName()))
            .setDefaultAclMode(defaultAcl.getDefaultAclMode())
            .build();
    if (checkpointHandler == null) {
      checkpointHandler = LocalFileCheckpointHandler.fromConfiguration();
    }

    useQueues = Configuration.getBoolean(TRAVERSE_USE_QUEUES, DEFAULT_USE_QUEUES).get();
    QueueCheckpoint.Builder builder = new QueueCheckpoint.Builder(useQueues);
    if (useQueues) {
      String tag =
          Configuration.getString(TRAVERSE_QUEUE_TAG, repository.getClass().getSimpleName()).get();
      builder.setCheckpointHandler(checkpointHandler)
          .setQueueA(QUEUE_NAME + "A: " + tag)
          .setQueueB(QUEUE_NAME + "B: " + tag);
    }
    queueCheckpoint = builder.build();

    numThreads = Configuration.getInteger(NUM_THREADS, DEFAULT_THREAD_NUM).get();
    // TODO(bmj): Fix this gross violation of encapsulation.
    numToAbort = Configuration
        .getValue(TraverseExceptionHandlerFactory.TRAVERSE_EXCEPTION_HANDLER, 0L, value -> {
          if (IGNORE_FAILURE.equals(value)) {
            return Long.MAX_VALUE;
          } else {
            try {
              return Long.parseLong(value);
            } catch (NumberFormatException e) {
              throw new InvalidConfigurationException(
                  "Unrecognized value for traversal exception handler: " + value, e);
            }
          }
        }).get();

    threadPoolExecutor =
        new ThreadPoolExecutor(
            numThreads,
            numThreads,
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(10 * numThreads),
            new ThreadPoolExecutor.CallerRunsPolicy());
    listeningExecutorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    partitionSize = Configuration.getInteger(TRAVERSE_PARTITION_SIZE, DEFAULT_PARTITION_SIZE).get();
    Configuration.checkConfiguration(
        partitionSize > 0,
        "Partition size can not be less than or equal to 0. Configured value %s",
        partitionSize);
    repositoryContext.getEventBus().register(this);
    repository.init(repositoryContext);
    logger.log(Level.INFO, "start full traversal connector executors");
  }

  /**
   * Performs a full repository traversal and uploads every document.
   *
   * <p>numToAbort determines what will happen when upload exceptions occur. Either ignore the
   * exceptions or force a traversal termination after a set number of exceptions occur.
   *
   * @throws IOException on SDK upload errors
   * @throws InterruptedException if exception handler is interrupted
   */
  @Override
  public void traverse() throws IOException, InterruptedException {
    String queueName = queueCheckpoint.getCurrentQueueName();
    if (useQueues) {
      String operationName = queueCheckpoint.getCurrentOperation();
      if (!Strings.isNullOrEmpty(operationName)) {
        try {
          Operation operation = indexingService.getOperation(operationName);
          if (!Boolean.TRUE.equals(operation.getDone())) {
            logger.log(Level.FINE, "Postponing full traversal; deleteQueueItems not finished "
                + "for {0}", operationName);
            return;
          }
          logger.log(Level.FINE, "deleteQueueItems result for {0}: {1}",
              new Object[] { operationName, getOperationResult(operation) });
        } catch (IOException e) {
          logger.log(Level.WARNING, "Error checking on status of deleteQueueItems "
              + operationName, e);
        }
        queueName = queueCheckpoint.getNextQueueName(queueName);
        queueCheckpoint.saveCheckpoint(queueName, "");
      }
    }

    if (!doTraverse("full traversal", CHECKPOINT_FULL, queueName, repository::getAllDocs)) {
      throw new NullPointerException("getAllDocs returned null");
    }

    if (useQueues) {
      String nextQueueName = queueCheckpoint.getNextQueueName(queueName);
      try {
        Operation operation = indexingService.deleteQueueItems(nextQueueName).get();
        if (Boolean.TRUE.equals(operation.getDone())) {
          logger.log(Level.FINE, "deleteQueueItems result for {0}: {1}",
              new Object[] { nextQueueName, getOperationResult(operation) });
          queueCheckpoint.saveCheckpoint(nextQueueName, "");
        } else {
          logger.log(Level.FINE, "deleteQueueItems running for {0}", operation.getName());
          queueCheckpoint.saveCheckpoint(queueName, operation.getName());
        }
      } catch (IOException | ExecutionException e) {
        logger.log(Level.WARNING, "Error calling deleteQueueItems", e);
      }
    }
  }

  private String getOperationResult(Operation operation) {
    if (operation.getError() != null) {
      return operation.getError().toString();
    } else if (operation.getResponse() != null) {
      return operation.getResponse().toString();
    }
    return "Done: " + operation.getDone();
  }

  @FunctionalInterface
  private interface GetDocsFunction {
    CheckpointCloseableIterable<ApiOperation> apply(byte[] checkpoint) throws RepositoryException;
  }

  /**
   * Performs a repository traversal of a given type.
   *
   * @throws IOException on SDK upload errors
   * @throws InterruptedException if exception handler is interrupted
   */
  private boolean doTraverse(String traversalType, String checkpointName, String queueName,
      GetDocsFunction getDocs)
      throws IOException, InterruptedException {
    logger.log(Level.INFO, "Begin {0} traversal.", traversalType);
    ExecuteCounter executeCounter = new ExecuteCounter();
    byte[] checkpoint = checkpointHandler.readCheckpoint(checkpointName);
    boolean hasMore = false;
    do {
      try (CheckpointCloseableIterable<ApiOperation> allDocs = getDocs.apply(checkpoint)) {
        if (allDocs == null) {
          logger.log(Level.INFO, "End {0} traversal.", traversalType);
          return false;
        }
        processApiOperations(allDocs, executeCounter, queueName);
        logCounters(executeCounter);
        checkpoint = allDocs.getCheckpoint();
        checkpointHandler.saveCheckpoint(checkpointName, checkpoint);
        hasMore = allDocs.hasMore();
      }
    } while (hasMore);
    logger.log(Level.INFO, "End {0} traversal.", traversalType);
    return true;
  }

  /**
   * Performs the asynchronously pushed operation from the {@link Repository}.
   *
   * <p>This is <em>only</em> used when the {@link Repository} supports asynchronous operations
   * outside of normal traversals. Use this operation for any asynchronously triggered action, such
   * as document deletion, modification, or creation.
   *
   * @param asyncOp the operation asynchronously pushed from the {@link Repository}
   */
  @Subscribe
  public synchronized void handleAsyncOperation(AsyncApiOperation asyncOp) {
    logger.log(Level.INFO, "Processing an asynchronous repository operation.");
    try {
      String queueName = queueCheckpoint.getCurrentQueueName();
      ExecuteCounter executeCounter = new ExecuteCounter();
      ListenableFuture<List<GenericJson>> future =
          listeningExecutorService.submit(
              new ExecuteOperationCallable(asyncOp.getOperation(), executeCounter, 0L, queueName));
      asyncOp.setResult(future);
    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          "Exception occured while processing an asynchronous repository operation: ", e);
      asyncOp.getResult().cancel(true);
    }
  }

  @Override
  public void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException {
    // N/A
  }

  /**
   * Performs any clean up code required of the {@link Repository}.
   */
  @Override
  public void destroy() {
    repository.close();
    repositoryContext.getEventBus().unregister(this);
    if (listeningExecutorService != null) {
      logger.log(Level.INFO, "Shutting down the full traversal connector executor");
      MoreExecutors.shutdownAndAwaitTermination(listeningExecutorService, 5L, TimeUnit.MINUTES);
    }
  }

  /**
   * Performs all actions necessary for incremental traversals.
   *
   * <p>If the {@link Repository} does not support incremental traversals, the
   * {@link Repository#getChanges(byte[])} method should return {@code null}.
   *
   * @throws IOException on SDK upload errors
   * @throws InterruptedException if exception handler is interrupted
   */
  @Override
  public synchronized void handleIncrementalChanges() throws IOException, InterruptedException {
    doTraverse("incremental traversal", CHECKPOINT_INCREMENTAL,
        queueCheckpoint.getCurrentQueueName(), repository::getChanges);
  }

  private void logCounters(ExecuteCounter executeCounter) {
    if (executeCounter.getFail() != 0) {
      logger.log(Level.WARNING, "{0} operations failed out of {1}",
          new Object[]{executeCounter.getFail(), executeCounter.getTotal()});
    } else if (executeCounter.getSuccess() > 0) {
      logger.log(Level.INFO, "{0} operations executed successfully.", executeCounter.getSuccess());
    } else {
      logger.log(Level.INFO, "No operations returned during traversal.");
    }
  }

  private void processApiOperations(
      Iterable<ApiOperation> allDocs, ExecuteCounter executeCounter, String queueName)
      throws IOException, InterruptedException {
    // Split list of operations into partitions to reduce memory usage
    for (List<ApiOperation> partition : Iterables.partition(allDocs, partitionSize)) {
      List<ListenableFuture<List<GenericJson>>> futures = new ArrayList<>();
      for (ApiOperation operation : partition) {
        // check if abort the traverse based on configuration
        if (executeCounter.getFail() > numToAbort) {
          break;
        }
        ListenableFuture<List<GenericJson>> future =
            listeningExecutorService.submit(
                new ExecuteOperationCallable(operation, executeCounter, numToAbort, queueName));
        futures.add(future);
      }
      ListenableFuture<List<List<GenericJson>>> updates = Futures.allAsList(futures);
      try {
        updates.get();
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
    }
  }

  /** A {@link Runnable} that executes one {@link ApiOperation}. */
  private class ExecuteOperationCallable implements Callable<List<GenericJson>> {

    private ApiOperation operation;
    private ExecuteCounter executeCounter;
    private long localNumToAbort;
    private String queueName;

    public ExecuteOperationCallable(ApiOperation operation,
        ExecuteCounter executeCounter, Long numToAbort, String queueName) {
      this.operation = operation;
      this.executeCounter = executeCounter;
      this.localNumToAbort = numToAbort;
      this.queueName = queueName;
    }

    @Override
    public List<GenericJson> call() throws Exception {
      return executeOperation(operation, executeCounter);
    }

    /**
     * Executes an {@link ApiOperation}.
     *
     * <p>This is common code used by all traversals.
     *
     * @param operation the {@link ApiOperation}, typically a {@link RepositoryDoc}
     * @throws InterruptedException if exception handler interrupted
     * @throws IOException on SDK problems
     */
    private List<GenericJson> executeOperation(
        ApiOperation operation, ExecuteCounter executeCounter)
            throws InterruptedException, IOException {
      // most should be update item ops, but allow other ops (delete item, etc.)
      executeCounter.incrementTotal();
      String displayId = "[not an item update]";
      if (operation instanceof RepositoryDoc) {
        displayId = ((RepositoryDoc) operation).getItem().getName();
      }
      try {
        List<GenericJson> res =
            operation.execute(indexingService, Optional.of(this::modifyApiOperation));
        executeCounter.incrementSuccess();
        return res;
      } catch (IOException e) {
        executeCounter.incrementFail();
        if (executeCounter.getFail() <= localNumToAbort) {
          logger.log(
              Level.WARNING,
              "Error updating item: {0} (abort skip count: {1}).",
              new Object[]{displayId, executeCounter.getFail()});
          logger.log(Level.WARNING, "Error executing API Operation", e);
          return Collections.emptyList();
        } else {
          logger.log(Level.WARNING, "Error updating item: {0} (aborting).", displayId);
          throw new IOException(e);
        }
      }
    }

    private void modifyApiOperation(ApiOperation apiOperation) {
      if (apiOperation instanceof RepositoryDoc) {
        RepositoryDoc doc = ((RepositoryDoc) operation);
        setDefaultAcls(doc);
        if (useQueues) {
          Item item = doc.getItem();
          if (item.getQueue() != null) {
            String existingQueue = item.getQueue();
            if (!(queueCheckpoint.isManagedQueueName(existingQueue))) {
              logger.log(Level.WARNING, "Overriding existing queue name ({1}) for item: {0}",
                  new Object[] { item.getName(), existingQueue });
            }
          }
          item.setQueue(queueName);
        }
      }
    }
  }

  static class ExecuteCounter {
    private AtomicLong total;
    private AtomicLong success;
    private AtomicLong fail;

    ExecuteCounter() {
      total = new AtomicLong();
      success = new AtomicLong();
      fail = new AtomicLong();
    }

    void incrementTotal() {
      total.getAndIncrement();
    }

    void incrementSuccess() {
      success.getAndIncrement();
    }

    void incrementFail() {
      fail.getAndIncrement();
    }

    long getTotal() {
      return total.get();
    }

    long getSuccess() {
      return success.get();
    }

    long getFail() {
      return fail.get();
    }
  }

  private void setDefaultAcls(RepositoryDoc doc) {
    ItemMetadata metadata = doc.getItem().getMetadata();
    if (metadata == null) {
      metadata = new ItemMetadata();
      doc.getItem().setMetadata(metadata);
    }
    defaultAcl.applyToIfEnabled(doc.getItem());
  }
}
