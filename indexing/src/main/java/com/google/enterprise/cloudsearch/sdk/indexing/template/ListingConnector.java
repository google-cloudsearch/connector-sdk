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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.eventbus.AsyncEventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.IncrementalChangeHandler;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.ConfigValue;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingConnector;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingConnectorContext;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever;
import com.google.enterprise.cloudsearch.sdk.indexing.traverser.TraverserConfiguration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Template connector that performs complete repository list traversals.
 *
 * <p>Use this connector type for large, dynamic, and hierarchical data repositories. This connector
 * pushes document ids to the Cloud Search queue and then processes them individually for indexing.
 *
 * <p>Sample usage:
 *
 * <pre>{@code
 * public static void main(String[] args) throws IOException, InterruptedException {
 *   Application application = new Application.Builder(
 *       new ListingConnector(new MyRepository()), args).build();
 *   application.start();
 * }
 * }</pre>
 *
 * <p>If the repository supports document change detection, the connector can perform an incremental
 * change traversal, which reads and re-indexes just the newly modified documents.
 *
 * <ul>
 *   <li>{@value #CONFIG_TRAVERSER} - Specifies the traverser names. If not specified it is set to
 *       default {@link TraverserConfiguration}.
 * </ul>
 */
public class ListingConnector implements IndexingConnector, ItemRetriever,
    IncrementalChangeHandler {
  private static final Logger logger = Logger.getLogger(ListingConnector.class.getName());

  /** Configuration key to define different {@link TraverserConfiguration}s used by connector. */
  public static final String CONFIG_TRAVERSER = "repository.traversers";
  /** Default full traversal checkpoint name. */
  public static final String CHECKPOINT_FULL = "checkpoint_full";
  /** Default incremental traversal checkpoint name. */
  public static final String CHECKPOINT_INCREMENTAL = "checkpoint_incremental";

  static final Integer DEFAULT_THREAD_NUM = 50;

  private final Repository repository;
  private IndexingService indexingService;
  private ExceptionHandler exceptionHandler;
  private ThreadPoolExecutor threadPoolExecutor;
  private ListeningExecutorService listeningExecutorService;
  private RepositoryContext repositoryContext;
  private CheckpointHandler checkpointHandler;
  private ConfigValue<List<String>> traverserConfigKey = Configuration
      .getMultiValue(CONFIG_TRAVERSER,
          Arrays.asList("default"), Configuration.STRING_PARSER);
  private DefaultAcl defaultAcl;

  /**
   * Creates an instance of {@link ListingConnector} for performing listing traversal over given
   * {@link Repository}
   *
   * @param repository implementation to fetch indexable items from
   */
  public ListingConnector(Repository repository) {
    this(repository, null);
  }

  /**
   * Creates an instance of {@link ListingConnector} for performing listing traversal over given
   * {@link Repository} with ability to manage traversal checkpoints using supplied instance of
   * {@link CheckpointHandler}
   *
   * @param repository implementation to fetch indexable items from
   * @param checkpointHandler to manage traversal checkpoints
   */
  public ListingConnector(Repository repository, CheckpointHandler checkpointHandler) {
    this.repository = checkNotNull(repository, "repository can not be null");
    this.checkpointHandler = checkpointHandler;
  }

  /** Use the repository class name for the default ID, rather than this template class name. */
  @Override
  public String getDefaultId() {
    return repository.getClass().getName();
  }

  @Override
  public void init(IndexingConnectorContext context) throws Exception {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    exceptionHandler = TraverseExceptionHandlerFactory.createFromConfig();
    indexingService = checkNotNull(context.getIndexingService());
    for (String traverseName : traverserConfigKey.get()) {
      context.registerTraverser(
          new TraverserConfiguration.Builder(traverseName).itemRetriever(this).build());
    }
    defaultAcl = DefaultAcl.fromConfiguration(indexingService);
    threadPoolExecutor =
        new ThreadPoolExecutor(
            DEFAULT_THREAD_NUM,
            DEFAULT_THREAD_NUM,
            0,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<Runnable>(10 * DEFAULT_THREAD_NUM),
            new ThreadPoolExecutor.CallerRunsPolicy());
    listeningExecutorService = MoreExecutors.listeningDecorator(threadPoolExecutor);
    repositoryContext =
        new RepositoryContext.Builder()
            .setEventBus(
                new AsyncEventBus(
                    "EventBus-" + ListingConnector.class.getName(), listeningExecutorService))
            .setDefaultAclMode(defaultAcl.getDefaultAclMode())
            .build();
    if (checkpointHandler == null) {
      checkpointHandler = LocalFileCheckpointHandler.fromConfiguration();
    }
    repositoryContext.getEventBus().register(this);
    repository.init(repositoryContext);
  }

  /**
   * Performs a list traversal on the data repository.
   *
   * <p>Push document ids to the Cloud Search queue. For hierarchical data repositories, push only
   * the root document ids. For non-hierarchical data repositories, push all the document ids.
   *
   * @throws IOException on SDK push errors
   * @throws InterruptedException if exception handler is interrupted
   */
  @Override
  public void traverse() throws IOException, InterruptedException {
    if (!doTraverse("full traversal", CHECKPOINT_FULL, repository::getIds)) {
      throw new NullPointerException("getIds returned null");
    }
  }

  @FunctionalInterface
  private interface GetDocsFunction {
    CheckpointCloseableIterable<ApiOperation> apply(byte[] checkpoint) throws RepositoryException;
  }

  private boolean doTraverse(String traversalType, String checkpointName, GetDocsFunction getDocs)
      throws IOException, InterruptedException {
    byte[] checkpoint = checkpointHandler.readCheckpoint(checkpointName);
    boolean hasMore = false;
    do {
      try (CheckpointCloseableIterable<ApiOperation> allIds = getDocs.apply(checkpoint)) {
        if (allIds == null) {
          return false;
        }
        execute(allIds, traversalType);
        checkpoint = allIds.getCheckpoint();
        checkpointHandler.saveCheckpoint(checkpointName, checkpoint);
        hasMore = allIds.hasMore();
      }
    } while (hasMore);
    return true;
  }

  /**
   * Performs the asynchronously pushed operation from the {@link Repository}.
   *
   * <p>This is <em>only</em> used when the {@link Repository} supports asynchronous operations
   * outside of normal traversals. Use this operation for triggered actions (such as created,
   * modified, or deleted documents) that cannot be queried from a full or incremental traversal or
   * when the change should be detected and acted on immediately.
   *
   * @param asyncOp the operation asynchronously pushed from the {@link Repository}
   */
  @Subscribe
  public void handleAsyncOperation(AsyncApiOperation asyncOp) {
    ListenableFuture<List<GenericJson>> result =
        listeningExecutorService.submit(
            () -> {
              logger.log(Level.INFO, "Processing an asynchronous repository operation.");
              try {
                return execute(
                    Collections.singleton(asyncOp.getOperation()),
                    "asynchronous repository operation");
              } catch (IOException ex) {
                logger.log(
                    Level.WARNING,
                    "Exception occured while processing an asynchronous repository operation: ",
                    ex);
                throw ex;
              }
            });
    asyncOp.setResult(result);
  }

  private List<GenericJson> execute(Iterable<ApiOperation> operations, String context)
      throws IOException, InterruptedException {
    int exceptionCount = 0;
    int operationCount = 0;
    List<GenericJson> results = new ArrayList<>();
    try {
      for (ApiOperation operation : operations) {
        try {
          operationCount++;
          results.addAll(operation.execute(indexingService, Optional.of(this::modifyApiOperation)));
        } catch (IOException e) {
          exceptionCount++;
          if (exceptionHandler.handleException(e, exceptionCount)) {
            logger.log(
                Level.WARNING,
                "Continue on error#{0}. Failed to execute ApiOperation {1} operation during {2}. "
                    + "{3}", new Object[]{exceptionCount, operation, context, e});
            continue;
          }
          throw e;
        }
      }
    } finally {
      if (exceptionCount != 0) {
        logger.log(
            Level.WARNING,
            "{0} operations failed out of {1}",
            new Object[] {exceptionCount, operationCount});
      } else if (operationCount > 0) {
        logger.log(
            Level.INFO,
            "{0} operations executed successfully during {1}.",
            new Object[]{operationCount, context});
      } else {
        logger.log(Level.INFO, "No operations provided during {0}.", context);
      }
    }
    return results;
  }

  @Override
  public void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException {
  }

  @Override
  public void destroy() {
    repository.close();
    repositoryContext.getEventBus().unregister(this);
    if (listeningExecutorService != null) {
      logger.log(Level.INFO, "Shutting down the executor service.");
      MoreExecutors.shutdownAndAwaitTermination(listeningExecutorService, 5L, TimeUnit.MINUTES);
    }
  }

  /**
   * Processes each polled document from the Cloud Search queue.
   *
   * <p>Retrieve the document for indexing. For hierarchical data repositories, also push the
   * children ids to the queue for later recursive processing.
   *
   * @param item {@link Item} representing a polled document
   * @throws IOException on SDK upload errors
   * @throws InterruptedException if exception handler is interrupted
   */
  @Override
  public void process(Item item) throws IOException, InterruptedException {
    ApiOperation apiOperation = repository.getDoc(item);
    execute(Collections.singleton(apiOperation), "process");
  }

  /**
   * Performs all actions necessary for incremental traversals.
   *
   * <p>If the {@link Repository} does not support incremental traversals, the {@link
   * Repository#getChanges(byte[])} method should return {@code null}.
   *
   * @throws IOException on SDK upload errors
   * @throws InterruptedException if exception handler is interrupted
   */
  @Override
  public void handleIncrementalChanges() throws IOException, InterruptedException {
    doTraverse("incremental traversal", CHECKPOINT_INCREMENTAL, repository::getChanges);
  }

  @VisibleForTesting
  void modifyApiOperation(ApiOperation apiOperation) {
    if (apiOperation instanceof RepositoryDoc) {
      RepositoryDoc repositoryDoc = (RepositoryDoc) apiOperation;
      defaultAcl.applyToIfEnabled(repositoryDoc.getItem());
    }
  }
}
