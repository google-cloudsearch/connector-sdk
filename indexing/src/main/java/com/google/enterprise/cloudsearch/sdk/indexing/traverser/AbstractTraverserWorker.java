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

import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.common.util.concurrent.SimpleTimeLimiter;
import com.google.common.util.concurrent.TimeLimiter;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

abstract class AbstractTraverserWorker implements TraverserWorker {
  private final Logger logger = Logger.getLogger(AbstractTraverserWorker.class.getName());

  /* used as named constant for calling TimeLimiter methods */
  protected static final boolean AM_INTERRUPTIBLE = true;

  protected final String name;
  protected final IndexingService indexingService;
  protected final PollItemsRequest pollRequest;
  protected final long timeout;
  protected final TimeUnit timeunit;

  private final ExecutorService timeLimiterExecutor;
  protected final TimeLimiter timeLimiter;

  public AbstractTraverserWorker(TraverserConfiguration conf, IndexingService indexingService) {
    this.name = conf.getName();
    this.indexingService = indexingService;
    this.pollRequest = conf.getPollRequest();
    this.timeout = conf.getTimeout();
    this.timeunit = conf.getTimeunit();

    timeLimiterExecutor = Executors.newCachedThreadPool();
    timeLimiter = SimpleTimeLimiter.create(timeLimiterExecutor);
  }
  @Override
  public String getName() {
    return name;
  }

  @Override
  public final void shutdown() {
    if (timeLimiterExecutor.isShutdown()) {
      return;
    }
    timeLimiterExecutor.shutdown();
    try {
      timeLimiterExecutor.awaitTermination(10L, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted during executor termination.", e);
      Thread.currentThread().interrupt();
    }
    timeLimiterExecutor.shutdownNow();
    shutdownWorker();
  }

  abstract void shutdownWorker();
}
