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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.enterprise.cloudsearch.sdk.ConnectorScheduler;
import com.google.enterprise.cloudsearch.sdk.indexing.traverser.TraverserConfiguration;
import com.google.enterprise.cloudsearch.sdk.indexing.traverser.TraverserWorker;
import com.google.enterprise.cloudsearch.sdk.indexing.traverser.TraverserWorkerManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;


/** Handles scheduling and execution of indexing connector traversal related tasks. */
public class ConnectorTraverser extends ConnectorScheduler<IndexingConnectorContext> {

  private final IndexingConnectorContext context;
  private List<TraverserWorker> traversers;

  private ConnectorTraverser(Builder builder) {
    super(builder);
    checkNotNull(builder.context);
    this.context = builder.context;
  }

  @Override
  public synchronized void start() {
    super.start();

    ConnectorSchedule schedule = getConnectorSchedule();
    if (schedule.isRunOnce()) {
      // We do not launch TraverserWorkers in RunOnce mode
      traversers = Collections.emptyList();
    } else {
      List<TraverserWorker> traverserWorkers = new ArrayList<>();
      for (TraverserConfiguration configuration : context.getTraverserConfiguration()) {
        TraverserWorker traverser =
            TraverserWorkerManager.newWorker(
                configuration, context.getIndexingService(), getBackgroundExecutor());
        traverserWorkers.add(traverser);
        Runnable pollQueueRunnable =
            new BackgroundRunnable(
                new OneAtATimeRunnable(() -> traverser.poll(), traverser.getName()));
        getScheduledExecutor().scheduleAtFixedRate(
            pollQueueRunnable, 0, schedule.getPollQueueIntervalSecs(), TimeUnit.SECONDS);
      }
      traversers = Collections.unmodifiableList(traverserWorkers);
    }
  }

  @Override
  public synchronized void stop() {
    traversers.forEach((f)->f.shutdown());
    super.stop();
  }

  public static class Builder extends
      AbstractBuilder<Builder, IndexingConnectorContext> {
    @Override
    protected Builder getThis() {
      return this;
    }

    @Override
    public ConnectorTraverser build() {
      return new ConnectorTraverser(this);
    }
  }
}
