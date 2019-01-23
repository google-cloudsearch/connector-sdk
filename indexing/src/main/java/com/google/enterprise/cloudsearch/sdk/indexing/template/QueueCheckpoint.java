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
import static com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector.CHECKPOINT_QUEUE;

import com.google.api.client.util.Key;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;

/**
 * Handles checkpoints for the queue names used for detecting deletes during full traversals.
 */
public class QueueCheckpoint {
  final boolean useQueues;
  final CheckpointHandler checkpointHandler;
  final String queueA;
  final String queueB;

  private QueueCheckpoint(boolean useQueues, CheckpointHandler checkpointHandler,
      String queueA, String queueB) {
    this.useQueues = useQueues;
    this.checkpointHandler = checkpointHandler;
    this.queueA = queueA;
    this.queueB = queueB;
  }

  boolean isManagedQueueName(String name) {
    if (name == null) {
      return false;
    }
    return name.equals(queueA) || name.equals(queueB);
  }

  synchronized String getNextQueueName(String currentQueue) {
    if (!useQueues) {
      return null;
    }
    return currentQueue.equals(queueA) ? queueB : queueA;
  }

  synchronized String getCurrentQueueName() throws IOException {
    if (!useQueues) {
      return null;
    }
    QueueData checkpoint = readQueueCheckpoint();
    if (checkpoint.getQueueName() == null) {
      checkpoint.setQueueName(queueA);
      saveQueueCheckpoint(checkpoint);
    }
    return checkpoint.getQueueName();
  }

  synchronized String getCurrentOperation() throws IOException {
    if (!useQueues) {
      return null;
    }
    QueueData checkpoint = readQueueCheckpoint();
    return checkpoint.getOperationName();
  }

  synchronized void saveCheckpoint(String queueName, String operationName) throws IOException {
    if (!useQueues) {
      return;
    }
    QueueData checkpoint = new QueueData()
        .setQueueName(queueName)
        .setOperationName(operationName);
    saveQueueCheckpoint(checkpoint);
  }

  private void saveQueueCheckpoint(QueueData checkpoint) throws IOException {
    checkpointHandler.saveCheckpoint(CHECKPOINT_QUEUE, checkpoint.get());
  }

  private QueueData readQueueCheckpoint() throws IOException {
    byte[] value = checkpointHandler.readCheckpoint(CHECKPOINT_QUEUE);
    if (value != null) {
      return JsonCheckpoint.parse(value, QueueData.class);
    }
    return new QueueData();
  }

  static class Builder {
    final boolean useQueues;
    CheckpointHandler checkpointHandler;
    String queueA;
    String queueB;

    Builder(boolean useQueues) {
      this.useQueues = useQueues;
    }

    Builder setCheckpointHandler(CheckpointHandler checkpointHandler) {
      this.checkpointHandler = checkpointHandler;
      return this;
    }

    Builder setQueueA(String queueA) {
      this.queueA = queueA;
      return this;
    }

    Builder setQueueB(String queueB) {
      this.queueB = queueB;
      return this;
    }

    QueueCheckpoint build() {
      if (useQueues) {
        checkNotNull(checkpointHandler, "CheckpointHandler can't be null");
        checkNotNull(queueA, "Queue A can't be null");
        checkNotNull(queueB, "Queue B can't be null");
      }
      return new QueueCheckpoint(useQueues, checkpointHandler, queueA, queueB);
    }
  }

  /** Represents queue checkpoint information maintained by {@link FullTraversalConnector} */
  public static class QueueData extends JsonCheckpoint {
    @Key
    private String queueName;

    @Key
    private String operationName;

    /** Constructs an empty instance of {@link QueueCheckpoint.QueueData}. */
    public QueueData() {
      super();
    }

    /**
     * Gets queue name used for traversal.
     *
     * @return queue name used for traversal.
     */
    public String getQueueName() {
      return queueName;
    }

    /**
     * Sets queue name used for traversal.
     *
     * @param queueName queue name used for traversal
     * @return this instance of {@link QueueCheckpoint.QueueData}
     */
    public QueueData setQueueName(String queueName) {
      this.queueName = queueName;
      return this;
    }

    /**
     * Gets resource name as returned by {@link IndexingService#deleteQueueItems} long running
     * operation.
     *
     * @return the resource name for {@link IndexingService#deleteQueueItems} long running
     *     operation.
     */
    public String getOperationName() {
      return operationName;
    }

    /**
     * Gets resource name as returned by {@link IndexingService#deleteQueueItems} long running
     * operation.
     *
     * @param operationName the resource name for {@link IndexingService#deleteQueueItems} long
     *     running operation.
     * @return this instance of {@link QueueCheckpoint.QueueData}
     */
    public QueueData setOperationName(String operationName) {
      this.operationName = operationName;
      return this;
    }
  }
}
