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

import com.google.api.client.json.GenericJson;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

/**
 * {@link ApiOperation} to delete zero or more {@link
 * com.google.api.services.cloudsearch.v1.model.Item} from a given queue.
 *
 * <p>This object calls {@link IndexingService#deleteQueueItems}.
 */
public class DeleteQueueItems implements ApiOperation {

  private final String queueName;

  /**
   * Deletes items from a queue.
   *
   * @param queueName items with this queue name will be deleted
   */
  DeleteQueueItems(String queueName) {
    this.queueName = checkNotNull(queueName);
  }

  @Override
  public List<GenericJson> execute(IndexingService service)
      throws IOException, InterruptedException {
    try {
      return Collections.singletonList(service.deleteQueueItems(queueName).get());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      } else {
        throw new IOException(e.getCause());
      }
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(queueName);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof DeleteQueueItems)) {
      return false;
    }
    DeleteQueueItems otherDelete = (DeleteQueueItems) other;
    return this.queueName.equals(otherDelete.queueName);
  }
}
