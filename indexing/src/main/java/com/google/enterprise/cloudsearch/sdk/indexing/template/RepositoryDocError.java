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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.client.json.GenericJson;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * {@link ApiOperation} to push repository error while processing an item from a content repository.
 */
public class RepositoryDocError implements ApiOperation {

  private final String itemId;
  private final RepositoryException repositoryDocException;

  // TODO(tvartak) : Add support for queue so item can be pushed to non default queue.
  public RepositoryDocError(String itemId, RepositoryException exception) {
    checkArgument(!Strings.isNullOrEmpty(itemId), "itemId can not be null or empty");
    this.itemId = itemId;
    this.repositoryDocException = checkNotNull(exception, "exception can not be null");
  }

  @Override
  public List<GenericJson> execute(IndexingService service)
      throws IOException, InterruptedException {
    PushItem item =
        new PushItem()
            .setType("REPOSITORY_ERROR")
            .setRepositoryError(repositoryDocException.getRepositoryError());
    try {
      return Collections.singletonList(service.push(itemId, item).get());
    } catch (ExecutionException e) {
      throw new IOException("Error pushing repository error", e.getCause());
    }
  }
}
