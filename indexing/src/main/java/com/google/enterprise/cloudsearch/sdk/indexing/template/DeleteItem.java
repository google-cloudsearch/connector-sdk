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
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nullable;

/**
 * {@link ApiOperation} to delete an {@link com.google.api.services.cloudsearch.v1.model.Item}.
 *
 * <p>This object calls {@link IndexingService#deleteItem}. The {@link Repository} can return this
 * operation when the {@link com.google.api.services.cloudsearch.v1.model.Item} is no longer
 * available in the data repository.
 */
public class DeleteItem implements ApiOperation {

  private static final RequestMode DEFAULT_REQUEST_MODE = RequestMode.UNSPECIFIED;
  private static final byte[] DEFAULT_DELETE_VERSION = null;
  private final String id;
  private final byte[] version;
  private final RequestMode requestMode;

  /**
   * Deletes an item.
   *
   * <p>Uses the SDK default item versioning and {@link RequestMode}.
   *
   * @param id item id to delete
   */
  DeleteItem(String id) {
    this(id, DEFAULT_DELETE_VERSION, DEFAULT_REQUEST_MODE);
  }

  /**
   * Deletes an item with a custom version.
   *
   * @param id item id to delete
   * @param version item version to delete
   * @param requestMode request mode for delete request
   */
  DeleteItem(String id, @Nullable byte[] version, RequestMode requestMode) {
    this.id = checkNotNull(id);
    this.version = version;
    this.requestMode = checkNotNull(requestMode);
  }

  @Override
  public List<GenericJson> execute(IndexingService service)
      throws IOException, InterruptedException {
    try {
      return Collections.singletonList(service.deleteItem(id, version, requestMode).get());
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  public String getId() {
    return id;
  }

  @Override
  public String toString() {
    return "DeleteItem [itemId=" + id + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, Arrays.hashCode(version), requestMode);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (!(other instanceof DeleteItem)) {
      return false;
    }
    DeleteItem otherDelete = (DeleteItem) other;
    return this.id.equals(otherDelete.id)
        && Arrays.equals(this.version, otherDelete.version)
        && requestMode == otherDelete.requestMode;
  }
}
