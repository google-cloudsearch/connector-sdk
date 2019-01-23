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

import com.google.api.client.json.GenericJson;
import com.google.common.annotations.Beta;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Object encapsulating an API request to be executed using {@link IndexingService}.
 *
 * <p>For examples of the API request types, refer to the general factory class
 * {@link ApiOperations} and also {@link RepositoryDoc} which is used for update item requests.
 */
public interface ApiOperation {

  /**
   * Executes the specific API operation for the implemented class.
   *
   * @param service the {@link IndexingService} instance on which to perform the action
   * @return the indexing service responses to each operation execution
   * @throws IOException on errors during the repository data retrieval
   * @throws InterruptedException when execution of indexing API call is interrupted
   */
  List<GenericJson> execute(IndexingService service) throws IOException, InterruptedException;

  /**
   * Executes the specific API operation for the implemented class.
   * <p>
   * This method may change without notice in future releases.
   *
   * @param service the {@link IndexingService} instance on which to perform the action
   * @param operationModifier method to be applied to the operation before calling {@link
   * #execute(IndexingService)}
   * @return the indexing service responses to each operation execution
   * @throws IOException on errors during the repository data retrieval
   * @throws InterruptedException when execution of indexing API call is interrupted
   */
  @Beta
  default List<GenericJson> execute(IndexingService service,
      Optional<Consumer<ApiOperation>> operationModifier)
      throws IOException, InterruptedException {
    operationModifier.orElse((op) -> {}).accept(this);
    return this.execute(service);
  }
}
