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
package com.google.enterprise.cloudsearch.sdk.identity;

import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import java.io.IOException;

/** An Identity repository for listing Users and Groups from a repository */
public interface Repository {
  /**
   * Initialize {@link Repository} for traversal. Called from {@link IdentityConnector#init}. This
   * method can be retried if {@link IdentityConnector#init} is retried.
   *
   * @param context
   * @throws IOException
   */
  void init(RepositoryContext context) throws IOException;

  /**
   * List all users from Identity Repository. Implement can use pagination provided by {@link
   * CheckpointCloseableIterable} to list users in batches.
   *
   * @param checkpoint checkpoint from previous batch.
   * @return Full list of {@link IdentityUser}s to sync
   * @throws IOException if listing of users fails.
   */
  CheckpointCloseableIterable<IdentityUser> listUsers(byte[] checkpoint) throws IOException;

  /**
   * List all groups from Identity Repository. Implement can use pagination provided by {@link
   * CheckpointCloseableIterable} to list groups in batches.
   *
   * @param checkpoint checkpoint from previous batch.
   * @return Full list of {@link IdentityGroup}s to sync
   * @throws IOException if listing of users fails.
   */
  CheckpointCloseableIterable<IdentityGroup> listGroups(byte[] checkpoint) throws IOException;

  /** Closes the data repository and releases resources such as connections or executors. */
  void close();
}
