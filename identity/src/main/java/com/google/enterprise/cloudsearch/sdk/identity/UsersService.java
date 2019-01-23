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

import com.google.api.services.admin.directory.model.User;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import java.io.IOException;
import java.util.Optional;

/** Wrapper for making Admin SDK Users API Calls */
interface UsersService extends Service {
  ListenableFuture<User> getUserMapping(String userId) throws IOException;

  ListenableFuture<User> updateUserMapping(
      String userId, String schemaName, String attributeName, Optional<String> value)
      throws IOException;

  Iterable<User> listUsers(String schemaName) throws IOException;
}
