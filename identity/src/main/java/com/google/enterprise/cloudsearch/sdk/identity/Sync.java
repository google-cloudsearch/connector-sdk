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

import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import javax.annotation.Nullable;

/** Abstraction to sync {@link IdentityPrincipal} with Google. */
interface Sync<T> {
  /**
   * Sync {@link IdentityPrincipal} with Google.
   *
   * @param previouslySynced {@link IdentityPrincipal} previously synced with Google
   * @return Updated {@link IdentityPrincipal} as result of sync operation
   * @throws IOException when sync fails
   */
  ListenableFuture<T> sync(@Nullable T previouslySynced, IdentityService service)
      throws IOException;

  /**
   * Remove {@link IdentityPrincipal} mapping from Google.
   *
   * @param service to unmap principal
   * @return Result of unmap operation. True if successful. False otherwise.
   * @throws IOException when unmap fails
   */
  ListenableFuture<Boolean> unmap(IdentityService service) throws IOException;
}
