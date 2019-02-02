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

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import java.io.Closeable;
import java.io.IOException;

interface StateManager extends Closeable {

  /**
   * Initialize {@link StateManager} with initial {@link IdentityState}
   *
   * @param identityStateLoader {@link IdentityStateLoader} to load initial {@link IdentityState}
   * @throws IOException if {@link IdentityStateLoader#getInitialIdentityState} fails
   */
  void init(IdentityStateLoader identityStateLoader) throws IOException;

  /**
   * Sync {@link IdentityUser} with Google using {@link IdentityService}. If sync is successful,
   * then {@link StateManagerImpl} is updated to include {@link IdentityUser}.
   *
   * @param user to sync
   * @param service to sync user with Google
   * @throws IOException if sync operation fails.
   */
  ListenableFuture<IdentityUser> syncUser(IdentityUser user, IdentityService service)
      throws IOException;

  /**
   * Sync {@link IdentityGroup} with Google using {@link IdentityService}. If sync is successful,
   * then {@link StateManagerImpl} is updated to include / update {@link IdentityGroup}.
   *
   * @param group to sync
   * @param service to sync group with Google
   * @throws IOException if sync operation fails.
   */
  ListenableFuture<IdentityGroup> syncGroup(IdentityGroup group, IdentityService service)
      throws IOException;

  /**
   * Sync all users with Google
   *
   * @param allUsers to sync
   * @param service IdentityService to update user mapping
   * @param exceptionHandler exception handler to handle errors during user identity sync
   * @throws IOException if sync fails
   */
  void syncAllUsers(
      Iterable<IdentityUser> allUsers, IdentityService service, ExceptionHandler exceptionHandler)
      throws IOException;

  /**
   * Remove a user mapping from Google for specified Google identity. If {@link IdentityUser#unmap}
   * is successful, corresponding {@link IdentityUser} is removed from {@link StateManagerImpl}.
   *
   * @param googleIdentity to unmap
   * @param service to unmap user identity
   * @throws IOException if unmap operation fails.
   */
  ListenableFuture<Boolean> removeUser(String googleIdentity, IdentityService service)
      throws IOException;

  /**
   * Sync all groups with Google
   *
   * @param allGroups to sync
   * @param service IdentityService to sync groups and memberships
   * @param exceptionHandler exception handler to handle errors during group sync
   * @throws IOException if sync fails
   */
  void syncAllGroups(
      Iterable<IdentityGroup> allGroups, IdentityService service, ExceptionHandler exceptionHandler)
      throws IOException;

  /**
   * Remove/ delete group from Google. If delete operation is successful, corresponding {@link
   * IdentityGroup} is removed from {@link StateManagerImpl}.
   *
   * @param groupKey for group to be deleted.
   * @param service identity service to delete group.
   * @throws IOException if delete group operation fails.
   */
  ListenableFuture<Boolean> removeGroup(EntityKey groupKey, IdentityService service)
      throws IOException;

  /**
   * Add a member to previously synced group.
   *
   * @param groupKey group key for parent group.
   * @param memberKey member key to add member for
   * @param service identity service to create membership
   * @throws IOException if create membership fails
   */
  void addMember(EntityKey groupKey, EntityKey memberKey, IdentityService service)
      throws IOException;

  /**
   * Remove a member from previously synced group.
   *
   * @param groupKey group key for parent group.
   * @param memberKey member key to remove member for
   * @param service identity service to delete membership
   * @throws IOException if delete membership fails
   */
  void removeMember(EntityKey groupKey, EntityKey memberKey, IdentityService service)
      throws IOException;
}