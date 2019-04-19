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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.admin.directory.model.User;
import com.google.api.services.cloudidentity.v1.model.Group;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.Operation;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.Optional;
import java.util.logging.Logger;

/** Access point between the connector developer and Google APIs for syncing identities. */
public class IdentityServiceImpl extends AbstractIdleService implements IdentityService {
  private static final Logger logger = Logger.getLogger(IdentityServiceImpl.class.getName());

  private final GroupsService groupsService;
  private final UsersService usersService;

  private IdentityServiceImpl(Builder builder) {
    this.groupsService = checkNotNull(builder.groupsService, "Group service can not be null");
    this.usersService = checkNotNull(builder.usersService, "users service can not be null");
  }

  /** Builder for creating an instance of {@link IdentityService} */
  public static class Builder {
    private GroupsService groupsService;
    private UsersService usersService;

    /**
     * Sets {@link GroupsService} to be used for syncing groups.
     *
     * @param groupsService to be used for syncing groups.
     */
    public Builder setGroupsService(GroupsService groupsService) {
      this.groupsService = groupsService;
      return this;
    }

    /**
     * Sets {@link UsersService} to be used for syncing users.
     *
     * @param usersService to be used for syncing users.
     */
    public Builder setUsersService(UsersService usersService) {
      this.usersService = usersService;
      return this;
    }

    /** Builds an instance of {@link IdentityService} */
    public IdentityService build() {
      return new IdentityServiceImpl(this);
    }
  }

  /** Creates a {@link Group} using Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Operation> createGroup(Group group) throws IOException {
    return groupsService.createGroup(group);
  }

  /** Gets a {@link Group} from Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Group> getGroup(String groupId) throws IOException {
    return groupsService.getGroup(groupId);
  }

  /** Deletes a {@link Group} using Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Operation> deleteGroup(String groupId) throws IOException {
    return groupsService.deleteGroup(groupId);
  }

  /**
   * Creates a {@link Membership} under group identified by {@code groupId} using Cloud Identity
   * Groups API.
   */
  @Override
  public ListenableFuture<Operation> createMembership(String groupId, Membership member)
      throws IOException {
    return groupsService.createMembership(groupId, member);
  }

  /** Gets a {@link Membership} from Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Membership> getMembership(String memberId) throws IOException {
    return groupsService.getMembership(memberId);
  }

  /** Deletes a {@link Membership} using Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Operation> deleteMembership(String memberId) throws IOException {
    return groupsService.deleteMembership(memberId);
  }

  /** Gets {@link User} from Google Admin SDK API. */
  @Override
  public ListenableFuture<User> getUserMapping(String userId) throws IOException {
    return usersService.getUserMapping(userId);
  }

  /** Updates {@link User}'s custom schema attributes using Google Admin SDK API. */
  @Override
  public ListenableFuture<User> updateUserMapping(
      String userId, String schemaName, String attributeName, Optional<String> value)
      throws IOException {
    return usersService.updateUserMapping(userId, schemaName, attributeName, value);
  }

  /** List all {@link Group}s available under given {@code groupNamespace} */
  @Override
  public Iterable<Group> listGroups(String groupNamespace) throws IOException {
    return groupsService.listGroups(groupNamespace);
  }

  /** List all {@link Membership}s under given {@code groupId} */
  @Override
  public Iterable<Membership> listMembers(String groupId) throws IOException {
    return groupsService.listMembers(groupId);
  }

  /** Lists all {@link User}s using Google Admin SDK API. */
  @Override
  public Iterable<User> listUsers(String schemaName) throws IOException {
    return usersService.listUsers(schemaName);
  }

  @Override
  protected void startUp() throws Exception {
    groupsService.startAsync().awaitRunning();
    usersService.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    groupsService.stopAsync().awaitTerminated();
    usersService.stopAsync().awaitTerminated();
  }
}
