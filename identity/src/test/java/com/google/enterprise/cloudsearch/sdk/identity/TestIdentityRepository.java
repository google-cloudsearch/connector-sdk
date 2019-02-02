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
import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestIdentityRepository implements Repository {
  private final Map<String, List<String>> userPages;
  private final Map<String, String> usersNextCheckpoint;
  private final Map<String, Boolean> usersHasMore;

  private final Map<String, List<String>> groupsPages;
  private final Map<String, String> groupsNextCheckpoint;
  private final Map<String, Boolean> groupsHasMore;
  private final String customerDomain;
  private RepositoryContext context;


  public TestIdentityRepository(Builder builder) {
    this.customerDomain = builder.customerDomain;
    this.userPages = builder.userPages;
    this.usersNextCheckpoint = builder.usersNextCheckpoint;
    this.usersHasMore = builder.usersHasMore;
    this.groupsPages = builder.groupsPages;
    this.groupsNextCheckpoint = builder.groupsNextCheckpoint;
    this.groupsHasMore = builder.groupsHasMore;
  }

  @Override
  public void init(RepositoryContext context) throws IOException {
    this.context = checkNotNull(context);
  }


  @Override
  public CheckpointCloseableIterable<IdentityUser> listUsers(byte[] checkpoint) throws IOException {
    String checkpointValue = (checkpoint == null) ? "" : new String(checkpoint, UTF_8);
    if (!userPages.containsKey(checkpointValue)) {
      throw new UnsupportedOperationException("unexpected checkpoint value " + checkpointValue);
    }
    List<IdentityUser> usersToReturn =
        userPages
            .get(checkpointValue)
            .stream()
            .map(s -> buildIdentityUser(s, context))
            .collect(ImmutableList.toImmutableList());
    return new CheckpointCloseableIterableImpl<>(
        usersToReturn, usersNextCheckpoint.get(checkpointValue), usersHasMore.get(checkpointValue));
  }

  @Override
  public CheckpointCloseableIterable<IdentityGroup> listGroups(byte[] checkpoint)
      throws IOException {
    String checkpointValue = checkpoint == null ? "" : new String(checkpoint, UTF_8);
    if (!groupsPages.containsKey(checkpointValue)) {
      throw new UnsupportedOperationException("unexpected checkpoint value " + checkpointValue);
    }
    List<IdentityGroup> groupsToReturn =
        groupsPages
            .get(checkpointValue)
            .stream()
            .map(s -> buildIdentityGroup(s, context))
            .collect(ImmutableList.toImmutableList());
    return new CheckpointCloseableIterableImpl<>(
        groupsToReturn,
        groupsNextCheckpoint.get(checkpointValue),
        groupsHasMore.get(checkpointValue));
  }

  @Override
  public void close() {
  }

  private IdentityUser buildIdentityUser(String userKey, RepositoryContext context) {
    return context.buildIdentityUser(
        String.format("%s@%s", userKey, customerDomain),
        String.format("%s\\%s", customerDomain, userKey));
  }

  private IdentityGroup buildIdentityGroup(String groupKey, RepositoryContext context) {
    String groupIdentifier = String.format("%s\\%s", customerDomain, groupKey);
    EntityKey key = context.buildEntityKeyForGroup(groupIdentifier);
    return new IdentityGroup.Builder()
        .setGroupIdentity(groupIdentifier)
        .setGroupKey(key)
        .setMembers(ImmutableSet.of())
        .build();
  }

  public ImmutableList<IdentityUser> getAllUsersList() {
    ImmutableList.Builder<IdentityUser> builder = ImmutableList.<IdentityUser>builder();
    for (Entry<String, List<String>> entry : userPages.entrySet()) {
      builder.addAll(
          entry
              .getValue()
              .stream()
              .map(s -> buildIdentityUser(s, context))
              .collect(ImmutableList.toImmutableList()));
    }
    return builder.build();
  }

  public ImmutableList<IdentityGroup> getAllGroupsList() {
    ImmutableList.Builder<IdentityGroup> builder = ImmutableList.<IdentityGroup>builder();
    for (Entry<String, List<String>> entry : groupsPages.entrySet()) {
      builder.addAll(
          entry
              .getValue()
              .stream()
              .map(s -> buildIdentityGroup(s, context))
              .collect(ImmutableList.toImmutableList()));
    }
    return builder.build();
  }

  public boolean allUserPagesClosed() {
    return true;
  }

  public boolean allGroupPagesClosed() {
    return true;
  }

  public static class Builder {
    private final String customerDomain;
    private final Map<String, List<String>> userPages = new HashMap<>();
    private final Map<String, String> usersNextCheckpoint = new HashMap<>();
    private final Map<String, Boolean> usersHasMore = new HashMap<>();

    private final Map<String, List<String>> groupsPages = new HashMap<>();
    private final Map<String, String> groupsNextCheckpoint = new HashMap<>();
    private final Map<String, Boolean> groupsHasMore = new HashMap<>();

    public Builder(String customerDomain) {
      this.customerDomain = customerDomain;
    }

    public Builder addUserPage(
        String forCheckpoint, String nextCheckpoint, boolean hasMore, List<String> usersInPage) {
      userPages.put(forCheckpoint, usersInPage);
      usersNextCheckpoint.put(forCheckpoint, nextCheckpoint);
      usersHasMore.put(forCheckpoint, hasMore);
      return this;
    }

    public Builder addGroupPage(
        String forCheckpoint, String nextCheckpoint, boolean hasMore, List<String> groupsInPage) {
      groupsPages.put(forCheckpoint, groupsInPage);
      groupsNextCheckpoint.put(forCheckpoint, nextCheckpoint);
      groupsHasMore.put(forCheckpoint, hasMore);
      return this;
    }

    public TestIdentityRepository build() {
      return new TestIdentityRepository(this);
    }
  }

  private static class CheckpointCloseableIterableImpl<T>
      implements CheckpointCloseableIterable<T> {
    private final Iterator<T> identities;
    private final byte[] nextCheckpoint;
    private final boolean hasMore;
    private final AtomicBoolean closed = new AtomicBoolean();

    private CheckpointCloseableIterableImpl(
        Collection<T> identities, String nextCheckpoint, boolean hasMore) {
      this.identities = identities.iterator();
      this.nextCheckpoint = nextCheckpoint == null ? null : nextCheckpoint.getBytes(UTF_8);
      this.hasMore = hasMore;
    }

    @Override
    public Iterator<T> iterator() {
      return identities;
    }

    @Override
    public byte[] getCheckpoint() {
      return nextCheckpoint;
    }

    @Override
    public boolean hasMore() {
      return hasMore;
    }

    @Override
    public void close() {
      closed.set(true);
    }}

  public static void main(String[] args) throws InterruptedException {
    // TODO(tvartak) : Make number of groups and domain name as command line inputs or
    // configuration values
    List<String> groups = new ArrayList<String>();
    for (int i = 0; i < 10; i++) {
      groups.add("testGroup-" + i);
    }
    IdentityApplication application =
        new IdentityApplication.Builder(
                new FullSyncIdentityConnector(
                    new TestIdentityRepository.Builder("mydomain.com")
                        .addUserPage("", "1", false, Arrays.asList("user1", "user2", "user3"))
                        .addGroupPage("", "1", false, groups)
                        .build()),
                args)
            .build();
    application.start();
  }
}
