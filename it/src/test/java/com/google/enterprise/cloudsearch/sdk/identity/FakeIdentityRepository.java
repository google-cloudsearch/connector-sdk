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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * This fake identity repository is used as a source of data for integration tests against the
 * identity connector SDK.
 *
 * An identity repository An identity {@link Repository} allows to list users or groups in "pages",
 * more or less as with
 *
 * <pre>
 *   page = repo.listUsers("".getBytes(StandardCharsets.UTF_8));
 *   while (!page.hasMore()) {
 *     // Process the users in current page...
 *     ...
 *     // Get the next batch of users...
 *     page = repo.listUsers(it.getCheckpoint());
 *   }
 * </pre>
 *
 * The {@code FakeIdentityRepository} allows to cluster users or groups in pages but also provides a
 * second level of aggregation called a "snapshot", which simulates the state of an identity source
 * at a given instant. This allows to model scenarios where users/groups are added or removed. For
 * example:
 *
 * <pre>
 *   - Snapshot 1:
 *     - Users:
 *       - Page 1: [user1, user2]
 *       - Page 2: [user3, user4]
 *   - Snapshot 2 (user5 was added):
 *     - Users:
 *       - Page 1: [user1, user2]
 *       - Page 2: [user3, user4, user5]
 *   - Snapshot 3 (user1 was deleted):
 *     - Users:
 *       - Page 1: [user2, user3, user4, user5]
 * </pre>
 *
 * The following is an example of the canonical usage of this class:
 *
 * <pre>
 *   FakeIdentityRepository fakeIdRepo =
 *       new FakeIdentityRepository.Builder("example.com")
 *       .addSnapshot(
 *           new String[][] {
 *               {"user1", "user2"}, // Page 1
 *               {"user3", "user4"}, // Page 2
 *           },
 *           new String[][] {
 *               {"user1", "user2"}, // Page 1
 *               {"user3", "user4"}, // Page 2
 *           })
 *       .build()
 *   while (fakeIdRepo.hasMoreSnapshots()) {
 *     fakeIdRepo.nextSnapshot();
 *     FakeIdentityRepository.Snapshot snapshot = fakeIdRepo.nextSnapshot();
 *     IdentityApplication application =
 *         new IdentityApplication.Builder(
 *             new FullSyncIdentityConnector(fakeIdRepo), args)
 *             .build();
 *     application.start();
 *     // Wait until the application is done.
 *     // Verify the users were synced correctly.
 *   }
 * </pre>
 *
 */
public class FakeIdentityRepository implements Repository {

  private static final Logger logger = Logger.getLogger(FakeIdentityRepository.class.getName());
  private static final byte[] LAST_CHECKPOINT_VALUE = new byte[0];

  private final ImmutableList<Snapshot> snapshots;
  private final Iterator<Snapshot> snapshotIterator;
  private final String domain;
  private Snapshot currentSnapshot;
  private RepositoryContext context;
  private Snapshot previousSnapshot;
  private AtomicBoolean isClosed = new AtomicBoolean(false);

  /**
   * Represents the state of the repository at a given time. The repository state is given by the
   * users in it (grouped by {@link Page}s for the purposes of the test).
   */
  static class Snapshot {

    private final ImmutableMap<String, Page> userPagesMap;
    private final ImmutableMap<String, Page> groupPagesMap;
    private final String domain;

    Snapshot(
        String domain, ImmutableMap<String, Page> userPagesMap,
        ImmutableMap<String, Page> groupPagesMap) {
      this.domain = domain;
      this.userPagesMap = userPagesMap;
      this.groupPagesMap = groupPagesMap;
    }

    /**
     * Gets all the user names (e.g., {@code "user"}) in this snapshot from all pages.
     */
    ImmutableSet<String> getAllUserNames() {
      return  userPagesMap
          .values()
          .stream()
          .map(Page::getNames)
          .flatMap(ImmutableList::stream)
          .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Gets all the group names (e.g., {@code "group"}) in this snapshot from all pages.
     */
    ImmutableSet<String> getAllGroupNames() {
      return  groupPagesMap
          .values()
          .stream()
          .map(Page::getNames)
          .flatMap(ImmutableList::stream)
          .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * Gets all the group IDs (e.g., {@code "example.com/group"}) in this snapshot from all pages.
     */
    ImmutableSet<String> getAllGroupIds() {
      return  getAllGroupNames()
          .stream()
          .map(name -> buildGroupId(domain, name))
          .collect(ImmutableSet.toImmutableSet());
    }

    ImmutableMap<String, Page> getUserPagesMap() {
      return userPagesMap;
    }
  }

  private static class Page {

    private final String checkpoint;
    private final ImmutableList<String> names;
    private final byte[] nextCheckpoint;
    private final boolean lastPage;

    Page(
        String checkpoint, ImmutableList<String> names, byte[] nextCheckpoint,
        boolean lastPage) {
      this.checkpoint = checkpoint;
      this.names = names;
      this.nextCheckpoint = nextCheckpoint;
      this.lastPage = lastPage;
    }

    public String toString() {
      return String.format(
          "{\n\tcheckpoint = \"%s\",\n\tnames = [%s],\n\tnextCheckpoint = \"%s\","
              + "\n\tlastPage = %b\n}",
          this.checkpoint, String.join(", ", this.names),
          new String(this.nextCheckpoint, UTF_8), this.lastPage);
    }

    byte[] getNextCheckpoint() {
      return nextCheckpoint;
    }

    ImmutableList<String> getNames() {
      return names;
    }

    boolean isLastPage() {
      return lastPage;
    }
  }

  static String buildExternalIdentity(String domain, String userKey) {
    return String.format("%s\\%s", domain, userKey);
  }

  static String buildUserEmail(String domain, String userKey) {
    return String.format("%s@%s", userKey, domain);
  }

  @Override
  public void init(RepositoryContext context) {
    checkState(currentSnapshot != null,
        "invalid current user stage (likely caused by nextSnapshot() not being called)");
    this.context = context;
  }

  @Override
  public CheckpointCloseableIterable<IdentityUser> listUsers(byte[] checkpoint) {
    return listIdentities(checkpoint, currentSnapshot.userPagesMap, this::buildIdentityUser);
  }

  @Override
  public CheckpointCloseableIterable<IdentityGroup> listGroups(byte[] checkpoint) {
    return listIdentities(checkpoint, currentSnapshot.groupPagesMap, this::buildIdentityGroup);
  }

  @Override
  public void close() {
    isClosed.set(true);
  }

  boolean hasMoreSnapshots() {
    return this.snapshotIterator.hasNext();
  }

  Snapshot nextSnapshot() {
    isClosed.set(false);
    previousSnapshot = currentSnapshot;
    currentSnapshot = snapshotIterator.next();
    return currentSnapshot;
  }

  /**
   * Returns all the users from all the snapshots. Useful to create and delete them during set up
   * and tear down.
   */
  ImmutableSet<String> getAllUserEmails() {
    return  snapshots
        .stream()
        .map(Snapshot::getAllUserNames)
        .flatMap(ImmutableSet::stream)
        .map(name -> buildUserEmail(domain, name))
        .collect(ImmutableSet.toImmutableSet());
  }

  ImmutableSet<String> getAllGroupNames() {
    return  snapshots
        .stream()
        .map(Snapshot::getAllGroupNames)
        .flatMap(ImmutableSet::stream)
        .collect(ImmutableSet.toImmutableSet());
  }

  ImmutableSet<String> getAllGroupIds() {
    return  getAllGroupNames()
        .stream()
        .map(name -> buildGroupId(domain, name))
        .collect(ImmutableSet.toImmutableSet());
  }

  String getDomain() {
    return domain;
  }

  boolean isClosed() {
    return isClosed.get();
  }

  /**
   * Returns the users present in the previous snapshot but not in the current one.
   */
  Set<String> getRemovedUserNames() {
    if (previousSnapshot == null) {
      return emptySet();
    }
    return Sets.difference(previousSnapshot.getAllUserNames(), currentSnapshot.getAllUserNames());
  }

  /**
   * Returns the IDs (e.g., "domain/name") of the groups present in the previous snapshot but not
   * in the current one.
   */
  Set<String> getRemovedGroupIds() {
    if (previousSnapshot == null) {
      return emptySet();
    }

    HashSet<String> diff = new HashSet<>(previousSnapshot.getAllGroupIds());
    diff.removeAll(currentSnapshot.getAllGroupIds());
    return diff;
  }

  private FakeIdentityRepository(String domain, ImmutableList<Snapshot> snapshots) {
    checkNotNull(domain);
    domain = domain.trim();
    checkArgument(!domain.isEmpty(), "domain is empty");
    checkNotNull(snapshots);
    this.domain = domain;
    this.snapshotIterator = snapshots.iterator();
    checkArgument(snapshotIterator.hasNext(), "no snapshots were provided");
    this.snapshots = snapshots;
  }

  private IdentityUser buildIdentityUser(String userKey) {
    return context.buildIdentityUser(
        buildUserEmail(domain, userKey),
        buildExternalIdentity(domain, userKey));
  }

  private static String buildGroupId(String domain, String groupName) {
    return String.format("%s/%s", domain, groupName);
  }

  private IdentityGroup buildIdentityGroup(String groupName) {
    String groupIdentifier = buildGroupId(domain, groupName);
    return new IdentityGroup.Builder()
        .setGroupIdentity(groupIdentifier)
        .setGroupKey(context.buildEntityKeyForGroup(groupIdentifier))
        .setMembers(ImmutableSet.of())
        .build();
  }

  private String convertCheckpointBytesToString(byte[] checkpoint) {
    return checkpoint == null ? "" : new String(checkpoint, UTF_8);
  }

  private <T extends IdentityPrincipal<T>> CheckpointCloseableIterable<T> listIdentities(
      byte[] checkpoint, ImmutableMap<String, Page> pagesMap, Function<String, T> identityBuilder) {
    if (pagesMap.isEmpty()) {
      return new CheckpointCloseableIterableImpl.Builder<>(new ArrayList<T>())
          .setCheckpoint(LAST_CHECKPOINT_VALUE)
          .setHasMore(false)
          .build();
    }
    String checkpointValue = convertCheckpointBytesToString(checkpoint);
    logger.log(Level.INFO, "Listing identities for checkpoint \"{0}\"...", checkpointValue);
    checkArgument(
        pagesMap.containsKey(checkpointValue),
        "unexpected checkpoint: %s", checkpointValue);
    Page page = pagesMap.get(checkpointValue);
    logger.log(
        Level.FINE, "Page for checkpoint \"{0}\" is {1}.", new Object[] {checkpointValue, page});
    List<T> groups = page.names.stream()
        .map(identityBuilder)
        .collect(Collectors.toList());
    return new CheckpointCloseableIterableImpl.Builder<>(groups)
        .setCheckpoint(page.nextCheckpoint)
        .setHasMore(!page.lastPage)
        .build();
  }

  static class Builder {

    final String domain;
    ArrayList<String[][]> userPagesList = new ArrayList<>();
    ArrayList<String[][]> groupPagesList = new ArrayList<>();

    Builder(String domain) {
      this.domain = domain;
    }

    Builder addSnapshot(String[][] userPages, String[][] groupPages) {
      userPagesList.add(userPages);
      groupPagesList.add(groupPages);
      return this;
    }

    FakeIdentityRepository build() {
      List<Snapshot> snapshots = new ArrayList<>();
      for (int i = 0; i < userPagesList.size(); i++) {
        Map<String, Page> userPagesMap = buildMap(userPagesList.get(i));
        Map<String, Page> groupPagesMap = buildMap(groupPagesList.get(i));
        Snapshot snapshot = new Snapshot(
            domain, ImmutableMap.copyOf(userPagesMap), ImmutableMap.copyOf(groupPagesMap));
        snapshots.add(snapshot);
      }
      return new FakeIdentityRepository(domain, ImmutableList.copyOf(snapshots));
    }

    private static String computeCurrentCheckpoint(int pageNumber) {
      return pageNumber == 0 ? "" : String.valueOf(pageNumber);
    }

    private static byte[] computeNextCheckpoint(boolean isLastPage, int pageNumber) {
      return isLastPage ? LAST_CHECKPOINT_VALUE : String.valueOf(pageNumber + 1).getBytes();
    }

    private static String[] trimStrings(String[] stringArray) {
      return Arrays.stream(stringArray)
          .map(String::trim)
          .toArray(String[]::new);
    }

    private static Map<String, Page> buildMap(String[][] pages) {
      Map<String, Page> pagesMap = new HashMap<>();
      for (int i = 0; i < pages.length; i++) {
        String curCheckpoint = computeCurrentCheckpoint(i);
        boolean lastPage = i == pages.length - 1;
        byte[] nextCheckpoint = computeNextCheckpoint(lastPage, i);
        String[] trimmedNames = trimStrings(pages[i]);
        ImmutableList<String> namesList = ImmutableList.copyOf(trimmedNames);
        pagesMap.put(
            curCheckpoint, new Page(
                curCheckpoint, namesList, nextCheckpoint, lastPage));
      }
      return pagesMap;
    }
  }
}
