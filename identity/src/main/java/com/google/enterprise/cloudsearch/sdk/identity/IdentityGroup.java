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

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Group;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.Operation;
import com.google.api.services.cloudidentity.v1.model.Status;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/** Represents a third-party group to be synced with Cloud identity Groups API. */
public class IdentityGroup extends IdentityPrincipal<IdentityGroup> {
  private static final Logger logger = Logger.getLogger(IdentityGroup.class.getName());

  /** Group label to be added for each synced group as required by Cloud Identity Groups API. */
  public static final ImmutableMap<String, String> GROUP_LABELS =
      ImmutableMap.of("system/groups/external", "");

  private final ConcurrentMap<EntityKey, Membership> members;
  private final EntityKey groupKey;
  private final Optional<String> groupResourceName;

  /** Creates an instance of {@link IdentityGroup}. */
  public IdentityGroup(Builder buider) {
    super(checkNotNullOrEmpty(buider.groupIdentity, "group identity can not be null"));
    Set<Membership> memberships = checkNotNull(buider.members.get(), "members can not be null");
    this.members =
        memberships
            .stream()
            .collect(Collectors.toConcurrentMap(k -> k.getPreferredMemberKey(), v -> v));
    this.groupKey = checkNotNull(buider.groupKey, "groupKey can not be null");
    this.groupResourceName =
        checkNotNull(buider.groupResourceName, "group resource name can not be null");
  }

  /**
   * Gets {@link Membership}s under identity group.
   *
   * @return memberships under identity group.
   */
  public ImmutableSet<Membership> getMembers() {
    return ImmutableSet.copyOf(members.values());
  }

  /**
   * Gets group key for identity group.
   *
   * @return group key for identity group.
   */
  public EntityKey getGroupKey() {
    return groupKey;
  }

  /**
   * Get kind for {@link IdentityPrincipal}. This is always {@link IdentityPrincipal.Kind#GROUP} for
   * {@link IdentityGroup}.
   */
  @Override
  public Kind getKind() {
    return Kind.GROUP;
  }

  /** Removes {@link IdentityGroup} from Cloud Identity Groups API using {@code service}. */
  @Override
  public ListenableFuture<Boolean> unmap(IdentityService service) throws IOException {
    ListenableFuture<Operation> deleteGroup = service.deleteGroup(groupResourceName.get());
    return Futures.transform(
        deleteGroup,
        new Function<Operation, Boolean>() {
          @Override
          @Nullable
          public Boolean apply(@Nullable Operation input) {
            try {
              validateOperation(input);
              return true;
            } catch (IOException e) {
              logger.log(Level.WARNING, String.format("Failed to delete Group %s", groupKey), e);
              return false;
            }
          }
        },
        getExecutor());
  }

  /**
   * Syncs {@link IdentityGroup} with Cloud Identity Groups API using {@code service} including
   * group memberships.
   */
  @Override
  public ListenableFuture<IdentityGroup> sync(
      IdentityGroup previouslySynced, IdentityService service) throws IOException {
    IdentityGroup identityGroupToReturn;
    if (previouslySynced != null) {
      checkArgument(
          Objects.equals(previouslySynced.groupKey, groupKey),
          "mismatch between current group %s and previous group %s",
          groupKey,
          previouslySynced.groupKey);
      checkArgument(
          previouslySynced.groupResourceName.isPresent(),
          "missing group resource name for previously synced group");
      identityGroupToReturn = previouslySynced;
    } else {
      IdentityGroup.Builder syncedGroup =
          new IdentityGroup.Builder()
              .setGroupIdentity(identity)
              .setGroupKey(groupKey)
              .setMembers(new HashSet<>());
      // Syncing group first time. Create Group and extract group resource name
      // for memberships calls.
      logger.log(Level.FINE, "Creating group {0}", groupKey);
      ListenableFuture<Operation> createGroup = service.createGroup(toGroup());
      try {
        Operation result = createGroup.get();
        syncedGroup.setGroupResourceName(extractResourceName(result));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while creating Group", e);
      } catch (ExecutionException e) {
        throw new IOException("Error creating group", e.getCause());
      }
      identityGroupToReturn = syncedGroup.build();
    }

    Map<EntityKey, Membership> previousMembers =
        previouslySynced == null ? Collections.emptyMap() : previouslySynced.members;
    Set<EntityKey> newlyAdded = Sets.difference(members.keySet(), previousMembers.keySet());
    Set<EntityKey> removed = Sets.difference(previousMembers.keySet(), members.keySet());
    List<ListenableFuture<Membership>> membershipInsertions = new ArrayList<>();
    // Insert new memberships
    for (EntityKey memberKey : newlyAdded) {
      ListenableFuture<Membership> addMember =
          identityGroupToReturn.addMember(members.get(memberKey), service);
      membershipInsertions.add(
          wrapAsCatchingFuture(
              addMember,
              getExecutor(),
              null,
              String.format("Error inserting member %s under group %s", memberKey, groupKey)));
    }

    // Remove previously synced memberships which are no longer available.
    List<ListenableFuture<Boolean>> membershipDeletions = new ArrayList<>();
    for (EntityKey memberKey : removed) {
      ListenableFuture<Boolean> removeMember =
          identityGroupToReturn.removeMember(memberKey, service);
      membershipDeletions.add(
          wrapAsCatchingFuture(
              removeMember,
              getExecutor(),
              false,
              String.format("Error removing member %s from group %s", memberKey, groupKey)));
    }
    ListenableFuture<List<Object>> membershipResults =
        Futures.allAsList(Iterables.concat(membershipInsertions, membershipDeletions));
    return Futures.transform(
        membershipResults,
        new Function<List<Object>, IdentityGroup>() {
          @Override
          @Nullable
          public IdentityGroup apply(@Nullable List<Object> input) {
            return identityGroupToReturn;
          }
        },
        getExecutor());
  }

  @Override
  public int hashCode() {
    return Objects.hash(identity, groupKey, members, groupResourceName);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (!(obj instanceof IdentityGroup)) {
      return false;
    }

    IdentityGroup other = (IdentityGroup) obj;
    return Objects.equals(identity, other.identity)
        && Objects.equals(groupKey, other.groupKey)
        && Objects.equals(groupResourceName, other.groupResourceName)
        && Objects.equals(members, other.members);
  }

  private Executor getExecutor() {
    return MoreExecutors.directExecutor();
  }

  ListenableFuture<Membership> addMember(Membership m, IdentityService identityService)
      throws IOException {
    Membership existing = members.get(m.getPreferredMemberKey());
    if (existing != null) {
      return Futures.immediateFuture(existing);
    }
    String groupId = groupResourceName.get();
    ListenableFuture<Operation> created =
        identityService.createMembership(groupId, m);
    return Futures.transformAsync(
        created,
        new AsyncFunction<Operation, Membership>() {
          @Override
          @Nullable
          public ListenableFuture<Membership> apply(@Nullable Operation input) {
            try {
              Membership memberCreated = m.setName(extractResourceName(input));
              addMember(memberCreated);
              logger.log(Level.FINE, "Successfully created membership {0}", memberCreated);
              return Futures.immediateFuture(memberCreated);
            } catch (IOException e) {
              logger.log(
                  Level.WARNING,
                  String.format("Failed to create membership %s under group %s", m, groupId),
                  e);
              return Futures.immediateFailedFuture(e);
            }
          }
        },
        getExecutor());
  }

  ListenableFuture<Boolean> removeMember(EntityKey memberKey, IdentityService identityService)
      throws IOException {
    Membership existing = members.get(memberKey);
    if (existing == null) {
      return Futures.immediateFuture(true);
    }
    String groupId = groupResourceName.get();
    String memberId = existing.getName();
    ListenableFuture<Operation> created = identityService.deleteMembership(memberId);
    return Futures.transform(
        created,
        new Function<Operation, Boolean>() {
          @Override
          @Nullable
          public Boolean apply(@Nullable Operation input) {
            try {
              validateOperation(input);
              removeMember(memberKey);
              return true;
            } catch (IOException e) {
              logger.log(
                  Level.WARNING,
                  String.format("Failed to delete membership %s under group %s", existing, groupId),
                  e);
              return false;
            }
          }
        },
        getExecutor());
  }

  private synchronized void addMember(Membership m) {
    members.put(m.getPreferredMemberKey(), m);
  }

  private synchronized void removeMember(EntityKey memberKey) {
    members.remove(memberKey);
  }

  /** Builder for {@link IdentityGroup} */
  public static class Builder {
    private String groupIdentity;
    private EntityKey groupKey;
    private Supplier<Set<Membership>> members;
    private Optional<String> groupResourceName = Optional.empty();

    /**
     * Sets external group identity. Mapped to display name of {@link Group}.
     *
     * @param groupIdentity external group identity.
     */
    public Builder setGroupIdentity(String groupIdentity) {
      this.groupIdentity = groupIdentity;
      return this;
    }

    /**
     * Sets group key. Mapped to {@link Group#getGroupKey}
     *
     * @param groupKey group key
     */
    public Builder setGroupKey(EntityKey groupKey) {
      this.groupKey = groupKey;
      return this;
    }

    /**
     * Sets {@link Membership}s to be synced under identity group.
     *
     * @param members {@link Membership}s to be synced
     */
    public Builder setMembers(Set<Membership> members) {
      this.members = () -> members;
      return this;
    }

    /**
     * Sets {@link Membership}s to be synced under identity group.
     *
     * @param membershipSupplier supplier for {@link Membership}s to be synced
     */
    public Builder setMembers(Supplier<Set<Membership>> membershipSupplier) {
      this.members = membershipSupplier;
      return this;
    }

    /**
     * Sets resource identifier assigned by Cloud Identity Groups API. Extracted from {@link
     * Group#getName}.
     *
     * @param groupResourceName resource identifier assigned by Cloud Identity Groups API
     */
    public Builder setGroupResourceName(String groupResourceName) {
      this.groupResourceName = Optional.ofNullable(groupResourceName);
      return this;
    }

    /** Builds an instance of {@link IdentityGroup}. */
    public IdentityGroup build() {
      return new IdentityGroup(this);
    }
  }

  private Group toGroup() {
    return new Group()
        .setGroupKey(groupKey)
        .setLabels(GROUP_LABELS)
        .setParent(groupKey.getNamespace())
        .setDisplayName(identity);
  }

  private static String extractResourceName(Operation op) throws IOException {
    validateOperation(op);
    Map<String, Object> response = op.getResponse();
    if (response == null) {
      throw new IOException(String.format("Operation failed with empty response %s", op));
    }
    return (String) response.get("name");
  }

  private static void validateOperation(Operation op) throws IOException {
    checkNotNull(op, "operation can not be null");
    Status error = op.getError();
    if ((error != null) && (error.getCode() != 0)) {
      throw new IOException(String.format("Operation failed with Error %s", error));
    }
    checkState(op.getDone(), "Operation not completed yet");
  }

  private static <T> ListenableFuture<T> wrapAsCatchingFuture(
      ListenableFuture<T> result, Executor executor, T defaultValue, String errorMessage) {
    return Futures.catching(
        result,
        IOException.class,
        new Function<IOException, T>() {
          @Override
          @Nullable
          public T apply(@Nullable IOException input) {
            checkNotNull(input);
            logger.log(Level.WARNING, errorMessage, input);
            return defaultValue;
          }
        },
        executor);
  }
}
