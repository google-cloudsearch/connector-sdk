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

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Logger;

/** Current state of all identities synced with Google. */
public class IdentityState {
  private static final Logger logger = Logger.getLogger(IdentityState.class.getName());

  private final ConcurrentMap<String, Optional<IdentityUser>> usersSyncedWithGoogle;

  private final ConcurrentMap<EntityKey, IdentityGroup> groupsSyncedWithGoogle;

  IdentityState(Builder builder) {
    this.usersSyncedWithGoogle = new ConcurrentHashMap<>(checkNotNull(builder.users));
    this.groupsSyncedWithGoogle = new ConcurrentHashMap<>(checkNotNull(builder.groups));
  }

  /**
   * Gets an {@link IdentityUser} from {@link IdentityState} for corresponding google identity
   *
   * @param googleIdentity google identity for {@link IdentityUser}
   * @return {@link IdentityUser} from {@link IdentityState} for corresponding google identity
   */
  IdentityUser getUser(String googleIdentity) {
    return usersSyncedWithGoogle.getOrDefault(googleIdentity, Optional.empty()).orElse(null);
  }

  /**
   * Adds an {@link IdentityUser} to {@link IdentityState}
   *
   * @param user to add
   */
  void putUser(IdentityUser user) {
    usersSyncedWithGoogle.put(user.getGoogleIdentity(), Optional.of(user));
  }

  /**
   * Adds user identity to {@link IdentityState} with empty mappings.
   *
   * @param googleIdentity to add
   */
  void putUnmappedUser(String googleIdentity) {
    usersSyncedWithGoogle.put(googleIdentity, Optional.empty());
  }

  /**
   * Gets an {@link IdentityGroup} from {@link IdentityState} for given {@link EntityKey}
   *
   * @param groupKey {@link EntityKey} for a group
   * @return {@link IdentityGroup} from {@link IdentityState} for given {@link EntityKey}
   */
  IdentityGroup getGroup(EntityKey groupKey) {
    return groupsSyncedWithGoogle.get(groupKey);
  }

  /**
   * Adds an {@link IdentityGroup} to {@link IdentityState}
   *
   * @param group to add
   */
  void putGroup(IdentityGroup group) {
    groupsSyncedWithGoogle.put(group.getGroupKey(), group);
  }

  /**
   * Gets all google identities for {@link IdentityUser}s available in {@link IdentityState}
   *
   * @return all google identities for {@link IdentityUser} available in {@link IdentityState}
   */
  ImmutableSet<String> getAllUserGoogleIdentities() {
    return ImmutableSet.copyOf(usersSyncedWithGoogle.keySet());
  }

  /**
   * Removes an {@link IdentityUser} mapping from {@link IdentityState} for given google identity
   *
   * @param googleIdentity google identity for user to be removed from {@link IdentityState}
   */
  void removeUser(String googleIdentity) {
    usersSyncedWithGoogle.put(googleIdentity, Optional.empty());
  }

  /**
   * Gets all {@link EntityKey}s for {@link IdentityGroup}s available in {@link IdentityState}
   *
   * @return all {@link EntityKey}s for {@link IdentityGroup}s available in {@link IdentityState}
   */
  ImmutableSet<EntityKey> getAllGroupKeys() {
    return ImmutableSet.copyOf(groupsSyncedWithGoogle.keySet());
  }

  /**
   * Removes an {@link IdentityGroup} from {@link IdentityState} for given {@link EntityKey}
   *
   * @param groupKey {@link EntityKey} for group to be removed from {@link IdentityState}
   */
  void removeGroup(EntityKey groupKey) {
    groupsSyncedWithGoogle.remove(groupKey);
  }
  /**
   * Indicates if user email specified exist in {@link IdentityState}.
   *
   * @param userGoogleIdentity user email to lookup in IdentityState
   * @return true if user exist in IdentityState. False otherwise.
   */
  boolean isGoogleIdentityExists(String userGoogleIdentity) {
    return usersSyncedWithGoogle.containsKey(userGoogleIdentity);
  }

  @VisibleForTesting
  ImmutableMap<String, Optional<IdentityUser>> getUsersSyncedWithGoogle() {
    return ImmutableMap.copyOf(usersSyncedWithGoogle);
  }

  @VisibleForTesting
  ImmutableMap<EntityKey, IdentityGroup> getGroupsSyncedWithGoogle() {
    return ImmutableMap.copyOf(groupsSyncedWithGoogle);
  }

  /** Builder object for {@link IdentityState} */
  public static class Builder {
    private Map<String, Optional<IdentityUser>> users = new HashMap<>();
    private Map<EntityKey, IdentityGroup> groups = new HashMap<>();

    /** Add previously mapped user to IdentityState */
    Builder addUser(IdentityUser user) {
      users.put(checkNotNull(user).getGoogleIdentity(), Optional.of(user));
      return this;
    }

    /** Add unmapped user identity to IdentityState */
    Builder addUnmappedUser(String googleIdentity) {
      users.put(googleIdentity, Optional.empty());
      return this;
    }

    /** Add previously mapped group to IdentityState */
    Builder addGroup(IdentityGroup group) {
      groups.put(checkNotNull(group).getGroupKey(), group);
      return this;
    }

    /**
     * Build {@link IdentityState} instance.
     *
     * @return {@link IdentityState} instance.
     */
    IdentityState build() {
      return new IdentityState(this);
    }
  }
}
