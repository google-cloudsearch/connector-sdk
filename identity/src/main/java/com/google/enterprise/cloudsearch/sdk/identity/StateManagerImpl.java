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
import static com.google.common.base.Preconditions.checkState;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.admin.directory.model.User;
import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.StatsManager;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.config.ConfigValue;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/** Current state of all identities synced with Google. */
class StateManagerImpl implements StateManager {
  private static final Logger logger = Logger.getLogger(StateManagerImpl.class.getName());

  public static final String CONFIG_TRAVERSE_PARTITION_SIZE = "traverse.partitionSize";
  public static final int DEFAULT_TRAVERSE_PARTITION_SIZE = 50;
  private static final OperationStats STATE_MANAGER_STATS =
      StatsManager.getComponent("StateManager");
  private static final ImmutableSet<Integer> UNKNOWN_USER_ERROR_CODES =
      new ImmutableSet.Builder<Integer>()
          .add(HTTP_NOT_FOUND) // User does not exist
          .add(HTTP_BAD_REQUEST) // Email address points to Google Group
          .add(HTTP_FORBIDDEN) // User email exist but from other domain
          .build();

  private final AtomicReference<IdentityState> identityState;
  private final ListeningExecutorService callbackExecutor;
  private final AtomicBoolean isRunning;
  private final ConfigValue<Integer> partitionSize;

  StateManagerImpl() {
    ExecutorService executor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("identitystate-callback")
                .build());
    this.callbackExecutor = MoreExecutors.listeningDecorator(executor);
    this.identityState = new AtomicReference<>();
    this.isRunning = new AtomicBoolean();
    this.partitionSize =
        Configuration.getInteger(CONFIG_TRAVERSE_PARTITION_SIZE, DEFAULT_TRAVERSE_PARTITION_SIZE);
  }

  /**
   * Sync {@link IdentityUser} with Google using {@link IdentityService}. If sync is successful,
   * then {@link IdentityState} is updated to include {@link IdentityUser}.
   *
   * @param user to sync
   * @param service to sync user with Google
   * @throws IOException if sync operation fails.
   */
  @Override
  public ListenableFuture<IdentityUser> syncUser(IdentityUser user, IdentityService service)
      throws IOException {
    checkState(isRunning(), "state manager is not active");
    String googleIdentity = user.getGoogleIdentity();
    if (!identityState.get().isGoogleIdentityExists(googleIdentity)) {
      logger.log(
          Level.INFO,
          "User [{0}] is not known as per Identity State. Performing lookup.",
          googleIdentity);
      if (!checkIfUserExist(googleIdentity, service)) {
        STATE_MANAGER_STATS.logResult("Unknown Google Identity for User", googleIdentity);
        logger.log(
            Level.WARNING,
            "User email [{0}] does not apear to be valid. Skipping user sync.",
            googleIdentity);
        return Futures.immediateFuture(null);
      } else {
        // Add newly discovered user to IdentityState
        logger.log(
            Level.INFO, "Adding newly discovered User [{0}] to identity state.", googleIdentity);
        identityState.get().putUnmappedUser(googleIdentity);
      }
    }
    IdentityUser previouslySynced = identityState.get().getUser(googleIdentity);
    ListenableFuture<IdentityUser> sync =
        callbackExecutor.submit(() -> user.sync(previouslySynced, service).get());
    return Futures.transform(
        sync,
        new Function<IdentityUser, IdentityUser>() {
          @Override
          @Nullable
          public IdentityUser apply(@Nullable IdentityUser input) {
            checkNotNull(input);
            identityState.get().putUser(user);
            logger.log(Level.INFO, "Successfully synced identity user {0}", user);
            return input;
          }
        },
        getExecutor());
  }

  /**
   * Sync {@link IdentityGroup} with Google using {@link IdentityService}. If sync is successful,
   * then {@link IdentityState} is updated to include / update {@link IdentityGroup}.
   *
   * @param group to sync
   * @param service to sync group with Google
   * @throws IOException if sync operation fails.
   */
  @Override
  public ListenableFuture<IdentityGroup> syncGroup(IdentityGroup group, IdentityService service)
      throws IOException {
    checkState(isRunning(), "state manager is not active");
    IdentityGroup previouslySynced = identityState.get().getGroup(group.getGroupKey());
    ListenableFuture<IdentityGroup> sync =
        callbackExecutor.submit(() -> group.sync(previouslySynced, service).get());
    return Futures.transform(
        sync,
        new Function<IdentityGroup, IdentityGroup>() {
          @Override
          @Nullable
          public IdentityGroup apply(@Nullable IdentityGroup result) {
            checkNotNull(result);
            identityState.get().putGroup(result);
            logger.log(Level.FINE, "Successfully synced identity group {0}", group.getGroupKey());
            return result;
          }
        },
        getExecutor());
  }

  /**
   * Sync all usersSyncedWithGoogle with Google
   *
   * @param allUsers to sync
   * @param service IdentityService to update user mapping
   * @throws IOException if sync fails
   */
  @Override
  public void syncAllUsers(
      Iterable<IdentityUser> allUsers, IdentityService service, ExceptionHandler exceptionHandler)
      throws IOException {
    checkState(isRunning(), "state manager is not active");
    Set<String> snapshot = new HashSet<>(identityState.get().getAllUserGoogleIdentities());
    AtomicInteger errorCounter = new AtomicInteger();
    for (Iterable<IdentityUser> partition : Iterables.partition(allUsers, partitionSize.get())) {
      List<ListenableFuture<IdentityUser>> usersFutures = new ArrayList<>();
      for (IdentityUser user : partition) {
        usersFutures.add(syncUser(user, service));
        snapshot.remove(user.getGoogleIdentity());
      }
      try {
        getCombinedFuture(usersFutures, exceptionHandler, errorCounter, getExecutor()).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while syncing users", e);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    }
    List<ListenableFuture<Boolean>> removeUsersFutures = new ArrayList<>();
    for (String missingInCurrentRun : snapshot) {
      removeUsersFutures.add(removeUser(missingInCurrentRun, service));
    }

    try {
      getCombinedFuture(removeUsersFutures, exceptionHandler, errorCounter, getExecutor()).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while removing users", e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  /**
   * Remove a user mapping from Google for specified Google identity. If {@link IdentityUser#unmap}
   * is successful, corresponding {@link IdentityUser} is removed from {@link IdentityState}.
   *
   * @param googleIdentity to unmap
   * @param service to unmap user identity
   * @throws IOException if unmap operation fails.
   */
  @Override
  public ListenableFuture<Boolean> removeUser(String googleIdentity, IdentityService service)
      throws IOException {
    checkState(isRunning(), "state manager is not active");
    IdentityUser identityUser = identityState.get().getUser(googleIdentity);
    if (identityUser == null) {
      return Futures.immediateFuture(true);
    }
    ListenableFuture<Boolean> unmap = identityUser.unmap(service);
    return Futures.transform(
        unmap,
        new Function<Boolean, Boolean>() {
          @Override
          @Nullable
          public Boolean apply(@Nullable Boolean result) {
            checkNotNull(result);
            if (result) {
              identityState.get().removeUser(googleIdentity);
              logger.log(Level.FINE, "Successfully unmapped identity user {0}", googleIdentity);
            } else {
              logger.log(Level.WARNING, "Failed to unmapped identity user {0}", googleIdentity);
            }
            return result;
          }
        },
        getExecutor());
  }

  @Override
  public void syncAllGroups(
      Iterable<IdentityGroup> allGroups, IdentityService service, ExceptionHandler exceptionHandler)
      throws IOException {
    checkState(isRunning(), "state manager is not active");
    Set<EntityKey> snapshot = new HashSet<>(identityState.get().getAllGroupKeys());
    AtomicInteger errorCounter = new AtomicInteger();
    for (Iterable<IdentityGroup> partition : Iterables.partition(allGroups, partitionSize.get())) {
      List<ListenableFuture<IdentityGroup>> groupsFutures = new ArrayList<>();
      for (IdentityGroup group : partition) {
        groupsFutures.add(syncGroup(group, service));
        snapshot.remove(group.getGroupKey());
      }
      try {
        getCombinedFuture(groupsFutures, exceptionHandler, errorCounter, getExecutor()).get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while syncing groups", e);
      } catch (ExecutionException e) {
        throw new IOException(e.getCause());
      }
    }
    List<ListenableFuture<Boolean>> removeGroupsFutures = new ArrayList<>();
    for (EntityKey missingInCurrentRun : snapshot) {
      removeGroupsFutures.add(removeGroup(missingInCurrentRun, service));
    }
    try {
      getCombinedFuture(removeGroupsFutures, exceptionHandler, errorCounter, getExecutor()).get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while removing groups", e);
    } catch (ExecutionException e) {
      throw new IOException(e.getCause());
    }
  }

  /**
   * Remove/ delete group from Google. If delete operation is successful, corresponding {@link
   * IdentityGroup} is removed from {@link IdentityState}.
   *
   * @param groupKey for group to be deleted.
   * @param service identity service to delete group.
   * @throws IOException if delete group operation fails.
   */
  @Override
  public ListenableFuture<Boolean> removeGroup(EntityKey groupKey, IdentityService service)
      throws IOException {
    checkState(isRunning(), "state manager is not active");
    IdentityGroup identityGroup = identityState.get().getGroup(groupKey);
    if (identityGroup == null) {
      return Futures.immediateFuture(true);
    }
    ListenableFuture<Boolean> unmap = identityGroup.unmap(service);
    return Futures.transform(
        unmap,
        new Function<Boolean, Boolean>() {

          @Override
          @Nullable
          public Boolean apply(@Nullable Boolean result) {
            checkNotNull(result);
            if (result) {
              identityState.get().removeGroup(groupKey);
              logger.log(Level.FINE, "Successfully unmapped identity group {0}", groupKey);
            } else {
              logger.log(Level.WARNING, "Failed to unmapped identity group {0}", groupKey);
            }
            return result;
          }
        },
        getExecutor());
  }
  /**
   * Add a member to previously synced group.
   *
   * @param groupKey group key for parent group.
   * @param memberKey member key to add member for
   * @param service identity service to create membership
   * @throws IOException if create membership fails
   */
  @Override
  public void addMember(EntityKey groupKey, EntityKey memberKey, IdentityService service)
      throws IOException {
    checkState(isRunning(), "state manager is not active");
    IdentityGroup identityGroup = identityState.get().getGroup(groupKey);
    if (identityGroup == null) {
      logger.log(Level.WARNING, "Group with Key {0} is not available in identity state", groupKey);
      return;
    }
    identityGroup.addMember(
        new Membership().setPreferredMemberKey(memberKey).setRoles(null), service);
  }

  /**
   * Remove a member from previously synced group.
   *
   * @param groupKey group key for parent group.
   * @param memberKey member key to remove member for
   * @param service identity service to delete membership
   * @throws IOException if delete membership fails
   */
  @Override
  public void removeMember(EntityKey groupKey, EntityKey memberKey, IdentityService service)
      throws IOException {
    checkState(isRunning(), "state manager is not active");
    IdentityGroup identityGroup = identityState.get().getGroup(groupKey);
    if (identityGroup == null) {
      logger.log(Level.WARNING, "Group with Key {0} is not available in identity state", groupKey);
      return;
    }
    identityGroup.removeMember(memberKey, service);
  }

  @Override
  public void close() throws IOException {
    MoreExecutors.shutdownAndAwaitTermination(callbackExecutor, 5, TimeUnit.MINUTES);
    isRunning.set(false);
  }

  @Override
  public void init(IdentityStateLoader identityStateLoader) throws IOException {
    IdentityState initialState = identityStateLoader.getInitialIdentityState();
    this.identityState.set(checkNotNull(initialState, "IdentityState can not be null"));
    this.isRunning.set(true);
  }

  private Executor getExecutor() {
    return callbackExecutor;
  }

  private boolean isRunning() {
    return isRunning.get();
  }

  private boolean checkIfUserExist(String googleIdentity, IdentityService service)
      throws IOException {
    ListenableFuture<User> lookupUser = service.getUserMapping(googleIdentity);
    try {
      User user =
          Futures.catchingAsync(
                  lookupUser,
                  GoogleJsonResponseException.class,
                  new AsyncFunction<GoogleJsonResponseException, User>() {

                    @Override
                    @Nullable
                    public ListenableFuture<User> apply(
                        @Nullable GoogleJsonResponseException input) {
                      checkNotNull(input);
                      logger.log(
                          Level.WARNING,
                          "Lookup request failed with error: {0}",
                          input.getDetails());
                      if (UNKNOWN_USER_ERROR_CODES.contains(input.getStatusCode())) {
                        logger.log(
                            Level.INFO,
                            String.format(
                                "User with email [%s] not found. Returning null.", googleIdentity),
                            input);
                        return Futures.immediateFuture(null);
                      }
                      return Futures.immediateFailedFuture(input);
                    }
                  },
                  getExecutor())
              .get();
      return user != null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while performing user lookup.", e);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IOException) {
        throw (IOException) e.getCause();
      }
      throw new IOException("Error performing user lookup.", e.getCause());
    }
  }

  private static <T> ListenableFuture<List<T>> getCombinedFuture(
      List<ListenableFuture<T>> futures,
      ExceptionHandler exceptionHandler,
      AtomicInteger errorCounter,
      Executor executor) {
    return Futures.whenAllComplete(futures)
        .call(
            () -> {
              List<T> result = new ArrayList<>();
              for (ListenableFuture<T> future : futures) {
                try {
                  result.add(future.get());
                } catch (ExecutionException e) {
                  int errorCount = errorCounter.incrementAndGet();
                  boolean continueOnError =
                      exceptionHandler.handleException((Exception) e.getCause(), errorCount);
                  if (continueOnError) {
                    logger.log(Level.INFO, "Continue to process after error# {0}", errorCount);
                    result.add(null);
                    continue;
                  } else {
                    logger.log(Level.WARNING, "Abort after error# {0}", errorCount);
                    throw (Exception) e.getCause();
                  }
                }
              }
              return result;
            },
            executor);
  }
}
