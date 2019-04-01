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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.enterprise.cloudsearch.sdk.AbortCountExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.PaginationIterable;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.Parser;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/** Connector implementation which periodically sync all users and groups with Google */
public class FullSyncIdentityConnector implements IdentityConnector {
  private static final Logger logger = Logger.getLogger(FullSyncIdentityConnector.class.getName());
  // TODO(tvartak) : Switch to TraverseExceptionHandlerFactory after moving from indexing SDK to
  // base SDK
  private static final String TRAVERSE_EXCEPTION_HANDLER = "traverse.exceptionHandler";
  private static final String IDENTITY_SYNC_TYPE = "connector.IdentitySyncType";
  private static final String IGNORE = "ignore";

  private final Repository identityRepository;
  private final Optional<IdentityStateLoader> identityStateLoader;
  private final StateManager stateManager;
  private ListeningExecutorService listeningExecutorService;
  private RepositoryContext repositoryContext;
  private IdentityService service;
  private ExceptionHandler traversalExceptionHandler;
  private IdentitySyncType identitySyncType;

  private enum IdentitySyncType {
    USERS_AND_GROUPS(true, true),
    USERS(true, false),
    GROUPS(false, true);

    final boolean syncUsers;
    final boolean syncGroups;

    private IdentitySyncType(boolean syncUsers, boolean syncGroups) {
      this.syncUsers = syncUsers;
      this.syncGroups = syncGroups;
    }
  }

  /**
   * Creates an instance of {@link FullSyncIdentityConnector} for syncing identities from supplied
   * identity {@link Repository}.
   *
   * @param identityRepository implementation for fetching users and groups from identity
   *     repository.
   */
  public FullSyncIdentityConnector(Repository identityRepository) {
    this(identityRepository, null);
  }

  /**
   * Creates an instance of {@link FullSyncIdentityConnector} for syncing identities from supplied
   * identity {@link Repository}.
   *
   * @param identityRepository implementation for fetching users and groups from identity
   *     repository.
   * @param identityStateLoader to load previously synced identities with Google APIs.
   */
  public FullSyncIdentityConnector(
      Repository identityRepository, IdentityStateLoader identityStateLoader) {
    this(identityRepository, identityStateLoader, new StateManagerImpl());
  }

  @VisibleForTesting
  FullSyncIdentityConnector(
      Repository identityRepository,
      IdentityStateLoader identityStateLoader,
      StateManager stateManager) {
    this.identityRepository = checkNotNull(identityRepository);
    this.identityStateLoader = Optional.ofNullable(identityStateLoader);
    this.stateManager = checkNotNull(stateManager);
  }

  @Override
  public void init(IdentityConnectorContext context) throws Exception {
    service = checkNotNull(context.getIdentityService());
    repositoryContext = RepositoryContext.fromConfiguration();
    identitySyncType =
        Configuration.getValue(
                IDENTITY_SYNC_TYPE, IdentitySyncType.USERS_AND_GROUPS, IdentitySyncType::valueOf)
            .get();
    logger.log(Level.CONFIG, "Identity Connector configured to sync [{0}]", identitySyncType);
    ExecutorService executor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder()
                .setDaemon(false)
                .setNameFormat("FullSyncIdentityConnector")
                .build());
    listeningExecutorService = MoreExecutors.listeningDecorator(executor);
    stateManager.init(
        identityStateLoader.orElse(
            new FromGoogleIdentityStateLoader(
                service,
                repositoryContext.getIdentitySourceConfiguration(),
                listeningExecutorService,
                identitySyncType)));
    identityRepository.init(repositoryContext);
    traversalExceptionHandler =
        Configuration.getValue(
                TRAVERSE_EXCEPTION_HANDLER,
                new AbortCountExceptionHandler(0, 0, TimeUnit.SECONDS),
                new ExceptionHandlerParser())
            .get();
  }

  /** Traverses {@link IdentityUser}s and {@link IdentityGroup}s from repository. */
  @Override
  public void traverse() throws IOException, InterruptedException {
    if (identitySyncType.syncUsers) {
      traverseUsers();
    } else {
      logger.log(
          Level.INFO,
          "Connector is not configured to sync users. IdentitySyncType is [{0}]",
          identitySyncType);
    }

    if (identitySyncType.syncGroups) {
      traverseGroups();
    } else {
      logger.log(
          Level.INFO,
          "Connector is not configured to sync Groups. IdentitySyncType is [{0}]",
          identitySyncType);
    }
  }

  @Override
  public void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException {
  }

  /** Releases resources and shuts down worker processes. */
  @Override
  public void destroy() {
    identityRepository.close();
    try {
      stateManager.close();
    } catch (IOException e) {
      logger.log(Level.WARNING, "Failed to close identity state sucessfully", e);
    }
    if (listeningExecutorService != null) {
      MoreExecutors.shutdownAndAwaitTermination(listeningExecutorService, 5, TimeUnit.MINUTES);
    }
  }

  private void traverseUsers() throws IOException {
    stateManager.syncAllUsers(
        new UsersIterable(identityRepository), service, traversalExceptionHandler);
  }

  private void traverseGroups() throws IOException {
    stateManager.syncAllGroups(
        new GroupsIterable(identityRepository), service, traversalExceptionHandler);
  }

  private static class UsersIterable extends PaginatingClosableIterable<IdentityUser> {

    private final Repository repository;

    public UsersIterable(Repository repository) {
      super(Optional.empty());
      this.repository = repository;
    }

    @Override
    CheckpointCloseableIterable<IdentityUser> getNextBatch(byte[] checkpoint) throws IOException {
      return repository.listUsers(checkpoint);
    }
  }

  private static class GroupsIterable extends PaginatingClosableIterable<IdentityGroup> {

    private final Repository repository;

    public GroupsIterable(Repository repository) {
      super(Optional.empty());
      this.repository = repository;
    }

    @Override
    CheckpointCloseableIterable<IdentityGroup> getNextBatch(byte[] checkpoint) throws IOException {
      return repository.listGroups(checkpoint);
    }
  }

  private abstract static class PaginatingClosableIterable<T>
      extends PaginationIterable<T, byte[]> {

    public PaginatingClosableIterable(Optional<byte[]> startPage) {
      super(startPage);
    }

    @Override
    public Page<T, byte[]> getPage(Optional<byte[]> nextPage) throws IOException {
      try (CheckpointCloseableIterable<T> batch = getNextBatch(nextPage.orElse(null))) {
        List<T> current = new ArrayList<>();
        Iterables.addAll(current, batch);
        byte[] checkpoint = batch.getCheckpoint();
        Optional<byte[]> nextPageToken =
            batch.hasMore() ? Optional.of(checkpoint) : Optional.empty();
        return new Page<>(current, nextPageToken);
      }
    }

    abstract CheckpointCloseableIterable<T> getNextBatch(byte[] checkpoint) throws IOException;
  }

  private static class FromGoogleIdentityStateLoader implements IdentityStateLoader {
    private final IdentityService service;
    private final IdentitySourceConfiguration sourceConfiguration;
    private final ListeningExecutorService listeningExecutor;
    private final IdentitySyncType identitySyncType;

    private FromGoogleIdentityStateLoader(
        IdentityService service,
        IdentitySourceConfiguration sourceConfiguration,
        ListeningExecutorService listeningExecutor,
        IdentitySyncType identitySyncType) {
      this.service = service;
      this.sourceConfiguration = sourceConfiguration;
      this.listeningExecutor = listeningExecutor;
      this.identitySyncType = identitySyncType;
    }

    @Override
    public IdentityState getInitialIdentityState() throws IOException {
      logger.info("Initializing IdentityState");
      IdentityState.Builder builder = new IdentityState.Builder();
      if (identitySyncType.syncUsers) {
        for (User u : service.listUsers(sourceConfiguration.getIdentitySourceSchema())) {
          Optional<IdentityUser> identityUser = buildIdentityUser(u);
          if (identityUser.isPresent()) {
            builder.addUser(identityUser.get());
          } else {
            builder.addUnmappedUser(u.getPrimaryEmail());
          }
        }
      }

      if (identitySyncType.syncGroups) {
        List<ListenableFuture<IdentityGroup>> allGroups = new ArrayList<>();
        for (Group g : service.listGroups(sourceConfiguration.getGroupNamespace())) {
          allGroups.add(listeningExecutor.submit(() -> buildGroup(g)));
        }

        try {
          List<IdentityGroup> identityGroups = Futures.allAsList(allGroups).get();
          identityGroups.forEach(i -> builder.addGroup(i));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted while building StateManagerImpl", e);
        } catch (ExecutionException e) {
          throw new IOException(e.getCause());
        }
      }
      return builder.build();
    }

    private Optional<IdentityUser> buildIdentityUser(User u) {
      Map<String, Map<String, Object>> customSchemas = u.getCustomSchemas();
      if (customSchemas == null) {
        return Optional.empty();
      }
      Map<String, Object> values = customSchemas.get(sourceConfiguration.getIdentitySourceSchema());
      if (values == null) {
        return Optional.empty();
      }
      Object value = values.get(sourceConfiguration.getIdentitySourceSchemaAttribute());
      if (value == null) {
        return Optional.empty();
      }

      String userIdentity = value.toString();
      if (userIdentity.isEmpty()) {
        return Optional.empty();
      }

      return Optional.of(
          new IdentityUser.Builder()
              .setGoogleIdentity(u.getPrimaryEmail())
              .setSchema(sourceConfiguration.getIdentitySourceSchema())
              .setAttribute(sourceConfiguration.getIdentitySourceSchemaAttribute())
              .setUserIdentity(userIdentity)
              .build());
    }

    private IdentityGroup buildGroup(Group g) throws IOException {
      List<Membership> members = new ArrayList<>();
      Iterables.addAll(members, service.listMembers(g.getName()));
      logger.log(Level.INFO, "Created Identity Group for previously synced group {0}", g);
      return new IdentityGroup.Builder()
          .setGroupIdentity(g.getGroupKey().getId())
          .setGroupKey(g.getGroupKey())
          .setGroupResourceName(g.getName())
          .setMembers(ImmutableSet.copyOf(members))
          .build();
    }
  }

  // TODO(tvartak) : Switch to TraverseExceptionHandlerFactory after moving from indexing SDK to
  // base SDK
  private static class ExceptionHandlerParser implements Parser<ExceptionHandler> {

    @Override
    public ExceptionHandler parse(String value) throws InvalidConfigurationException {
      if (IGNORE.equalsIgnoreCase(value)) {
        return new AbortCountExceptionHandler(Integer.MAX_VALUE, 0, TimeUnit.SECONDS);
      }
      int exceptionCount;
      try {
        exceptionCount = Integer.parseInt(value);
      } catch (NumberFormatException e) {
        throw new InvalidConfigurationException(
            "Unrecognized value for traversal exception handler: " + value, e);
      }
      return new AbortCountExceptionHandler(exceptionCount, 0, TimeUnit.SECONDS);
    }
  }
}
