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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.Directory.Users;
import com.google.api.services.admin.directory.Directory.Users.Get;
import com.google.api.services.admin.directory.Directory.Users.Update;
import com.google.api.services.admin.directory.model.User;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.RateLimiter;
import com.google.enterprise.cloudsearch.sdk.AsyncRequest;
import com.google.enterprise.cloudsearch.sdk.BaseApiService;
import com.google.enterprise.cloudsearch.sdk.BatchPolicy;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.GoogleProxy;
import com.google.enterprise.cloudsearch.sdk.PaginationIterable;
import com.google.enterprise.cloudsearch.sdk.RetryPolicy;
import com.google.enterprise.cloudsearch.sdk.StatsManager;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class UsersServiceImpl extends BaseApiService<Directory> implements UsersService {

  private static final Set<String> APPLICATION_SCOPES =
      ImmutableSet.of("https://www.googleapis.com/auth/admin.directory.user");
  private static final OperationStats USER_SERVICE_STATS =
      StatsManager.getComponent("UsersService");
  private static final String CUSTOMER_ID_CONFIG = "api.customerId";
  private static final RateLimiter API_RATE_LIMITER = RateLimiter.create(10);

  private final BatchRequestService batchService;
  private final String customerId;

  private UsersServiceImpl(Builder builder) {
    super(builder);
    this.batchService = checkNotNull(builder.batchService);
    checkArgument(
        !Strings.isNullOrEmpty(builder.customerId), "customerId can not be null or empty");
    this.customerId = builder.customerId;
  }

  /** Creates an instance of {@link UsersServiceImpl} from connector configuration. */
  public static UsersServiceImpl fromConfiguration(CredentialFactory credentialFactory)
      throws GeneralSecurityException, IOException {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    return new UsersServiceImpl.Builder()
        .setCredentialFactory(credentialFactory)
        .setBatchPolicy(BatchPolicy.fromConfiguration())
        .setRetryPolicy(RetryPolicy.fromConfiguration())
        .setCustomer(Configuration.getString(CUSTOMER_ID_CONFIG, null).get())
        .setProxy(GoogleProxy.fromConfiguration())
        .build();
  }

  /** Builder for creating an instance of {@link UsersServiceImpl} */
  public static class Builder extends BaseApiService.AbstractBuilder<Builder, Directory> {

    private BatchRequestService batchService;
    private String customerId;

    /** Returns this {@link UsersServiceImpl.Builder} instance */
    @Override
    public Builder getThis() {
      return this;
    }

    /**
     * Sets {@link BatchRequestService} to be used for request batching.
     *
     * @param batchService to be used for request batching
     */
    public Builder setBatchRequestService(BatchRequestService batchService) {
      this.batchService = batchService;
      return this;
    }

    /**
     * Sets customer ID to be used for syncing user identities.
     *
     * @param customerId to be used for syncing user identities.
     */
    public Builder setCustomer(String customerId) {
      this.customerId = customerId;
      return this;
    }

    /** Returns API scopes to be use for making Google Admin SDK API requests. */
    @Override
    public Set<String> getApiScopes() {
      return APPLICATION_SCOPES;
    }

    /**
     * Gets an instance of {@link com.google.api.services.admin.directory.Directory.Builder} for
     * creating an instance of {@link Directory} to make Google Admin SDK API requests.
     */
    @Override
    public Directory.Builder getServiceBuilder(
        HttpTransport transport,
        JsonFactory jsonFactory,
        HttpRequestInitializer requestInitializer) {
      return new Directory.Builder(transport, jsonFactory, requestInitializer);
    }

    /** Builds an instance of {@link UsersServiceImpl} */
    @Override
    public UsersServiceImpl build() throws GeneralSecurityException, IOException {
      GoogleCredential credentials = setupServiceAndCredentials();
      if (batchService == null) {
        batchService =
            new BatchRequestService.Builder(service)
                .setBatchPolicy(batchPolicy)
                .setRetryPolicy(retryPolicy)
                .setGoogleCredential(credentials)
                .build();
      }

      return new UsersServiceImpl(this);
    }
  }

  /** Gets {@link User} from Google Admin SDK API. */
  @Override
  public ListenableFuture<User> getUserMapping(String userId) throws IOException {
    Get get = service.users().get(userId).setProjection("full");
    return batchRequest(get, retryPolicy, batchService);
  }

  /** Updates {@link User}'s custom schema attributes using Google Admin SDK API. */
  @Override
  public ListenableFuture<User> updateUserMapping(
      String userId, String schemaName, String attributeName, Optional<String> value)
      throws IOException {
    Update update =
        service
            .users()
            .update(
                userId,
                new User()
                    .setCustomSchemas(
                        Collections.singletonMap(
                            schemaName,
                            Collections.singletonMap(attributeName, value.orElse("")))));
    return batchRequest(update, retryPolicy, batchService);
  }

  /** Lists all {@link User}s using Google Admin SDK API. */
  @Override
  public Iterable<User> listUsers(String schemaName) throws IOException {
    return new UsersIterable(schemaName);
  }

  @Override
  protected void startUp() throws Exception {
    batchService.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    batchService.stopAsync().awaitTerminated();
  }

  private class UsersIterable extends PaginationIterable<User, String> {

    private final String schemaName;

    UsersIterable(String schemaName) {
      super(Optional.empty());
      this.schemaName = schemaName;
    }

    @Override
    public Page<User, String> getPage(Optional<String> nextPage) throws IOException {
      Users.List list =
          service
              .users()
              .list()
              .setCustomer(customerId)
              .setCustomFieldMask(schemaName)
              .setProjection("custom")
              .setPageToken(nextPage.orElse(null));
      API_RATE_LIMITER.acquire();
      com.google.api.services.admin.directory.model.Users users = list.execute();
      List<User> toReturn = users.getUsers();
      return new Page<>(
          toReturn == null ? Collections.emptyList() : toReturn,
          Optional.ofNullable(users.getNextPageToken()));
    }
  }

  private static <T> ListenableFuture<T> batchRequest(
      AbstractGoogleJsonClientRequest<T> requestToExecute,
      RetryPolicy retryPolicy,
      BatchRequestService batchService)
      throws IOException {
    AsyncRequest<T> userRequest =
        new AsyncRequest<>(requestToExecute, retryPolicy, USER_SERVICE_STATS);
    try {
      API_RATE_LIMITER.acquire();
      batchService.add(userRequest);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while batching request", e);
    }
    return userRequest.getFuture();
  }
}
