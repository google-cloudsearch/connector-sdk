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
import com.google.api.services.cloudidentity.v1.CloudIdentity;
import com.google.api.services.cloudidentity.v1.CloudIdentity.Groups;
import com.google.api.services.cloudidentity.v1.CloudIdentity.Groups.Memberships;
import com.google.api.services.cloudidentity.v1.model.Group;
import com.google.api.services.cloudidentity.v1.model.ListMembershipsResponse;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.Operation;
import com.google.api.services.cloudidentity.v1.model.SearchGroupsResponse;
import com.google.common.base.Splitter;
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

/**
 * Wrapper for Cloud Identity Groups API for performing CRUD operations on Groups and Memberships.
 */
public class GroupsServiceImpl extends BaseApiService<CloudIdentity> implements GroupsService {

  private static final Set<String> APPLICATION_SCOPES =
      ImmutableSet.of("https://www.googleapis.com/auth/cloud-identity");
  private static final OperationStats GROUP_SERVICE_STATS =
      StatsManager.getComponent("GroupsService");
  private static final String SEARCH_QUERY_FORMAT =
      "parent == '%s' && 'system/groups/external' in labels";
  private static final String GROUP_RESOURCE_PART = "groups";
  private static final String MEMBERSHIPS_RESOURCE_PART = "memberships";
  private static final Splitter RESOURCE_NAME_SPLITTER = Splitter.on("/").omitEmptyStrings();
  private static final RateLimiter API_RATE_LIMITER = RateLimiter.create(10);


  private final BatchRequestService batchService;

  private GroupsServiceImpl(Builder builder) {
    super(builder);
    this.batchService = checkNotNull(builder.batchService);
  }

  /** Creates a {@link Group} using Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Operation> createGroup(Group group) throws IOException {
    Groups.Create create = service.groups().create(checkNotNull(group));
    return batchRequest(create, retryPolicy, batchService);
  }

  /** Gets a {@link Group} from Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Group> getGroup(String groupId) throws IOException {
    validateGroupResourceName(groupId);
    Groups.Get get = service.groups().get(groupId);
    return batchRequest(get, retryPolicy, batchService);
  }

  /** Deletes a {@link Group} using Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Operation> deleteGroup(String groupId) throws IOException {
    validateGroupResourceName(groupId);
    Groups.Delete delete = service.groups().delete(groupId);
    return batchRequest(delete, retryPolicy, batchService);
  }

  /**
   * Creates a {@link Membership} under group identified by {@code groupId} using Cloud Identity
   * Groups API.
   */
  @Override
  public ListenableFuture<Operation> createMembership(String groupId, Membership member)
      throws IOException {
    Memberships.Create create = service.groups().memberships().create(groupId, member);
    return batchRequest(create, retryPolicy, batchService);
  }

  /** Gets a {@link Membership} from Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Membership> getMembership(String memberId) throws IOException {
    validateMemberResourceName(memberId);
    Memberships.Get get = service.groups().memberships().get(memberId);
    return batchRequest(get, retryPolicy, batchService);
  }

  /** Deletes a {@link Membership} using Cloud Identity Groups API. */
  @Override
  public ListenableFuture<Operation> deleteMembership(String memberId) throws IOException {
    validateMemberResourceName(memberId);
    Memberships.Delete delete = service.groups().memberships().delete(memberId);
    return batchRequest(delete, retryPolicy, batchService);
  }

  /** List all {@link Group}s available under given {@code groupNamespace} */
  @Override
  public Iterable<Group> listGroups(String groupNamespace) throws IOException {
    return new SearchGroupsIterable(groupNamespace);
  }

  /** List all {@link Membership}s under given {@code groupId} */
  @Override
  public Iterable<Membership> listMembers(String groupId) throws IOException {
    validateGroupResourceName(groupId);
    return new ListMembersIterable(groupId);
  }

  /** Creates an instance of {@link GroupsServiceImpl} from connector configuration */
  public static GroupsServiceImpl fromConfiguration(CredentialFactory credentialFactory)
      throws GeneralSecurityException, IOException {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    return new GroupsServiceImpl.Builder()
        .setCredentialFactory(credentialFactory)
        .setBatchPolicy(BatchPolicy.fromConfiguration())
        .setRetryPolicy(RetryPolicy.fromConfiguration())
        .setProxy(GoogleProxy.fromConfiguration())
        .build();
  }

  /** Builder object for creating an instance of {@link GroupsServiceImpl} */
  public static class Builder extends BaseApiService.AbstractBuilder<Builder, CloudIdentity> {
    private BatchRequestService batchService;

    /**
     * Sets {@link BatchRequestService} to be used for request batching.
     *
     * @param batchService to be used for request batching.
     * @return this builder instance.
     */
    public Builder setBatchRequestService(BatchRequestService batchService) {
      this.batchService = batchService;
      return this;
    }

    /** Returns this instance of {@link GroupsServiceImpl.Builder} */
    @Override
    public Builder getThis() {
      return this;
    }

    /** Returns API scopes to be used for making Cloud Identity Groups API requests. */
    @Override
    public Set<String> getApiScopes() {
      return APPLICATION_SCOPES;
    }

    /**
     * Gets {@link com.google.api.services.cloudidentity.v1beta1.CloudIdentity.Builder} instance to
     * build {@link CloudIdentity} API client.
     */
    @Override
    public CloudIdentity.Builder getServiceBuilder(
        HttpTransport transport,
        JsonFactory jsonFactory,
        HttpRequestInitializer requestInitializer) {
      return new CloudIdentity.Builder(transport, jsonFactory, requestInitializer);
    }

    /** Builds an instance of {@link GroupsServiceImpl} */
    @Override
    public GroupsServiceImpl build() throws GeneralSecurityException, IOException {
      GoogleCredential credentials = setupServiceAndCredentials();
      if (batchService == null) {
        batchService =
            new BatchRequestService.Builder(service)
                .setBatchPolicy(batchPolicy)
                .setRetryPolicy(retryPolicy)
                .setGoogleCredential(credentials)
                .build();
      }
      return new GroupsServiceImpl(this);
    }
  }

  @Override
  protected void startUp() throws Exception {
    batchService.startAsync().awaitRunning();
  }

  @Override
  protected void shutDown() throws Exception {
    batchService.stopAsync().awaitTerminated();
  }

  private class SearchGroupsIterable extends PaginationIterable<Group, String> {

    private final String query;

    private SearchGroupsIterable(String namespace) {
      super(Optional.empty());
      this.query = String.format(SEARCH_QUERY_FORMAT, namespace);
    }

    @Override
    public Page<Group, String> getPage(Optional<String> nextPage) throws IOException {
      Groups.Search search = service.groups().search();
      search.setPageToken(nextPage.orElse(null));
      search.setQuery(query);
      API_RATE_LIMITER.acquire();
      SearchGroupsResponse response = executeRequest(search, GROUP_SERVICE_STATS, false);
      List<Group> toReturn = response.getGroups();
      return new Page<>(
          toReturn == null ? Collections.emptyList() : toReturn,
          Optional.ofNullable(response.getNextPageToken()));
    }
  }

  private class ListMembersIterable extends PaginationIterable<Membership, String> {
    private final String groupId;

    private ListMembersIterable(String groupId) {
      super(Optional.empty());
      this.groupId = groupId;
    }

    @Override
    public Page<Membership, String> getPage(Optional<String> nextPage) throws IOException {
      Memberships.List list = service.groups().memberships().list(groupId);
      list.setPageToken(nextPage.orElse(null));
      API_RATE_LIMITER.acquire();
      ListMembershipsResponse response = executeRequest(list, GROUP_SERVICE_STATS, false);
      List<Membership> memberships = response.getMemberships();
      return new Page<>(
          memberships == null ? Collections.emptyList() : memberships,
          Optional.ofNullable(response.getNextPageToken()));
    }
  }

  private static void validateGroupResourceName(String groupResourceName) {
    checkArgument(
        !Strings.isNullOrEmpty(groupResourceName), "group resource name can not be null or empty");
    List<String> parts = RESOURCE_NAME_SPLITTER.splitToList(groupResourceName);
    checkArgument(
        (parts.size() == 2) && GROUP_RESOURCE_PART.equals(parts.get(0)),
        "Invalid group resource name %s",
        groupResourceName);
  }

  private static void validateMemberResourceName(String membershipResourceName) {
    checkArgument(
        !Strings.isNullOrEmpty(membershipResourceName),
        "membership resource name can not be null or empty");
    List<String> parts = RESOURCE_NAME_SPLITTER.splitToList(membershipResourceName);
    checkArgument(
        (parts.size() == 4)
            && GROUP_RESOURCE_PART.equals(parts.get(0))
            && MEMBERSHIPS_RESOURCE_PART.equals(parts.get(2)),
        "Invalid membership resource name %s",
        membershipResourceName);
  }

  private static <T> ListenableFuture<T> batchRequest(
      AbstractGoogleJsonClientRequest<T> requestToExecute,
      RetryPolicy retryPolicy,
      BatchRequestService batchService)
      throws IOException {
    AsyncRequest<T> request =
        new AsyncRequest<>(requestToExecute, retryPolicy, GROUP_SERVICE_STATS);
    try {
      API_RATE_LIMITER.acquire();
      batchService.add(request);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while batching request", e);
    }
    return request.getFuture();
  }
}
