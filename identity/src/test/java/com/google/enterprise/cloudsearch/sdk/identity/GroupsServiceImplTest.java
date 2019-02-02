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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudidentity.v1.CloudIdentity;
import com.google.api.services.cloudidentity.v1.CloudIdentity.Groups;
import com.google.api.services.cloudidentity.v1.CloudIdentity.Groups.Memberships;
import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Group;
import com.google.api.services.cloudidentity.v1.model.ListMembershipsResponse;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.Operation;
import com.google.api.services.cloudidentity.v1.model.SearchGroupsResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service.State;
import com.google.enterprise.cloudsearch.sdk.AsyncRequest;
import com.google.enterprise.cloudsearch.sdk.AsyncRequest.SettableFutureCallback;
import com.google.enterprise.cloudsearch.sdk.BatchPolicy;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Properties;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GroupsServiceImplTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock private CloudIdentity service;
  @Mock private Groups groups;
  @Mock private Memberships memberships;

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final CredentialFactory credentialFactory =
      scopes -> new MockGoogleCredential.Builder()
          .setTransport(GoogleNetHttpTransport.newTrustedTransport())
          .setJsonFactory(JSON_FACTORY)
          .build();
  private BatchRequestService batchRequestService;
  private GroupsServiceImpl groupsService;

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    properties.put("api.identitySourceId", "idSource1");
    setupConfig.initConfig(properties);
    batchRequestService =
        spy(
            new BatchRequestService.Builder(service)
                .setGoogleCredential(credentialFactory.getCredential(Collections.emptyList()))
                .build());
    when(service.groups()).thenReturn(groups);
    when(groups.memberships()).thenReturn(memberships);
    groupsService = getService();
  }

  @After
  public void shutdown() throws Exception {
    groupsService.stopAsync().awaitTerminated();
  }

  @Test
  public void testFromConfiguration() throws GeneralSecurityException, IOException {
    GroupsServiceImpl.fromConfiguration(credentialFactory);
  }

  @Test
  public void testBuilder() throws GeneralSecurityException, IOException {
    new GroupsServiceImpl.Builder()
        .setCredentialFactory(credentialFactory)
        .setBatchPolicy(BatchPolicy.fromConfiguration())
        .build();
  }

  @Test
  public void testBuilderNullCredentialFactory() throws GeneralSecurityException, IOException {
    thrown.expect(IllegalArgumentException.class);
    new GroupsServiceImpl.Builder()
        .setBatchPolicy(BatchPolicy.fromConfiguration())
        .build();
  }

  @Test
  public void testStartAndStop() throws GeneralSecurityException, IOException {
    BatchRequestService batchRequestService =
        new BatchRequestService.Builder(service)
            .setGoogleCredential(credentialFactory.getCredential(Collections.emptyList()))
            .build();
    GroupsServiceImpl groupService =
        new GroupsServiceImpl.Builder()
            .setBatchRequestService(batchRequestService)
            .setCredentialFactory(credentialFactory)
            .setBatchPolicy(BatchPolicy.fromConfiguration())
            .build();
    groupService.startAsync().awaitRunning();
    assertTrue(batchRequestService.isRunning());
    groupService.stopAsync().awaitTerminated();
    assertEquals(State.TERMINATED, batchRequestService.state());
  }

  @Test
  public void testGetGroup() throws Exception {
    String groupId = "groups/id1";
    Groups.Get getRequest = mock(Groups.Get.class);
    when(groups.get(groupId)).thenReturn(getRequest);
    Group groupToReturn = new Group().setName(groupId);
    doAnswer(
            invocation -> {
              AsyncRequest<Group> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, groupToReturn);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<Group> group = groupsService.getGroup(groupId);
    assertEquals(groupToReturn, group.get());
  }

  @Test
  public void testCreateGroup() throws Exception {
    Group groupToCreate =
        new Group().setGroupKey(new EntityKey().setNamespace("ns").setId("group1"));
    Groups.Create request = mock(Groups.Create.class);
    when(groups.create(groupToCreate)).thenReturn(request);
    Operation operation = new Operation().setDone(true);
    doAnswer(
            invocation -> {
              AsyncRequest<Operation> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, operation);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<Operation> group = groupsService.createGroup(groupToCreate);
    assertEquals(operation, group.get());
  }

  @Test
  public void testDeleteGroup() throws Exception {
    String groupId = "groups/id1";
    Groups.Delete deleteRequest = mock(Groups.Delete.class);
    when(groups.delete(groupId)).thenReturn(deleteRequest);
    Operation operation = new Operation().setDone(true);
    doAnswer(
            invocation -> {
              AsyncRequest<Operation> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, operation);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<Operation> delete = groupsService.deleteGroup(groupId);
    assertEquals(operation, delete.get());
  }

  @Test
  public void testListGroups() throws Exception {
    String namespace = "ns";
    String query = "parent == 'ns' && 'system/groups/external' in labels";
    Groups.Search search1 = mock(Groups.Search.class);
    Group group1 = new Group().setName("groups/g1");
    SearchGroupsResponse page1 =
        new SearchGroupsResponse()
            .setGroups(Collections.singletonList(group1))
            .setNextPageToken("nextToken");
    when(search1.execute()).thenReturn(page1);
    Groups.Search search2 = mock(Groups.Search.class);
    Group group2 = new Group().setName("groups/g2");
    SearchGroupsResponse page2 =
        new SearchGroupsResponse()
            .setGroups(Collections.singletonList(group2))
            .setNextPageToken(null);
    when(search2.execute()).thenReturn(page2);
    when(groups.search()).thenReturn(search1, search2);
    ImmutableList<Group> expected = ImmutableList.of(group1, group2);
    assertTrue(Iterables.elementsEqual(expected, groupsService.listGroups(namespace)));
    verify(search1).setQuery(query);
    verify(search1).setPageToken(null);
    verify(search2).setQuery(query);
    verify(search2).setPageToken("nextToken");
  }

  @Test
  public void testGetMemberships() throws Exception {
    String memberId = "groups/g1/memberships/m1";
    Memberships.Get getRequest = mock(Memberships.Get.class);
    when(memberships.get(memberId)).thenReturn(getRequest);
    Membership toReturn = new Membership().setName(memberId);
    doAnswer(
            invocation -> {
              AsyncRequest<Membership> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, toReturn);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<Membership> member = groupsService.getMembership(memberId);
    assertEquals(toReturn, member.get());
  }

  @Test
  public void testDeleteMemberships() throws Exception {
    String memberId = "groups/g1/memberships/m1";
    Memberships.Delete deleteRequest = mock(Memberships.Delete.class);
    when(memberships.delete(memberId)).thenReturn(deleteRequest);
    Operation operation = new Operation().setDone(true);
    doAnswer(
            invocation -> {
              AsyncRequest<Operation> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, operation);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<Operation> member = groupsService.deleteMembership(memberId);
    assertEquals(operation, member.get());
  }

  @Test
  public void testCreateMemberships() throws Exception {
    String groupId = "groups/g1";
    Memberships.Create request = mock(Memberships.Create.class);
    Membership toCreate = new Membership();
    when(memberships.create(groupId, toCreate)).thenReturn(request);
    Operation operation = new Operation().setDone(true);
    doAnswer(
            invocation -> {
              AsyncRequest<Operation> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, operation);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<Operation> member = groupsService.createMembership(groupId, toCreate);
    assertEquals(operation, member.get());
  }

  @Test
  public void testListMemberships() throws Exception {
    Memberships.List list1 = mock(Memberships.List.class);
    Membership member1 = new Membership().setName("groups/g1/memberships/m1");
    ListMembershipsResponse page1 =
        new ListMembershipsResponse()
            .setMemberships(Collections.singletonList(member1))
            .setNextPageToken("nextToken");
    when(list1.execute()).thenReturn(page1);
    Memberships.List list2 = mock(Memberships.List.class);
    Membership member2 = new Membership().setName("groups/g1/memberships/m2");
    ListMembershipsResponse page2 =
        new ListMembershipsResponse()
            .setMemberships(Collections.singletonList(member2))
            .setNextPageToken(null);
    when(list2.execute()).thenReturn(page2);
    when(memberships.list("groups/g1")).thenReturn(list1, list2);
    ImmutableList<Membership> expected = ImmutableList.of(member1, member2);
    assertTrue(Iterables.elementsEqual(expected, groupsService.listMembers("groups/g1")));
    verify(list1).setPageToken(null);
    verify(list2).setPageToken("nextToken");
  }

  private GroupsServiceImpl getService() throws Exception {
    GroupsServiceImpl groupService =
        new GroupsServiceImpl.Builder()
            .setService(service)
            .setBatchRequestService(batchRequestService)
            .setCredentialFactory(credentialFactory)
            .setBatchPolicy(BatchPolicy.fromConfiguration())
            .build();
    groupService.startAsync().awaitRunning();
    return groupService;
  }

  private static <T> void setAsyncRequestResponse(AsyncRequest<T> input, T response)
      throws IOException {
    SettableFutureCallback<T> callback = input.getCallback();
    callback.onStart();
    callback.onSuccess(response, new HttpHeaders());
  }
}
