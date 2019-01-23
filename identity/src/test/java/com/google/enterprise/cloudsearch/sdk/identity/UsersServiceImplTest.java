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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.Directory.Users;
import com.google.api.services.admin.directory.model.User;
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
import java.util.Map;
import java.util.Optional;
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
public class UsersServiceImplTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock private Directory service;
  @Mock private Users users;

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final CredentialFactory credentialFactory =
      scopes -> new MockGoogleCredential.Builder()
          .setTransport(GoogleNetHttpTransport.newTrustedTransport())
          .setJsonFactory(JSON_FACTORY)
          .build();
  private BatchRequestService batchRequestService;
  private UsersServiceImpl usersService;

  @Before
  public void setup() throws Exception {
    Properties properties = new Properties();
    properties.put("api.identitySourceId", "idSource1");
    properties.put("api.customerId", "customer1");
    setupConfig.initConfig(properties);
    batchRequestService =
        spy(
            new BatchRequestService.Builder(service)
                .setGoogleCredential(credentialFactory.getCredential(Collections.emptyList()))
                .build());
    when(service.users()).thenReturn(users);
    usersService = getService();
  }

  @After
  public void shutdown() throws Exception {
    usersService.stopAsync().awaitTerminated();
  }

  @Test
  public void testFromConfiguration() throws GeneralSecurityException, IOException {
    UsersServiceImpl.fromConfiguration(credentialFactory);
  }

  @Test
  public void testBuilder() throws GeneralSecurityException, IOException {
    assertNotNull(
        new UsersServiceImpl.Builder()
            .setCredentialFactory(credentialFactory)
            .setBatchPolicy(BatchPolicy.fromConfiguration())
            .setCustomer("c1")
            .build());
  }

  @Test
  public void testBuilderNullCredentialFactory() throws GeneralSecurityException, IOException {
    thrown.expect(IllegalArgumentException.class);
    new UsersServiceImpl.Builder().setBatchPolicy(BatchPolicy.fromConfiguration()).build();
  }

  @Test
  public void testStartAndStop() throws GeneralSecurityException, IOException {
    BatchRequestService batchRequestService =
        new BatchRequestService.Builder(service)
            .setGoogleCredential(credentialFactory.getCredential(Collections.emptyList()))
            .build();
    UsersServiceImpl usersService =
        new UsersServiceImpl.Builder()
            .setBatchRequestService(batchRequestService)
            .setCredentialFactory(credentialFactory)
            .setCustomer("c1")
            .setBatchPolicy(BatchPolicy.fromConfiguration())
            .build();
    usersService.startAsync().awaitRunning();
    assertTrue(batchRequestService.isRunning());
    usersService.stopAsync().awaitTerminated();
    assertEquals(State.TERMINATED, batchRequestService.state());
  }

  @Test
  public void testGetUser() throws Exception {
    String userId = "user1@mydomain.com";
    Users.Get getRequest = mock(Users.Get.class);
    when(getRequest.setProjection("full")).thenReturn(getRequest);
    when(users.get(userId)).thenReturn(getRequest);
    User toReturn = new User().setPrimaryEmail(userId);
    doAnswer(
            invocation -> {
              AsyncRequest<User> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, toReturn);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<User> user = usersService.getUserMapping(userId);
    assertEquals(toReturn, user.get());
  }

  @Test
  public void testUpdateUserMappingWithValue() throws Exception {
    String userId = "user1@mydomain.com";
    Users.Update updateRequest = mock(Users.Update.class);
    Map<String, Map<String, Object>> schema =
        Collections.singletonMap("schema", Collections.singletonMap("attribute", "value"));
    when(users.update(userId, new User().setCustomSchemas(schema))).thenReturn(updateRequest);
    User toReturn = new User().setPrimaryEmail(userId);
    doAnswer(
            invocation -> {
              AsyncRequest<User> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, toReturn);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<User> user =
        usersService.updateUserMapping(userId, "schema", "attribute", Optional.of("value"));
    assertEquals(toReturn, user.get());
  }

  @Test
  public void testUpdateUserMappingWithoutValue() throws Exception {
    String userId = "user1@mydomain.com";
    Users.Update updateRequest = mock(Users.Update.class);
    Map<String, Map<String, Object>> schema =
        Collections.singletonMap("schema", Collections.singletonMap("attribute", ""));
    when(users.update(userId, new User().setCustomSchemas(schema))).thenReturn(updateRequest);
    User toReturn = new User().setPrimaryEmail(userId);
    doAnswer(
            invocation -> {
              AsyncRequest<User> input = invocation.getArgument(0);
              setAsyncRequestResponse(input, toReturn);
              return null;
            })
        .when(batchRequestService)
        .add(any());
    ListenableFuture<User> user =
        usersService.updateUserMapping(userId, "schema", "attribute", Optional.empty());
    assertEquals(toReturn, user.get());
  }

  @Test
  public void testListUsers() throws Exception {
    Users.List list1 = mock(Users.List.class);
    when(list1.setCustomer("customer1")).thenReturn(list1);
    when(list1.setCustomFieldMask("schema")).thenReturn(list1);
    when(list1.setProjection("custom")).thenReturn(list1);
    when(list1.setPageToken(null)).thenReturn(list1);

    User user1 = new User().setPrimaryEmail("user1@mydomain.com");
    when(list1.execute())
        .thenReturn(
            new com.google.api.services.admin.directory.model.Users()
                .setNextPageToken("nextPage")
                .setUsers(Collections.singletonList(user1)));

    Users.List list2 = mock(Users.List.class);
    when(list2.setCustomer("customer1")).thenReturn(list2);
    when(list2.setCustomFieldMask("schema")).thenReturn(list2);
    when(list2.setProjection("custom")).thenReturn(list2);
    when(list2.setPageToken("nextPage")).thenReturn(list2);

    User user2 = new User().setPrimaryEmail("user2@mydomain.com");
    when(list2.execute())
        .thenReturn(
            new com.google.api.services.admin.directory.model.Users()
                .setUsers(Collections.singletonList(user2)));
    when(users.list()).thenReturn(list1, list2);
    Iterable<User> actual = usersService.listUsers("schema");
    assertTrue(Iterables.elementsEqual(ImmutableList.of(user1, user2), actual));
  }

  private UsersServiceImpl getService() throws Exception {
    UsersServiceImpl usersService =
        new UsersServiceImpl.Builder()
            .setService(service)
            .setBatchRequestService(batchRequestService)
            .setCredentialFactory(credentialFactory)
            .setBatchPolicy(BatchPolicy.fromConfiguration())
            .setCustomer("customer1")
            .build();
    usersService.startAsync().awaitRunning();
    return usersService;
  }

  private static <T> void setAsyncRequestResponse(AsyncRequest<T> input, T response)
      throws IOException {
    SettableFutureCallback<T> callback = input.getCallback();
    callback.onStart();
    callback.onSuccess(response, new HttpHeaders());
  }
}
