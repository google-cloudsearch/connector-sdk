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

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Iterables;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.util.Collections;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FullSyncIdentityConnectorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock private IdentityService mockIdentityService;
  @Mock private Repository mockIdentityRepository;
  @Mock private IdentityConnectorContext mockConnectorContext;
  @Captor private ArgumentCaptor<Iterable<IdentityUser>> usersCaptor;
  @Captor private ArgumentCaptor<Iterable<IdentityGroup>> groupsCaptor;
  private IdentitySourceConfiguration identitySourceConfiguration;

  @Before
  public void setup() {
    when(mockConnectorContext.getIdentityService()).thenReturn(mockIdentityService);
  }

  @Test
  public void testNullRepository() {
    thrown.expect(NullPointerException.class);
    new FullSyncIdentityConnector(null);
  }

  @Test
  public void testConstructorAndInit() throws Exception {
    initConfig(new Properties());
    IdentityConnector connector = new FullSyncIdentityConnector(mockIdentityRepository);
    connector.init(mockConnectorContext);
    verify(mockIdentityRepository).init(any());
  }

  @Test
  public void testTraverse() throws Exception {
    initConfig(new Properties());
    when(mockIdentityService.listGroups(identitySourceConfiguration.getGroupNamespace()))
        .thenReturn(Collections.emptyList());
    when(mockIdentityService.listUsers(identitySourceConfiguration.getIdentitySourceSchema()))
        .thenReturn(Collections.emptyList());
    StateManager stateManager = spy(new StateManagerImpl());
    doAnswer(
            invocation -> {
              Iterable<IdentityUser> users = invocation.getArgument(0);
              // This ensures that test iterate through all identities.
              Iterables.size(users);
              return null;
            })
        .when(stateManager)
        .syncAllUsers(usersCaptor.capture(), eq(mockIdentityService), any());
    doAnswer(
            invocation -> {
              Iterable<IdentityGroup> groups = invocation.getArgument(0);
              // This ensures that test iterate through all identities.
              Iterables.size(groups);
              return null;
            })
        .when(stateManager)
        .syncAllGroups(groupsCaptor.capture(), eq(mockIdentityService), any());
    TestIdentityRepository identityRepository =
        spy(
            new TestIdentityRepository.Builder("mydomain.com")
                .addUserPage("", "next", false, Collections.singletonList("user1"))
                .addGroupPage("", "", false, Collections.singletonList("group1"))
                .build());
    IdentityConnector connector =
        new FullSyncIdentityConnector(identityRepository, null, stateManager);
    connector.init(mockConnectorContext);
    connector.traverse();
    assertTrue(
        Iterables.elementsEqual(identityRepository.getAllUsersList(), usersCaptor.getValue()));
    assertTrue(
        Iterables.elementsEqual(identityRepository.getAllGroupsList(), groupsCaptor.getValue()));
    assertTrue(identityRepository.allGroupPagesClosed());
    connector.destroy();
    verify(stateManager).close();
    verify(identityRepository).close();
  }

  @Test
  public void testTraverseUsersOnly() throws Exception {
    Properties overrideDefaults = new Properties();
    overrideDefaults.put("connector.IdentitySyncType", "USERS");
    initConfig(overrideDefaults);
    when(mockIdentityService.listUsers(identitySourceConfiguration.getIdentitySourceSchema()))
        .thenReturn(Collections.emptyList());
    StateManager stateManager = spy(new StateManagerImpl());
    doAnswer(
            invocation -> {
              Iterable<IdentityUser> users = invocation.getArgument(0);
              // This ensures that test iterate through all identities.
              Iterables.size(users);
              return null;
            })
        .when(stateManager)
        .syncAllUsers(usersCaptor.capture(), eq(mockIdentityService), any());
    TestIdentityRepository identityRepository =
        spy(
            new TestIdentityRepository.Builder("mydomain.com")
                .addUserPage("", "next", false, Collections.singletonList("user1"))
                .addGroupPage("", "", false, Collections.singletonList("group1"))
                .build());
    IdentityConnector connector =
        new FullSyncIdentityConnector(identityRepository, null, stateManager);
    connector.init(mockConnectorContext);
    connector.traverse();
    connector.destroy();
    verify(stateManager).close();
    verify(identityRepository).init(any());
    verify(identityRepository).listUsers(eq(null));
    verify(identityRepository).close();
    verifyNoMoreInteractions(identityRepository);
  }

  @Test
  public void testTraverseGroupsOnly() throws Exception {
    Properties overrideDefaults = new Properties();
    overrideDefaults.put("connector.IdentitySyncType", "GROUPS");
    initConfig(overrideDefaults);
    when(mockIdentityService.listGroups(identitySourceConfiguration.getGroupNamespace()))
        .thenReturn(Collections.emptyList());
    StateManager stateManager = spy(new StateManagerImpl());
    doAnswer(
            invocation -> {
              Iterable<IdentityGroup> groups = invocation.getArgument(0);
              // This ensures that test iterate through all identities.
              Iterables.size(groups);
              return null;
            })
        .when(stateManager)
        .syncAllGroups(groupsCaptor.capture(), eq(mockIdentityService), any());
    TestIdentityRepository identityRepository =
        spy(
            new TestIdentityRepository.Builder("mydomain.com")
                .addUserPage("", "next", false, Collections.singletonList("user1"))
                .addGroupPage("", "", false, Collections.singletonList("group1"))
                .build());
    IdentityConnector connector =
        new FullSyncIdentityConnector(identityRepository, null, stateManager);
    connector.init(mockConnectorContext);
    connector.traverse();
    connector.destroy();
    verify(stateManager).close();
    verify(identityRepository).init(any());
    verify(identityRepository).listGroups(eq(null));
    verify(identityRepository).close();
    verifyNoMoreInteractions(identityRepository);
  }

  private void initConfig(Properties overrideDefaults) {
    Properties properties = new Properties();
    properties.put("api.identitySourceId", "idSource1");
    properties.putAll(overrideDefaults);
    setupConfig.initConfig(properties);
    identitySourceConfiguration = IdentitySourceConfiguration.fromConfiguration();
  }
}
