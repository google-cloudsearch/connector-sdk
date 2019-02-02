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
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException.Builder;
import com.google.api.services.admin.directory.model.User;
import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StateManagerImplTest {
  private static final EntityKey GROUP_KEY = new EntityKey().setId("group1").setNamespace("ns1");

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock private IdentityService mockIdentityService;
  @Mock private ExceptionHandler mockExceptionHandler;

  @Test
  public void syncIdentityUserFirstTime() throws Exception {
    IdentityState identityState =
        new IdentityState.Builder().addUnmappedUser("user1@domain.com").build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    IdentityUser delegate =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1")
            .build();
    IdentityUser identityUser = spy(delegate);
    setupSyncUser(null, identityUser);
    manager.syncUser(identityUser, mockIdentityService).get();
    assertEquals(
        ImmutableMap.of("user1@domain.com", Optional.of(delegate)),
        identityState.getUsersSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncIdentityUserUpdated() throws Exception {
    IdentityUser existing =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1")
            .build();
    IdentityState identityState = new IdentityState.Builder().addUser(existing).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    IdentityUser delegate =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1-updated")
            .build();
    IdentityUser identityUser = spy(delegate);
    setupSyncUser(existing, identityUser);
    manager.syncUser(identityUser, mockIdentityService).get();
    assertEquals(
        ImmutableMap.of("user1@domain.com", Optional.of(delegate)),
        identityState.getUsersSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncAllUsers() throws IOException, InterruptedException {
    setupConfig.initConfig(new Properties());
    IdentityUser existing =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1")
            .build();
    IdentityUser older =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("userupdated@domain.com")
            .setUserIdentity("domain\\older")
            .build();
    IdentityUser removed =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("removed@domain.com")
                .setUserIdentity("domain\\removed")
                .build());

    IdentityUser added =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("added@domain.com")
                .setUserIdentity("domain\\added")
                .build());
    IdentityUser updated =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("userupdated@domain.com")
                .setUserIdentity("domain\\updated")
                .build());

    IdentityUser error =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("error@domain.com")
                .setUserIdentity("domain\\error")
                .build());
    IdentityUser addedLater =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("addedLater@domain.com")
                .setUserIdentity("domain\\addedLater")
                .build());
    when(mockIdentityService.getUserMapping("addedLater@domain.com"))
        .thenReturn(Futures.immediateFuture(new User()));
    setupSyncUser(null, addedLater);

    IdentityUser userLookup404 =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("userLookup404@domain.com")
                .setUserIdentity("domain\\userLookup404")
                .build());
    GoogleJsonError looupError = new GoogleJsonError();
    looupError.setCode(404);
    looupError.setMessage("Not allowed to mess with admin user");

    GoogleJsonResponseException lookupException =
        new GoogleJsonResponseException(
            new Builder(looupError.getCode(), looupError.getMessage(), new HttpHeaders()),
            looupError);
    when(mockIdentityService.getUserMapping("userLookup404@domain.com"))
        .thenReturn(Futures.immediateFailedFuture(lookupException));

    IdentityState identityState =
        new IdentityState.Builder()
            .addUser(existing)
            .addUser(older)
            .addUser(removed)
            .addUnmappedUser("added@domain.com")
            .addUnmappedUser("error@domain.com")
            .build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    Collection<IdentityUser> allUsers =
        Arrays.asList(existing, added, updated, error, addedLater, userLookup404);
    setupSyncUser(null, added);
    setupSyncUser(older, updated);
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(true);
            })
        .when(removed)
        .unmap(eq(mockIdentityService));
    IOException userSyncError = new IOException("error syncing user");
    doAnswer(
            invocation -> {
              return Futures.immediateFailedFuture(userSyncError);
            })
        .when(error)
        .sync(eq(null), eq(mockIdentityService));
    doAnswer(invocation -> true).when(mockExceptionHandler).handleException(any(), eq(1));
    manager.syncAllUsers(allUsers, mockIdentityService, mockExceptionHandler);
    assertEquals(
        new ImmutableMap.Builder<>()
            .put("user1@domain.com", Optional.of(existing))
            .put("removed@domain.com", Optional.empty())
            .put("error@domain.com", Optional.empty())
            .put("added@domain.com", Optional.of(added))
            .put("userupdated@domain.com", Optional.of(updated))
            .put("addedLater@domain.com", Optional.of(addedLater))
            .build(),
        identityState.getUsersSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncAllUsersAbortOnError() throws IOException, InterruptedException {
    Properties properties = new Properties();
    properties.put(StateManagerImpl.CONFIG_TRAVERSE_PARTITION_SIZE, "2");
    setupConfig.initConfig(properties);
    IdentityUser error1 =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("error1@domain.com")
                .setUserIdentity("domain\\error1")
                .build());
    IdentityUser added =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("added@domain.com")
                .setUserIdentity("domain\\added")
                .build());
    IdentityUser anotherAdded =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("anotherAdded@domain.com")
                .setUserIdentity("domain\\anotherAdded")
                .build());

    IdentityUser error2 =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("error2@domain.com")
                .setUserIdentity("domain\\error2")
                .build());

    IdentityUser noInteraction =
        spy(
            new IdentityUser.Builder()
                .setAttribute("att")
                .setSchema("schema")
                .setGoogleIdentity("noInteraction@domain.com")
                .setUserIdentity("domain\\noInteraction")
                .build());

    IdentityState identityState =
        new IdentityState.Builder()
            .addUnmappedUser("error1@domain.com")
            .addUnmappedUser("error2@domain.com")
            .addUnmappedUser("anotherAdded@domain.com")
            .addUnmappedUser("added@domain.com")
            .addUnmappedUser("noInteraction@domain.com")
            .build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    Collection<IdentityUser> allUsers =
        Arrays.asList(error1, added, error2, anotherAdded, noInteraction);
    setupSyncUser(null, added);
    setupSyncUser(null, anotherAdded);
    IOException userSyncError = new IOException("error syncing user");
    doAnswer(
            invocation -> {
              return Futures.immediateFailedFuture(userSyncError);
            })
        .when(error1)
        .sync(eq(null), eq(mockIdentityService));
    doAnswer(
            invocation -> {
              return Futures.immediateFailedFuture(userSyncError);
            })
        .when(error2)
        .sync(eq(null), eq(mockIdentityService));
    doAnswer(invocation -> true).when(mockExceptionHandler).handleException(any(), eq(1));
    doAnswer(invocation -> false).when(mockExceptionHandler).handleException(any(), eq(2));
    try {
      manager.syncAllUsers(allUsers, mockIdentityService, mockExceptionHandler);
      fail("expected to fail with IOException");
    } catch (IOException expected) {
    }
    manager.close();
  }

  @Test
  public void removeExistingUser() throws Exception {
    IdentityUser delegate =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1-updated")
            .build();
    IdentityUser identityUser = spy(delegate);
    IdentityState identityState =
        new IdentityState.Builder().addUser(identityUser).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(true);
            })
        .when(identityUser)
        .unmap(eq(mockIdentityService));
    manager.removeUser("user1@domain.com", mockIdentityService).get();
    assertEquals(
        ImmutableMap.of("user1@domain.com", Optional.empty()),
        identityState.getUsersSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void removeNonExistingUser() throws Exception {
    IdentityUser delegate =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1-updated")
            .build();
    IdentityState identityState = new IdentityState.Builder().addUser(delegate).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    manager.removeUser("usernonexisting@domain.com", mockIdentityService).get();
    assertEquals(
        ImmutableMap.of("user1@domain.com", Optional.of(delegate)),
        identityState.getUsersSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncIdentityUserUpdatedFails() throws IOException {
    IdentityUser existing =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1")
            .build();
    IdentityState identityState = new IdentityState.Builder().addUser(existing).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    IdentityUser delegate =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1-updated")
            .build();
    IdentityUser identityUser = spy(delegate);
    doAnswer(
            invocation -> {
              return Futures.immediateFailedFuture(new IOException("error updating user mapping"));
            })
        .when(identityUser)
        .sync(eq(existing), eq(mockIdentityService));
    manager.syncUser(identityUser, mockIdentityService);
    assertEquals(
        ImmutableMap.of("user1@domain.com", Optional.of(existing)),
        identityState.getUsersSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncIdentityUserAgain() throws Exception {
    IdentityUser existing =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1")
            .build();
    IdentityState identityState = new IdentityState.Builder().addUser(existing).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    IdentityUser identityUser =
        new IdentityUser.Builder()
            .setAttribute("att")
            .setSchema("schema")
            .setGoogleIdentity("user1@domain.com")
            .setUserIdentity("domain\\user1")
            .build();
    assertEquals(identityUser, manager.syncUser(identityUser, mockIdentityService).get());
    assertEquals(
        ImmutableMap.of("user1@domain.com", Optional.of(existing)),
        identityState.getUsersSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncIdentityGroupFirstTime() throws Exception {
    IdentityState identityState = new IdentityState.Builder().build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    IdentityGroup delegate =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of())
            .build();
    IdentityGroup identityGroup = spy(delegate);
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(delegate);
            })
        .when(identityGroup)
        .sync(eq(null), eq(mockIdentityService));
    assertEquals(delegate, manager.syncGroup(identityGroup, mockIdentityService).get());
    assertEquals(ImmutableMap.of(GROUP_KEY, delegate), identityState.getGroupsSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncIdentityGroupAgainNoChange() throws Exception {
    IdentityGroup existing =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of())
            .build();
    IdentityState identityState = new IdentityState.Builder().addGroup(existing).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    IdentityGroup identityGroup =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setMembers(ImmutableSet.of())
            .build();
    assertEquals(existing, manager.syncGroup(identityGroup, mockIdentityService).get());
    assertEquals(ImmutableMap.of(GROUP_KEY, existing), identityState.getGroupsSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncIdentityGroupWithAdditionalMember() throws Exception {
    IdentityGroup existing =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of())
            .build();
    IdentityState identityState = new IdentityState.Builder().addGroup(existing).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    Membership member =
        new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com"));
    Membership memberToReturn =
        new Membership()
            .setPreferredMemberKey(new EntityKey().setId("user@domain.com"))
            .setName("groups/id1/memberships/member1");
    IdentityGroup delegate =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setMembers(ImmutableSet.of(member))
            .build();
    IdentityGroup identityGroupToReturn =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(memberToReturn))
            .build();

    IdentityGroup identityGroup = spy(delegate);
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(identityGroupToReturn);
            })
        .when(identityGroup)
        .sync(eq(existing), eq(mockIdentityService));

    assertEquals(
        identityGroupToReturn, manager.syncGroup(identityGroup, mockIdentityService).get());
    assertEquals(
        ImmutableMap.of(GROUP_KEY, identityGroupToReturn),
        identityState.getGroupsSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncIdentityGroupFailsNoUpdateToState() throws IOException {
    IdentityGroup existing =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of())
            .build();
    IdentityState identityState = new IdentityState.Builder().addGroup(existing).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    Membership member =
        new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com"));
    IdentityGroup delegate =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setMembers(ImmutableSet.of(member))
            .build();

    IdentityGroup identityGroup = spy(delegate);
    doAnswer(
            invocation -> {
              return Futures.immediateFailedFuture(new IOException("error syncing group"));
            })
        .when(identityGroup)
        .sync(eq(existing), eq(mockIdentityService));

    manager.syncGroup(identityGroup, mockIdentityService);
    assertEquals(ImmutableMap.of(GROUP_KEY, existing), identityState.getGroupsSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void removeExistingGroup() throws Exception {
    IdentityGroup delegate =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of())
            .build();
    IdentityGroup identityGroup = spy(delegate);
    IdentityState identityState = new IdentityState.Builder().addGroup(identityGroup).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(true);
            })
        .when(identityGroup)
        .unmap(eq(mockIdentityService));
    manager.removeGroup(GROUP_KEY, mockIdentityService).get();
    assertEquals(ImmutableMap.of(), identityState.getGroupsSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void removeNonExistingGroup() throws IOException {
    IdentityGroup existing =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of())
            .build();
    IdentityState identityState = new IdentityState.Builder().addGroup(existing).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    manager.removeGroup(new EntityKey().setId("unknown").setNamespace("ns1"), mockIdentityService);
    assertEquals(ImmutableMap.of(GROUP_KEY, existing), identityState.getGroupsSyncedWithGoogle());
    manager.close();
  }

  @Test
  public void syncAllGroups() throws IOException {
    setupConfig.initConfig(new Properties());
    IdentityGroup existing =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of())
            .build();
    Membership member =
        new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com"));
    EntityKey olderKey = new EntityKey().setNamespace("ns1").setId("older");
    IdentityGroup older =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(olderKey)
            .setMembers(ImmutableSet.of())
            .build();

    EntityKey addedKey = new EntityKey().setNamespace("ns1").setId("added");
    IdentityGroup added =
        spy(
            new IdentityGroup.Builder()
                .setGroupIdentity("domain\\group1")
                .setGroupKey(addedKey)
                .setMembers(ImmutableSet.of(member))
                .build());

    IdentityGroup removed =
        spy(
            new IdentityGroup.Builder()
                .setGroupIdentity("domain\\group1")
                .setGroupKey(new EntityKey().setNamespace("ns1").setId("removed"))
                .setMembers(ImmutableSet.of(member))
                .build());

    IdentityGroup updated =
        spy(
            new IdentityGroup.Builder()
                .setGroupIdentity("domain\\group1")
                .setGroupKey(olderKey)
                .setMembers(ImmutableSet.of(member))
                .build());

    IdentityState identityState =
        new IdentityState.Builder().addGroup(existing).addGroup(removed).addGroup(older).build();
    StateManagerImpl manager = new StateManagerImpl();
    manager.init(() -> identityState);
    Collection<IdentityGroup> allGroups = Arrays.asList(existing, added, updated);
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(true);
            })
        .when(removed)
        .unmap(eq(mockIdentityService));
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(added);
            })
        .when(added)
        .sync(eq(null), eq(mockIdentityService));
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(updated);
            })
        .when(updated)
        .sync(eq(older), eq(mockIdentityService));

    manager.syncAllGroups(allGroups, mockIdentityService, mockExceptionHandler);
    assertEquals(
        ImmutableMap.of(GROUP_KEY, existing, addedKey, added, olderKey, updated),
        identityState.getGroupsSyncedWithGoogle());
    manager.close();
  }

  private void setupSyncUser(IdentityUser older, IdentityUser updated) throws IOException {
    doAnswer(
            invocation -> {
              return Futures.immediateFuture(updated);
            })
        .when(updated)
        .sync(eq(older), eq(mockIdentityService));
  }
}
