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

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Group;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.Operation;
import com.google.api.services.cloudidentity.v1.model.Status;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityPrincipal.Kind;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IdentityGroupTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock private IdentityService mockIdentityService;

  private static final EntityKey GROUP_KEY = new EntityKey().setId("group1").setNamespace("ns1");

  @Test
  public void testBuilderNullIdentity() {
    thrown.expect(IllegalArgumentException.class);
    new IdentityGroup.Builder().setGroupKey(GROUP_KEY).setMembers(Collections.emptySet()).build();
  }

  @Test
  public void testBuilderNullGroupKey() {
    thrown.expect(NullPointerException.class);
    new IdentityGroup.Builder()
        .setGroupIdentity("domain\\group1")
        .setGroupKey(null)
        .setMembers(Collections.emptySet())
        .build();
  }

  @Test
  public void testBuilderNullMembers() {
    Set<Membership> members = null;
    thrown.expect(NullPointerException.class);
    new IdentityGroup.Builder()
        .setGroupIdentity("domain\\group1")
        .setGroupKey(GROUP_KEY)
        .setMembers(members)
        .build();
  }

  @Test
  public void testBuilderNullMembersViaSupplier() {
    thrown.expect(NullPointerException.class);
    new IdentityGroup.Builder()
        .setGroupIdentity("domain\\group1")
        .setGroupKey(GROUP_KEY)
        .setMembers(() -> null)
        .build();
  }

  @Test
  public void testBuilder() {
    Set<Membership> members =
        Collections.singleton(
            new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com")));
    IdentityGroup group =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setMembers(members)
            .build();
    assertEquals("domain\\group1", group.getIdentity());
    assertEquals(GROUP_KEY, group.getGroupKey());
    assertTrue(Iterables.elementsEqual(members, group.getMembers()));
    assertEquals(Kind.GROUP, group.getKind());
  }

  @Test
  public void testUnmap() throws IOException, InterruptedException, ExecutionException {
    Set<Membership> members =
        Collections.singleton(
            new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com")));
    IdentityGroup group =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setMembers(members)
            .setGroupResourceName("groups/id1")
            .build();
    when(mockIdentityService.deleteGroup("groups/id1"))
        .thenReturn(Futures.immediateFuture(new Operation().setDone(true)));
    ListenableFuture<Boolean> unmap = group.unmap(mockIdentityService);
    assertTrue(unmap.get());
  }

  @Test
  public void testUnmapFails() throws IOException, InterruptedException, ExecutionException {
    Set<Membership> members =
        Collections.singleton(
            new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com")));
    IdentityGroup group =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setMembers(members)
            .setGroupResourceName("groups/id1")
            .build();
    when(mockIdentityService.deleteGroup("groups/id1"))
        .thenReturn(Futures.immediateFailedFuture(new IOException("error deleting group")));
    ListenableFuture<Boolean> unmap = group.unmap(mockIdentityService);
    thrown.expectCause(isA(IOException.class));
    unmap.get();
  }

  @Test
  public void testSyncPreviousNull() throws IOException, InterruptedException, ExecutionException {
    Membership member =
        new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com"));
    Membership memberToReturn =
        new Membership()
            .setPreferredMemberKey(new EntityKey().setId("user@domain.com"))
            .setName("groups/id1/memberships/member1");
    Set<Membership> members = Collections.singleton(member);
    IdentityGroup group =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setMembers(members)
            .build();
    Group groupToCreate =
        new Group()
            .setGroupKey(GROUP_KEY)
            .setLabels(IdentityGroup.GROUP_LABELS)
            .setParent(GROUP_KEY.getNamespace())
            .setDisplayName("domain\\group1");
    Group groupToReturn =
        new Group()
            .setName("groups/id1")
            .setGroupKey(GROUP_KEY)
            .setLabels(IdentityGroup.GROUP_LABELS)
            .setParent(GROUP_KEY.getNamespace())
            .setDisplayName("domain\\group1");
    IdentityGroup expcetd =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(Collections.singleton(memberToReturn))
            .build();
    when(mockIdentityService.createGroup(groupToCreate))
        .thenReturn(
            Futures.immediateFuture(new Operation().setDone(true).setResponse(groupToReturn)));
    when(mockIdentityService.createMembership("groups/id1", member))
        .thenReturn(
            Futures.immediateFuture(new Operation().setDone(true).setResponse(memberToReturn)));
    ListenableFuture<IdentityGroup> synced = group.sync(null, mockIdentityService);
    assertEquals(expcetd, synced.get());
  }

  @Test
  public void testSyncWithAdditionalMemberAndRemovedMember()
      throws IOException, InterruptedException, ExecutionException {
    Membership member =
        new Membership()
            .setName("groups/id1/memberships/existing")
            .setPreferredMemberKey(new EntityKey().setId("existing@domain.com"));
    Membership removed =
        new Membership()
            .setName("groups/id1/memberships/removed")
            .setPreferredMemberKey(new EntityKey().setId("removed@domain.com"));

    IdentityGroup previous =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, removed))
            .build();

    Membership added =
        new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com"));
    Membership memberToReturn =
        new Membership()
            .setPreferredMemberKey(new EntityKey().setId("user@domain.com"))
            .setName("groups/id1/memberships/added");
    IdentityGroup current =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, added))
            .build();

    IdentityGroup expected =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, memberToReturn))
            .build();

    when(mockIdentityService.createMembership("groups/id1", added))
        .thenReturn(
            Futures.immediateFuture(new Operation().setDone(true).setResponse(memberToReturn)));
    when(mockIdentityService.deleteMembership("groups/id1/memberships/removed"))
        .thenReturn(Futures.immediateFuture(new Operation().setDone(true)));
    ListenableFuture<IdentityGroup> synced = current.sync(previous, mockIdentityService);
    assertEquals(expected, synced.get());
  }

  @Test
  public void testSyncWithAdditionalMemberAndRemovedMemberFailsErrorStatus()
      throws IOException, InterruptedException, ExecutionException {
    Membership member =
        new Membership()
            .setName("groups/id1/memberships/existing")
            .setPreferredMemberKey(new EntityKey().setId("existing@domain.com"));
    Membership removed =
        new Membership()
            .setName("groups/id1/memberships/removed")
            .setPreferredMemberKey(new EntityKey().setId("removed@domain.com"));

    IdentityGroup previous =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, removed))
            .build();

    Membership added =
        new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com"));
    Membership memberToReturn =
        new Membership()
            .setPreferredMemberKey(new EntityKey().setId("user@domain.com"))
            .setName("groups/id1/memberships/added");
    IdentityGroup current =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, added))
            .build();

    IdentityGroup expected =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, removed, memberToReturn))
            .build();

    when(mockIdentityService.createMembership("groups/id1", added))
        .thenReturn(
            Futures.immediateFuture(new Operation().setDone(true).setResponse(memberToReturn)));
    when(mockIdentityService.deleteMembership("groups/id1/memberships/removed"))
        .thenReturn(
            Futures.immediateFuture(
                new Operation().setDone(true).setError(new Status().setCode(500))));
    ListenableFuture<IdentityGroup> synced = current.sync(previous, mockIdentityService);
    assertEquals(expected, synced.get());
  }

  @Test
  public void testSyncWithAdditionalMemberAndRemovedMemberFailsIOException()
      throws IOException, InterruptedException, ExecutionException {
    Membership member =
        new Membership()
            .setName("groups/id1/memberships/existing")
            .setPreferredMemberKey(new EntityKey().setId("existing@domain.com"));
    Membership removed =
        new Membership()
            .setName("groups/id1/memberships/removed")
            .setPreferredMemberKey(new EntityKey().setId("removed@domain.com"));

    IdentityGroup previous =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, removed))
            .build();

    Membership added =
        new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com"));
    Membership memberToReturn =
        new Membership()
            .setPreferredMemberKey(new EntityKey().setId("user@domain.com"))
            .setName("groups/id1/memberships/added");
    IdentityGroup current =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, added))
            .build();

    IdentityGroup expected =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, removed, memberToReturn))
            .build();

    when(mockIdentityService.createMembership("groups/id1", added))
        .thenReturn(
            Futures.immediateFuture(new Operation().setDone(true).setResponse(memberToReturn)));
    when(mockIdentityService.deleteMembership("groups/id1/memberships/removed"))
        .thenReturn(Futures.immediateFailedFuture(new IOException("error removing member")));
    ListenableFuture<IdentityGroup> synced = current.sync(previous, mockIdentityService);
    assertEquals(expected, synced.get());
  }

  @Test
  public void testAddMember() throws IOException, InterruptedException, ExecutionException {
    Membership member =
        new Membership()
            .setName("groups/id1/memberships/existing")
            .setPreferredMemberKey(new EntityKey().setId("existing@domain.com"));

    IdentityGroup identityGroup =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member))
            .build();
    Membership added =
        new Membership().setPreferredMemberKey(new EntityKey().setId("user@domain.com"));
    Membership memberToReturn =
        new Membership()
            .setPreferredMemberKey(new EntityKey().setId("user@domain.com"))
            .setName("groups/id1/memberships/added");
    IdentityGroup expected =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, memberToReturn))
            .build();
    when(mockIdentityService.createMembership("groups/id1", added))
        .thenReturn(
            Futures.immediateFuture(new Operation().setDone(true).setResponse(memberToReturn)));
    ListenableFuture<Membership> addMember = identityGroup.addMember(added, mockIdentityService);
    assertEquals(memberToReturn, addMember.get());
    assertEquals(expected, identityGroup);
  }

  @Test
  public void testAddExistingMember() throws IOException, InterruptedException, ExecutionException {
    Membership member =
        new Membership()
            .setName("groups/id1/memberships/existing")
            .setPreferredMemberKey(new EntityKey().setId("existing@domain.com"));

    IdentityGroup identityGroup =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member))
            .build();
    Membership added =
        new Membership().setPreferredMemberKey(new EntityKey().setId("existing@domain.com"));
    IdentityGroup expected =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member))
            .build();
    ListenableFuture<Membership> addMember = identityGroup.addMember(added, mockIdentityService);
    assertEquals(member, addMember.get());
    assertEquals(expected, identityGroup);
    verifyZeroInteractions(mockIdentityService);
  }

  @Test
  public void testRemoveMember() throws IOException, InterruptedException, ExecutionException {
    Membership member =
        new Membership()
            .setName("groups/id1/memberships/existing")
            .setPreferredMemberKey(new EntityKey().setId("existing@domain.com"));
    EntityKey memberKeyToRemove = new EntityKey().setId("removed@domain.com");
    Membership removed =
        new Membership()
            .setName("groups/id1/memberships/removed")
            .setPreferredMemberKey(memberKeyToRemove);

    IdentityGroup identityGroup =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member, removed))
            .build();
    IdentityGroup expected =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member))
            .build();
    when(mockIdentityService.deleteMembership("groups/id1/memberships/removed"))
        .thenReturn(Futures.immediateFuture(new Operation().setDone(true)));
    ListenableFuture<Boolean> removeMember =
        identityGroup.removeMember(
            memberKeyToRemove, mockIdentityService);
    assertTrue(removeMember.get());
    assertEquals(expected, identityGroup);
  }

  @Test
  public void testRemoveNonExistingMember()
      throws IOException, InterruptedException, ExecutionException {
    Membership member =
        new Membership()
            .setName("groups/id1/memberships/existing")
            .setPreferredMemberKey(new EntityKey().setId("existing@domain.com"));

    IdentityGroup identityGroup =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member))
            .build();
    EntityKey removed = new EntityKey().setId("nonexisting@domain.com");
    IdentityGroup expected =
        new IdentityGroup.Builder()
            .setGroupIdentity("domain\\group1")
            .setGroupKey(GROUP_KEY)
            .setGroupResourceName("groups/id1")
            .setMembers(ImmutableSet.of(member))
            .build();
    ListenableFuture<Boolean> removeMember =
        identityGroup.removeMember(removed, mockIdentityService);
    assertTrue(removeMember.get());
    assertEquals(expected, identityGroup);
    verifyZeroInteractions(mockIdentityService);
  }
}
