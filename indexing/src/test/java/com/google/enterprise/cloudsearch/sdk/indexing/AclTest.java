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
package com.google.enterprise.cloudsearch.sdk.indexing;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.api.services.cloudsearch.v1.model.GSuitePrincipal;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl.InheritanceType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link Acl}. */

@RunWith(MockitoJUnitRunner.class)
public class AclTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testBuilderSimple() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    assertEquals(2, acl.getReaders().size());
    assertEquals(1, acl.getDeniedReaders().size());
    assertNull(acl.getInheritFrom());
  }

  @Test
  public void testBuilderSimpleWithInheritance() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .setInheritanceType(InheritanceType.CHILD_OVERRIDE)
        .setInheritFrom("parent", "fragment")
        .build();
    assertEquals(2, acl.getReaders().size());
    assertEquals(1, acl.getDeniedReaders().size());
    assertEquals("parent", acl.getInheritFrom());
    assertEquals("fragment", acl.getInheritFromFragment());
    assertEquals(InheritanceType.CHILD_OVERRIDE, acl.getInheritanceType());
  }

  @Test
  public void testBuilderSimpleWithBothPermitInheritance() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .setInheritanceType(InheritanceType.BOTH_PERMIT)
        .setInheritFrom("parent", "fragment")
        .build();
    assertEquals(2, acl.getReaders().size());
    assertEquals(1, acl.getDeniedReaders().size());
    assertEquals("parent", acl.getInheritFrom());
    assertEquals("fragment", acl.getInheritFromFragment());
    assertEquals(InheritanceType.BOTH_PERMIT, acl.getInheritanceType());
  }

  @Test
  public void testBuilderWithIncompleteInheritance() {
    Acl.Builder builder = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .setInheritFrom("parent", "fragment");
    thrown.expect(NullPointerException.class);
    builder.build();
  }

  @Test
  public void testBuilder_nullReader() {
    Acl.Builder builder = new Acl.Builder();
    thrown.expect(NullPointerException.class);
    builder.setReaders(Collections.singletonList(null));
  }

  @Test
  public void testBuilder_nullDeniedReader() {
    Acl.Builder builder = new Acl.Builder();
    thrown.expect(NullPointerException.class);
    builder.setDeniedReaders(Collections.singletonList(null));
  }

  @Test
  public void testBuilder_nullOwner() {
    Acl.Builder builder = new Acl.Builder();
    thrown.expect(NullPointerException.class);
    builder.setOwners(Collections.singletonList(null));
  }

  @Test
  public void testCreateFragmentItemOf() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    Item actual = acl.createFragmentItemOf("parent", "fragment");
    ItemAcl expectedAcl =
        new ItemAcl()
            .setReaders(
                Arrays.asList(
                    new Principal().setUserResourceName("AllowedUser1"),
                    new Principal().setUserResourceName("AllowedUser2")))
            .setDeniedReaders(
                Collections.singletonList(new Principal().setGroupResourceName("DeniedGroup")))
            .setOwners(
                Collections.singletonList(new Principal().setUserResourceName("AllowedUser1")));

    Item expected =
        new Item()
            .setName("parent#fragment")
            .setAcl(expectedAcl)
            .setItemType(ItemType.VIRTUAL_CONTAINER_ITEM.name());
    assertEquals(expected, actual);
  }

  @Test
  public void testApplyTo() {
    Item originItem = new Item().setName("testItem");
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    Item actual = acl.applyTo(originItem);

    ItemAcl expectedAcl =
        new ItemAcl()
            .setReaders(
                Arrays.asList(
                    new Principal().setUserResourceName("AllowedUser1"),
                    new Principal().setUserResourceName("AllowedUser2")))
            .setDeniedReaders(
                Collections.singletonList(new Principal().setGroupResourceName("DeniedGroup")))
            .setOwners(
                Collections.singletonList(new Principal().setUserResourceName("AllowedUser1")));

    Item expected = new Item().setName("testItem").setAcl(expectedAcl);
    assertEquals(expected, actual);
  }

  @Test
  public void testApplyTo_immutableReaders() {
    Item item = new Item().setName("testItem");
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    acl.applyTo(item);
    ItemAcl itemAcl = item.getAcl();
    itemAcl.getReaders().remove(0);
    itemAcl.getReaders().get(0).setUserResourceName("AllowedUser3");
    assertEquals(ImmutableList.copyOf(user("AllowedUser3")), itemAcl.getReaders());
    assertEquals(user("AllowedUser1", "AllowedUser2"), acl.getReaders());
  }

  @Test
  public void testApplyTo_immutableDeniedReaders() {
    Item item = new Item().setName("testItem");
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    acl.applyTo(item);
    ItemAcl itemAcl = item.getAcl();
    itemAcl.getDeniedReaders().remove(0);
    assertEquals(ImmutableList.of(), itemAcl.getDeniedReaders());
    assertEquals(group("DeniedGroup"), acl.getDeniedReaders());
  }

  @Test
  public void testApplyTo_immutableOwners() {
    Item item = new Item().setName("testItem");
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    acl.applyTo(item);
    ItemAcl itemAcl = item.getAcl();
    itemAcl.getOwners().get(0).setUserResourceName("AllowedUser3");
    assertEquals(ImmutableList.copyOf(user("AllowedUser3")), itemAcl.getOwners());
    assertEquals(user("AllowedUser1"), acl.getOwners());
  }

  @Test
  public void testFragmentId() {
    assertEquals("id#fragment", Acl.fragmentId("id", "fragment"));
    assertEquals("id", Acl.fragmentId("id", null));
    assertEquals("iTem#frG", Acl.fragmentId("iTem", "frG"));
  }

  @Test
  public void testFragmentIdWithNull() {
    thrown.expect(NullPointerException.class);
    Acl.fragmentId(null, "frG");
  }

  @Test
  public void testGetPrincipalName() {
    assertEquals("namespace:name", Acl.getPrincipalName("name", "namespace"));
    assertEquals("name", Acl.getPrincipalName("name", null));
    assertEquals("nS:namE", Acl.getPrincipalName("namE", "nS"));
  }

  @Test
  public void testGetPrincipalNameWithNull() {
    thrown.expect(NullPointerException.class);
    Acl.getPrincipalName(null, "namespace");
  }

  @Test
  public void testGetGoogleUserPrincipal() {
    Principal principal = Acl.getGoogleUserPrincipal("userId");
    assertEquals("userId", principal.getGsuitePrincipal().getGsuiteUserEmail());
  }

  @Test
  public void testGetGoogleGroupPrincipal() {
    Principal principal = Acl.getGoogleGroupPrincipal("groupId");
    assertEquals("groupId", principal.getGsuitePrincipal().getGsuiteGroupEmail());
  }

  @Test
  public void testGetCustomerPrincipal() {
    Principal principal = Acl.getCustomerPrincipal();
    assertTrue(principal.getGsuitePrincipal().getGsuiteDomain());
  }

  @Test
  public void testGetUserPrincipal() {
    Principal principal = Acl.getUserPrincipal("userId");
    assertEquals("userId", principal.getUserResourceName());
  }

  @Test
  public void testGetUserPrincipalWithIdentitySourceId() {
    Principal principal = Acl.getUserPrincipal("userId", "idSource1");
    assertEquals(
        new Principal().setUserResourceName("identitysources/idSource1/users/userId"), principal);
  }

  @Test
  public void testGetGroupPrincipal() {
    Principal principal = Acl.getGroupPrincipal("groupId");
    assertEquals("groupId", principal.getGroupResourceName());
  }

  @Test
  public void testGetGroupPrincipalWithIdentitySourceId() {
    Principal principal = Acl.getGroupPrincipal("groupId", "idSource1");
    assertEquals(
        new Principal().setGroupResourceName("identitysources/idSource1/groups/groupId"),
        principal);
  }

  @Test
  public void testGetGroupPrincipalEncoded() {
    Principal principal = Acl.getGroupPrincipal("group Id");
    assertEquals("group%20Id", principal.getGroupResourceName());
  }

  @Test
  public void addResourcePrefixUser() {
    Principal principal = Acl.getUserPrincipal("john doe");
    String before = principal.toString();
    assertTrue(before, Acl.addResourcePrefixUser(principal, "foo"));
    String after = principal.toString();
    assertThat(after, not(equalTo(before)));
    assertFalse(after, Acl.addResourcePrefixUser(principal, "foo"));
    assertEquals(after, principal.toString());
  }

  @Test
  public void addResourcePrefixGroup() {
    Principal principal = Acl.getGroupPrincipal("john doe");
    String before = principal.toString();
    assertTrue(before, Acl.addResourcePrefixGroup(principal, "foo"));
    String after = principal.toString();
    assertThat(after, not(equalTo(before)));
    assertFalse(after, Acl.addResourcePrefixGroup(principal, "foo"));
    assertEquals(after, principal.toString());
  }

  @Test
  public void getPrincipalType_googleUser() {
    Principal principal = Acl.getGoogleUserPrincipal("john doe");
    assertEquals(Acl.PrincipalType.GSUITE_USER, Acl.getPrincipalType(principal));
  }

  @Test
  public void getPrincipalType_googleGroup() {
    Principal principal = Acl.getGoogleGroupPrincipal("my friends");
    assertEquals(Acl.PrincipalType.GSUITE_GROUP, Acl.getPrincipalType(principal));
  }

  @Test
  public void getPrincipalType_googleDomain() {
    Principal principal = Acl.getCustomerPrincipal();
    assertEquals(Acl.PrincipalType.GSUITE_DOMAIN, Acl.getPrincipalType(principal));
  }

  @Test
  public void getPrincipalType_externalUser() {
    Principal principal = Acl.getUserPrincipal("john doe");
    assertEquals(Acl.PrincipalType.USER, Acl.getPrincipalType(principal));
  }

  @Test
  public void getPrincipalType_externalGroup() {
    Principal principal = Acl.getGroupPrincipal("john doe");
    assertEquals(Acl.PrincipalType.GROUP, Acl.getPrincipalType(principal));
  }

  @Test
  public void getPrincipalType_emptyPrincipal() {
    Principal principal = new Principal();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid principal");
    Acl.getPrincipalType(principal);
  }

  @Test
  public void getPrincipalType_emptyGSuitePrincipal() {
    Principal principal = new Principal().setGsuitePrincipal(new GSuitePrincipal());
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid principal");
    Acl.getPrincipalType(principal);
  }

  @Test
  public void testEqualsNegative() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1")).build();
    assertEquals(acl, acl);
    assertFalse(acl.equals(null));
    assertFalse(acl.equals(new Acl.Builder()));
  }

  @Test
  public void testApplyToNullItem() {
    Acl acl = new Acl.Builder().setReaders(Collections.singletonList(Acl.getCustomerPrincipal()))
        .build();
    Item item = null;
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("cannot be null"));
    acl.applyTo(item);
  }

  @Test
  public void testEquals() {
    Acl acl1 =
        new Acl.Builder()
            .setInheritFrom("inheritFrom")
            .setInheritanceType(InheritanceType.CHILD_OVERRIDE)
            .setOwners(Collections.singleton(new Principal().setUserResourceName("#owner")))
            .setReaders(
                Arrays.asList(
                    new Principal().setUserResourceName("#AllowedUser1"),
                    new Principal().setUserResourceName("#AllowedUser2")))
            .setDeniedReaders(
                Collections.singleton(new Principal().setGroupResourceName("#DeniedGroup")))
            .build();

    Acl acl2 =
        new Acl.Builder()
            .setInheritFrom("inheritFrom")
            .setInheritanceType(InheritanceType.CHILD_OVERRIDE)
            .setOwners(Collections.singleton(new Principal().setUserResourceName("#owner")))
            .setReaders(
                Arrays.asList(
                    new Principal().setUserResourceName("#AllowedUser2"),
                    new Principal().setUserResourceName("#AllowedUser2"),
                    new Principal().setUserResourceName("#AllowedUser1")))
            .setDeniedReaders(
                Collections.singleton(new Principal().setGroupResourceName("#DeniedGroup")))
            .build();
    assertEquals(acl1, acl2);
    assertEquals(acl1.hashCode(), acl2.hashCode());
  }

  @Test
  public void testCopyConstructor_equals() {
    Acl acl1 = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    Acl acl2 = new Acl.Builder(acl1).build();
    assertEquals(acl1, acl2);
    assertEquals(acl1.hashCode(), acl2.hashCode());
  }

  @Test
  public void testCopyConstructor_modified() {
    Acl acl1 = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    Acl acl2 = new Acl.Builder(acl1).setOwners(user("AllowedUser2")).build();
    assertThat(acl1, not(equalTo(acl2)));
    assertThat(acl1.hashCode(), not(equalTo(acl2.hashCode())));
  }

  @Test
  public void toString_minimal() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    String aclString = acl.toString();
    assertThat(aclString, startsWith("Acl ["));
    assertThat(aclString, containsString("AllowedUser1"));
    assertThat(aclString, containsString("AllowedUser2"));
    assertThat(aclString, containsString("DeniedGroup"));
  }

  @Test
  public void testGetReaders() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    assertEquals(user("AllowedUser1", "AllowedUser2"), acl.getReaders());
    thrown.expect(UnsupportedOperationException.class);
    acl.getReaders().addAll(group("AllowedGroup"));
  }

  @Test
  public void testGetDeniedReaders() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    assertEquals(group("DeniedGroup"), acl.getDeniedReaders());
    thrown.expect(UnsupportedOperationException.class);
    acl.getDeniedReaders().addAll(user("DeniedUser"));
  }

  @Test
  public void testGetOwners() {
    Acl acl = new Acl.Builder()
        .setReaders(user("AllowedUser1", "AllowedUser2"))
        .setDeniedReaders(group("DeniedGroup"))
        .setOwners(user("AllowedUser1"))
        .build();
    assertEquals(user("AllowedUser1"), acl.getOwners());

    ItemAcl itemAcl = acl.applyTo(new Item()).getAcl();
    itemAcl.getOwners().remove(0);
    acl.getOwners().iterator().next().setUserResourceName("AllowedUser3");
    assertEquals(user("AllowedUser1"), acl.getOwners());

    thrown.expect(UnsupportedOperationException.class);
    acl.getOwners().addAll(group("AllowedGroup"));
  }

  @Test
  public void testCreateAcl() {
    Acl targetAcl =
        new Acl.Builder()
            .setReaders(
                Arrays.asList(
                    Acl.getUserPrincipal("extUser1"),
                    Acl.getGoogleUserPrincipal("user1"),
                    Acl.getGroupPrincipal("extGroup1"),
                    Acl.getGoogleGroupPrincipal("group1")))
            .setDeniedReaders(
                Arrays.asList(
                    Acl.getUserPrincipal("deny1"),
                    Acl.getUserPrincipal("deny2"),
                    Acl.getGroupPrincipal("extDenyGroup1"),
                    Acl.getGoogleGroupPrincipal("denyGroup1")))
            .build();
    Acl acl = Acl.createAcl(" extUser1, google:user1", "extGroup1, google:group1 ",
        ", deny1, , deny2 , ", "extDenyGroup1, google:denyGroup1");
    assertEquals(targetAcl, acl);
  }

  @Test
  public void testEmptyReadersCreateAcl() {
    Acl targetAcl =
        new Acl.Builder()
            .setDeniedReaders(
                Arrays.asList(Acl.getUserPrincipal("deny1"), Acl.getUserPrincipal("deny2")))
            .build();
    Acl acl = Acl.createAcl(" ", " ", ", deny1, , deny2 , ", "");
    assertEquals(targetAcl, acl);
  }

  @Test
  public void testEmptyCreateAcl() {
    assertEquals(new Acl.Builder().build(), Acl.createAcl(" ", " ", " , ", ""));
  }

  private static ImmutableSet<Principal> user(String... names) {
  return Arrays.stream(names)
      .map(name -> new Principal().setUserResourceName(name))
      .collect(ImmutableSet.toImmutableSet());
  }

  private static ImmutableSet<Principal> group(String... names) {
  return Arrays.stream(names)
      .map(name -> new Principal().setGroupResourceName(name))
      .collect(ImmutableSet.toImmutableSet());
  }
}
