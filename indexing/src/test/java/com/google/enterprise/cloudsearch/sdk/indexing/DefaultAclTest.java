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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.api.services.cloudsearch.v1.model.Status;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl.InheritanceType;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import javax.annotation.Nullable;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link DefaultAcl}. */
@RunWith(MockitoJUnitRunner.class)
public class DefaultAclTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock private IndexingService indexingServiceMock;

  private static final String TEST_READER_USERS = "google:user1@mydomain.com,extUser1,extUser2";
  private static final String TEST_READER_GROUPS =
      "google:group1@mydomain.com,google:group2@mydomain.com,extGroup1,extGroup2";
  private static final String TEST_DENIED_USERS =
      "google:dUser1@other.com,google:dUser2@other.com,dExtUser1";
  private static final String TEST_DENIED_GROUPS = "google:dGroup1@other.com,dExtGroup1";

  private List<Principal> getTestReaders() {
    return Arrays.asList(
        Acl.getGoogleUserPrincipal("user1@mydomain.com"),
        Acl.getUserPrincipal("extUser1"),
        Acl.getUserPrincipal("extUser2"),
        Acl.getGoogleGroupPrincipal("group1@mydomain.com"),
        Acl.getGoogleGroupPrincipal("group2@mydomain.com"),
        Acl.getGroupPrincipal("extGroup1"),
        Acl.getGroupPrincipal("extGroup2"));
  }

  private List<Principal> getTestDenied() {
    return Arrays.asList(
        Acl.getGoogleUserPrincipal("dUser1@other.com"),
        Acl.getGoogleUserPrincipal("dUser2@other.com"),
        Acl.getUserPrincipal("dExtUser1"),
        Acl.getGoogleGroupPrincipal("dGroup1@other.com"),
        Acl.getGroupPrincipal("dExtGroup1"));
  }

  // return a mutable list
  private List<Principal> getExtraPrincipal(String name) {
    List<Principal> users = new ArrayList<>();
    users.add(Acl.getUserPrincipal(name));
    return users;
  }

  private Item defaultAclContainer;
  private Acl publicAcl;
  private Acl readersAcl;

  @Before
  public void setUp() throws IOException {
    defaultAclContainer = new IndexingItemBuilder(DefaultAcl.DEFAULT_ACL_NAME_DEFAULT)
        .setItemType(IndexingItemBuilder.ItemType.VIRTUAL_CONTAINER_ITEM)
        .setQueue(DefaultAcl.DEFAULT_ACL_QUEUE)
        .build();
    publicAcl = new Acl.Builder()
        .setReaders(Collections.singletonList(Acl.getCustomerPrincipal()))
        .build();
    readersAcl = new Acl.Builder()
        .setReaders(getTestReaders())
        .setDeniedReaders(getTestDenied())
        .build();
    doAnswer(
            invocation -> {
              SettableFuture<Operation> result = SettableFuture.create();
              result.set(new Operation().setDone(true));
              return result;
            })
        .when(indexingServiceMock)
        .indexItem(any(), eq(RequestMode.SYNCHRONOUS));
  }

  private Item getItemInheritFromDefaultAcl() {
    return getItemInheritFromDefaultAcl(null);
  }

  // returns an item with container inherited properties and optional owners
  private Item getItemInheritFromDefaultAcl(@Nullable List<Principal> owners) {
    Item targetItem = new Item().setName("test");
    Acl.Builder builder = new Acl.Builder()
        .setInheritFrom(DefaultAcl.DEFAULT_ACL_NAME_DEFAULT)
        .setInheritanceType(Acl.InheritanceType.PARENT_OVERRIDE);
    if (owners != null) {
      builder.setOwners(owners);
    }
    Acl inheritAcl = builder.build();
    inheritAcl.applyTo(targetItem);
    return targetItem;
  }

  private Item getItemWithPublicAcl() {
    return getItemWithPublicAcl(null);
  }

  private Item getItemWithPublicAcl(@Nullable List<Principal> owners) {
    Acl.Builder builder =
        new Acl.Builder().setReaders(ImmutableList.of(Acl.getCustomerPrincipal()));
    if (owners != null) {
      builder.setOwners(owners);
    }
    return builder.build().applyTo(new Item().setName("test"));
  }

  @Test
  public void testConfigConstants() {
    assertEquals("defaultAcl.mode", DefaultAcl.DEFAULT_ACL_MODE);
    assertEquals("defaultAcl.public", DefaultAcl.DEFAULT_ACL_PUBLIC);
    assertEquals("defaultAcl.readers.users", DefaultAcl.DEFAULT_ACL_READERS_USERS);
    assertEquals("defaultAcl.readers.groups", DefaultAcl.DEFAULT_ACL_READERS_GROUPS);
    assertEquals("defaultAcl.denied.users", DefaultAcl.DEFAULT_ACL_DENIED_USERS);
    assertEquals("defaultAcl.denied.groups", DefaultAcl.DEFAULT_ACL_DENIED_GROUPS);
    assertEquals("defaultAcl.name", DefaultAcl.DEFAULT_ACL_NAME);
    assertEquals("DEFAULT_ACL_VIRTUAL_CONTAINER", DefaultAcl.DEFAULT_ACL_NAME_DEFAULT);
    assertEquals("DEFAULT_ACL_VIRTUAL_CONTAINER_QUEUE", DefaultAcl.DEFAULT_ACL_QUEUE);
  }

  @Test
  public void testConstructorNoDefaultAcl() {
    setupConfig.initConfig(new Properties());
    DefaultAcl.fromConfiguration(indexingServiceMock);
    verifyNoMoreInteractions(indexingServiceMock);
  }

  @Test
  public void testConstructorPublicAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    setupConfig.initConfig(config);
    DefaultAcl.fromConfiguration(indexingServiceMock);

    publicAcl.applyTo(defaultAclContainer);
    verifyNoMoreInteractions(indexingServiceMock);
  }

  @Test
  public void testConstructorReadersAcl() throws IOException {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl.fromConfiguration(indexingServiceMock);

    readersAcl.applyTo(defaultAclContainer);
    verify(indexingServiceMock).indexItem(defaultAclContainer, RequestMode.SYNCHRONOUS);
  }

  @Test
  public void testConstructorCustomContainerName() throws IOException {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "google:user1@example.com");
    config.put(DefaultAcl.DEFAULT_ACL_NAME, "My Acl Container Name");
    setupConfig.initConfig(config);
    DefaultAcl.fromConfiguration(indexingServiceMock);

    defaultAclContainer.setName("My Acl Container Name");
    new Acl.Builder()
        .setReaders(ImmutableList.of(Acl.getGoogleUserPrincipal("user1@example.com")))
        .build()
        .applyTo(defaultAclContainer);
    verify(indexingServiceMock).indexItem(defaultAclContainer, RequestMode.SYNCHRONOUS);
  }

  @Test
  public void testNoDefaultAcl() {
    setupConfig.initConfig(new Properties());
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);
    assertFalse(defAcl.applyToIfEnabled(new Item()));
  }

  @Test
  public void testPublicAclFallbackNoAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    Item expectedItem = getItemWithPublicAcl();
    Item item = new Item().setName("test");
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testPublicAclFallbackWithOwner() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    Item expectedItem = getItemWithPublicAcl(getExtraPrincipal("myOwner"));
    Item item = new Item().setName("test");
    Acl itemAcl = new Acl.Builder().setOwners(getExtraPrincipal("myOwner")).build();
    itemAcl.applyTo(item);
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testPublicAclFallbackWithAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    // create a test item and a copy
    Acl itemAcl = new Acl.Builder()
        .setReaders(Collections.singletonList(Acl.getUserPrincipal("ext1")))
        .build();
    Item item = new Item().setName("test");
    itemAcl.applyTo(item);
    Item itemClone = item.clone();

    // verify that calling applyToIfEnabled didn't change the item
    boolean applyResult = defAcl.applyToIfEnabled(item);
    assertFalse(applyResult);
    assertEquals(itemClone, item);
  }

  public void testDefaultAclFallback() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    Item expectedItem = getItemInheritFromDefaultAcl();
    Item item = new Item().setName("test");
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testDefaultAclAppendNoAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "append");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    // "append" when there's no acl on the item uses inheritance
    Item expectedItem = getItemInheritFromDefaultAcl();
    Item item = new Item().setName("test");
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testDefaultAclAppendEmptyAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "append");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    // "append" when there's an empty acl on the item uses inheritance
    Item expectedItem = getItemInheritFromDefaultAcl();
    Item item = new Item().setName("test");
    Acl empty = new Acl.Builder().build();
    empty.applyTo(item);
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testDefaultAclAppendWithAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "append");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    Item item =
        new Item()
            .setName("test")
            .setAcl(
                new ItemAcl()
                    .setInheritAclFrom("parent item")
                    .setAclInheritanceType(InheritanceType.CHILD_OVERRIDE.name())
                    .setReaders(getExtraPrincipal("extraReader1"))
                    .setDeniedReaders(getExtraPrincipal("extraDenied1")));
    assertTrue(defAcl.applyToIfEnabled(item));

    // check the item's updated acl
    List<Principal> expectedReaders = new ArrayList<>(getExtraPrincipal("extraReader1"));
    expectedReaders.addAll(getTestReaders());
    List<Principal> expectedDenied = new ArrayList<>(getExtraPrincipal("extraDenied1"));
    expectedDenied.addAll(getTestDenied());

    assertEquals(expectedReaders, item.getAcl().getReaders());
    assertEquals(expectedDenied, item.getAcl().getDeniedReaders());
    assertEquals("CHILD_OVERRIDE", item.getAcl().getAclInheritanceType());
    assertEquals("parent item", item.getAcl().getInheritAclFrom());
    // Acl.applyTo replaces null with empty list
    assertEquals(Collections.emptyList(), item.getAcl().getOwners());
  }

  @Test
  public void testDefaultAclAppendWithAclNoInheritance() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "append");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    Item item =
        new Item()
            .setName("test")
            .setAcl(
                new ItemAcl()
                    .setReaders(getExtraPrincipal("extraReader1"))
                    .setDeniedReaders(getExtraPrincipal("extraDenied1")));
    assertTrue(defAcl.applyToIfEnabled(item));

    // check the item's updated acl
    List<Principal> expectedReaders = new ArrayList<>(getExtraPrincipal("extraReader1"));
    expectedReaders.addAll(getTestReaders());
    List<Principal> expectedDenied = new ArrayList<>(getExtraPrincipal("extraDenied1"));
    expectedDenied.addAll(getTestDenied());

    assertEquals(expectedReaders, item.getAcl().getReaders());
    assertEquals(expectedDenied, item.getAcl().getDeniedReaders());
    assertEquals(null, item.getAcl().getAclInheritanceType());
    assertEquals(null, item.getAcl().getInheritAclFrom());
    // Acl.applyTo replaces null with empty list
    assertEquals(Collections.emptyList(), item.getAcl().getOwners());
  }

  @Test
  public void testDefaultAclOverrideNoAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "override");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    // "override" uses inheritance
    Item expectedItem = getItemInheritFromDefaultAcl();
    Item item = new Item().setName("test");
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testDefaultAclOverrideEmptyAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "override");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    // "override" uses inheritance
    Item expectedItem = getItemInheritFromDefaultAcl();
    Item item = new Item().setName("test");
    Acl empty = new Acl.Builder().build();
    empty.applyTo(item);
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testDefaultAclOverrideWithAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "override");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_READER_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_READER_GROUPS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_DENIED_USERS);
    config.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_DENIED_GROUPS);
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    // switching the readers/denied to verify the change occurs
    Item expectedItem = getItemInheritFromDefaultAcl();
    Acl itemAcl = new Acl.Builder()
        .setReaders(getTestDenied())
        .setDeniedReaders(getTestReaders())
        .build();
    Item item = new Item().setName("test");
    itemAcl.applyTo(item);

    // "override" uses inheritance
    // verify that the items start out not equal before applying the default acl
    assertNotEquals(expectedItem, item);
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testNoneDefaultAcl() {
    DefaultAcl defAcl = new DefaultAcl.Builder()
        .setIndexingService(indexingServiceMock)
        .setMode(DefaultAclMode.NONE)
        .build();

    Acl itemAcl = new Acl.Builder()
        .setReaders(Collections.singletonList(Acl.getUserPrincipal("ext1")))
        .build();
    Item item = new Item().setName("test");
    itemAcl.applyTo(item);
    Item expectedItem = item.clone();

    assertFalse(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testNoneDefaultAclFromConfig() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "none");
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    Acl itemAcl = new Acl.Builder()
        .setReaders(Collections.singletonList(Acl.getUserPrincipal("ext1")))
        .build();
    Item item = new Item().setName("test");
    itemAcl.applyTo(item);
    Item expectedItem = item.clone();

    assertFalse(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }

  @Test
  public void testPublicWithDefaultAcl() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "extUser1");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("can not specify reader users if default acl is public"));
    DefaultAcl.fromConfiguration(indexingServiceMock);
  }

  @Test
  public void testDefaultAclWithNoneDefined() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("no principal specified for non public default ACL"));
    DefaultAcl.fromConfiguration(indexingServiceMock);
  }

  @Test
  public void testNullItem() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);
    Item item = null;
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("cannot be null"));
    defAcl.applyToIfEnabled(item);
  }

  @Test
  public void testNullItemWithDefaultDisabled() {
    setupConfig.initConfig(new Properties());
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);
    Item item = null;
    defAcl.applyToIfEnabled(item);
  }

  @Test
  public void testConfigurationNotInitalized() {
    thrown.expect(IllegalStateException.class);
    DefaultAcl.fromConfiguration(indexingServiceMock);
  }

  @Test
  public void testBadMode() {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "invalid");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(DefaultAcl.DEFAULT_ACL_MODE));
    DefaultAcl.fromConfiguration(indexingServiceMock);
  }

  @Test
  public void testBuildException() throws IOException {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "google:user1@example.com");
    setupConfig.initConfig(config);
    when(indexingServiceMock.indexItem(any(), any())).thenThrow(new IOException("Testing Error"));
    thrown.expect(StartupException.class);
    thrown.expectMessage(containsString("Unable to upload default ACL"));
    DefaultAcl.fromConfiguration(indexingServiceMock);
  }

  @Test
  public void testBuildExceptionOperationNotDone() throws IOException {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "google:user1@example.com");
    setupConfig.initConfig(config);
    SettableFuture<Operation> result = SettableFuture.create();
    result.set(new Operation().setDone(false));
    when(indexingServiceMock.indexItem(any(), any())).thenReturn(result);
    thrown.expect(StartupException.class);
    thrown.expectMessage("Unable to upload default ACL");
    DefaultAcl.fromConfiguration(indexingServiceMock);
  }

  @Test
  public void testBuildExceptionOperationError() throws IOException {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "google:user1@example.com");
    setupConfig.initConfig(config);
    SettableFuture<Operation> result = SettableFuture.create();
    result.set(new Operation().setDone(true).setError(new Status().setCode(500)));
    when(indexingServiceMock.indexItem(any(), any())).thenReturn(result);
    thrown.expect(StartupException.class);
    thrown.expectMessage("Unable to upload default ACL");
    DefaultAcl.fromConfiguration(indexingServiceMock);
  }

  @Test
  public void testBuildExceptionExecutionException() throws IOException {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "google:user1@example.com");
    setupConfig.initConfig(config);
    SettableFuture<Operation> result = SettableFuture.create();
    result.setException(new IOException("failed to create item"));
    when(indexingServiceMock.indexItem(any(), any())).thenReturn(result);
    thrown.expect(StartupException.class);
    thrown.expectMessage("Unable to upload default ACL");
    DefaultAcl.fromConfiguration(indexingServiceMock);
  }

  /* Variations on item acl to test fix for potential NullPointerException; API object
   * fields can be null. This isn't intended to test the results, just verify that no
   * exception was thrown.
   */
  @Test
  public void testIsAclEmpty() {
    assertTrue(DefaultAcl.isAclEmpty(
            new Item()));
    assertTrue(DefaultAcl.isAclEmpty(
            new Item().setAcl(new ItemAcl())));
    assertTrue(DefaultAcl.isAclEmpty(
            new Item().setAcl(new ItemAcl().setReaders(Collections.emptyList()))));
    assertTrue(DefaultAcl.isAclEmpty(
            new Item().setAcl(new ItemAcl().setDeniedReaders(Collections.emptyList()))));
  }

  /* Variations on item acl to test fix for potential NullPointerException; API object
   * fields can be null. This isn't intended to test the results, just verify that no
   * exception was thrown.
   */
  @Test
  public void testApplyToIfEnabledAppend() {
    Principal defaultReader = Acl.getUserPrincipal("default reader");
    Principal itemReader = Acl.getUserPrincipal("item reader");
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "append");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, defaultReader.getUserResourceName());
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    // The acl must not be empty for APPEND to be used.
    Item item = new Item().setAcl(new ItemAcl().setReaders(Lists.newArrayList(itemReader)));
    assertFalse(DefaultAcl.isAclEmpty(item));
    assertTrue(defAcl.applyToIfEnabled(item));

    item = new Item().setAcl(new ItemAcl().setDeniedReaders(Lists.newArrayList(itemReader)));
    assertFalse(DefaultAcl.isAclEmpty(item));
    assertTrue(defAcl.applyToIfEnabled(item));

    item =
        new Item()
            .setAcl(
                new ItemAcl()
                    .setInheritAclFrom("parent item")
                    .setOwners(ImmutableList.of())
                    .setReaders(ImmutableList.of(Acl.getGoogleUserPrincipal("user1")))
                    .setDeniedReaders(ImmutableList.of(Acl.getUserPrincipal("user2")))
                    .setOwners(ImmutableList.of(Acl.getUserPrincipal("owner")))
                    .setAclInheritanceType(InheritanceType.BOTH_PERMIT.name()));
    assertFalse(DefaultAcl.isAclEmpty(item));
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(
        new ItemAcl()
            .setInheritAclFrom("parent item")
            .setOwners(ImmutableList.of(Acl.getUserPrincipal("owner")))
            .setAclInheritanceType(InheritanceType.BOTH_PERMIT.name())
            .setReaders(ImmutableList.of(Acl.getGoogleUserPrincipal("user1"), defaultReader))
            .setDeniedReaders(ImmutableList.of(Acl.getUserPrincipal("user2"))),
        item.getAcl());
  }

  /* Variations on item acl to test fix for potential NullPointerException; API object
   * fields can be null. This isn't intended to test the results, just verify that no
   * exception was thrown.
   */
  @Test
  public void testApplyToIfEnabledAclWithNoOwner() throws IOException {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    setupConfig.initConfig(config);
    DefaultAcl defAcl = DefaultAcl.fromConfiguration(indexingServiceMock);

    Item expectedItem = getItemWithPublicAcl();
    // This item has an acl but getOwners will return null.
    Item item = new Item().setName("test").setAcl(new ItemAcl());
    assertEquals(null, item.getAcl().getOwners());
    assertTrue(defAcl.applyToIfEnabled(item));
    assertEquals(expectedItem, item);
  }
}
