/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.enterprise.cloudsearch.sdk.identity.externalgroups;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.api.services.cloudidentity.v1.model.MembershipRole;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.ExternalGroups;
import com.google.enterprise.cloudsearch.sdk.GroupIdEncoder;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityGroup;
import com.google.enterprise.cloudsearch.sdk.identity.RepositoryContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for {@link ExternalGroupsRepository}.
 */
@RunWith(JUnit4.class)
public class ExternalGroupsRepositoryTest {

  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();

  // The connector requires api.identitySourceId implicitly through the use of
  // RepositoryContext in the Identity SDK.
  @Test
  public void init_missingIdentitySourceId_throwsException() throws IOException {
    setupConfig.initConfig(new Properties());
    thrown.expect(InvalidConfigurationException.class);
    RepositoryContext.fromConfiguration();
  }

  @Test
  public void listGroups_missingFileConfig_throwsException() throws IOException {
    Properties config = new Properties();
    config.put("api.identitySourceId", "test-identitySourceId");
    setupConfig.initConfig(config);
    RepositoryContext repositoryContext = RepositoryContext.fromConfiguration();
    ExternalGroupsRepository groupsRepository = new ExternalGroupsRepository();
    thrown.expect(InvalidConfigurationException.class);
    groupsRepository.listGroups(null);
  }

  @Test
  public void listGroups_invalidFileConfig_throwsException() throws IOException {
    Properties config = new Properties();
    config.put("api.identitySourceId", "test-identitySourceId");
    config.put("externalgroups.filename", "no-such-file");
    setupConfig.initConfig(config);
    RepositoryContext repositoryContext = RepositoryContext.fromConfiguration();
    ExternalGroupsRepository groupsRepository = new ExternalGroupsRepository();
    thrown.expect(InvalidConfigurationException.class);
    groupsRepository.listGroups(null);
  }

  @Test
  public void listGroups_emptyIterable() throws Exception {
    ExternalGroupsRepository groupsRepository =
        new ExternalGroupsRepository(() -> fromString("{}"));
    try (CheckpointCloseableIterable<IdentityGroup> iterable = groupsRepository.listGroups(null)) {
      assertNull(iterable.getCheckpoint());
      assertFalse(iterable.hasMore());
      Iterator<IdentityGroup> iterator = iterable.iterator();
      assertNotNull(iterator);
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void listGroups_oneGroup() throws Exception {
    String identitySourceId = "test-identitySourceId";
    final String testGroups =
        "{\"externalGroups\":["
        + " {\"name\":\"group-name\","
        + "  \"members\":[{\"id\":\"member11\"}, {\"id\":\"member22\"}]}"
        + "]}";
    IdentityGroup expected = makeIdentityGroup(identitySourceId, "group-name",
        makeEntityKey("member11"), makeEntityKey("member22"));

    Properties config = new Properties();
    config.put("api.identitySourceId", identitySourceId);
    config.put("externalgroups.filename", temporaryFolder.newFile("unused").toString());
    setupConfig.initConfig(config);
    RepositoryContext repositoryContext = RepositoryContext.fromConfiguration();
    ExternalGroupsRepository groupsRepository =
        new ExternalGroupsRepository(() -> fromString(testGroups));
    groupsRepository.init(repositoryContext);

    List<IdentityGroup> groups = getAll(groupsRepository.listGroups(null));
    assertEquals(Arrays.asList(expected), groups);
  }

  @Test
  public void listGroups_windowsGroups() throws Exception {
    String identitySourceId = "test-identitySourceId";
    final String testGroups =
        "{\"externalGroups\":["
        + " {\"name\":\"Everyone\","
        + "  \"members\":[ {\"id\":\"customer-principal-group@example.com\"} ]},"
        + " {\"name\":\"BUILTIN\\\\Administrators\","
        + "  \"members\":[ {\"id\":\"admin-group\","
        + "                 \"namespace\":\"identitysources/1234567899\"} ]}"
        + "]}";
    List<IdentityGroup> expected = Arrays.asList(
        makeIdentityGroup(identitySourceId, "Everyone",
            makeEntityKey("customer-principal-group@example.com")),
        makeIdentityGroup(identitySourceId, "BUILTIN\\Administrators",
            makeEntityKey("admin-group", "identitysources/1234567899")));
    Properties config = new Properties();
    config.put("api.identitySourceId", identitySourceId);
    config.put("externalgroups.filename", temporaryFolder.newFile("unused").toString());
    setupConfig.initConfig(config);
    RepositoryContext repositoryContext = RepositoryContext.fromConfiguration();
    ExternalGroupsRepository groupsRepository =
        new ExternalGroupsRepository(() -> fromString(testGroups));
    groupsRepository.init(repositoryContext);

    List<IdentityGroup> groups = getAll(groupsRepository.listGroups(null));
    assertEquals(expected, groups);
  }

  @Test
  public void listGroups_referenceIdentitySource() throws Exception {
    String identitySourceId = "test-identitySourceId";
    final String testGroups =
        "{\"externalGroups\":["
        + " {\"name\":\"Everyone\","
        + "  \"members\":[ {\"id\":\"customer-principal-group@example.com\"} ]},"
        + " {\"name\":\"BUILTIN\\\\Administrators\","
        + "  \"members\":[ {\"id\":\"admin-group\","
        + "                 \"referenceIdentitySourceName\":\"idSourceForFileShare\"} ]}"
        + "]}";
    List<IdentityGroup> expected = Arrays.asList(
        makeIdentityGroup(identitySourceId, "Everyone",
            makeEntityKey("customer-principal-group@example.com")),
        makeIdentityGroup(identitySourceId, "BUILTIN\\Administrators",
            makeEntityKey("admin-group", "identitysources/9876543211")));

    Properties config = new Properties();
    config.put("api.identitySourceId", identitySourceId);
    config.put("api.referenceIdentitySources", "idSourceForFileShare, idSourceForHogwarts");
    config.put("api.referenceIdentitySource.idSourceForFileShare.id", "9876543211");
    config.put("api.referenceIdentitySource.idSourceForHogwarts.id", "77777777");
    config.put("externalgroups.filename", temporaryFolder.newFile("unused").toString());
    setupConfig.initConfig(config);
    RepositoryContext repositoryContext = RepositoryContext.fromConfiguration();
    ExternalGroupsRepository groupsRepository =
        new ExternalGroupsRepository(() -> fromString(testGroups));
    groupsRepository.init(repositoryContext);

    List<IdentityGroup> groups = getAll(groupsRepository.listGroups(null));
    assertEquals(2, groups.size());
    assertEquals(expected, groups);
  }

  @Test
  public void listGroups_referenceIdentitySource_missingConfig() throws Exception {
    String identitySourceId = "test-identitySourceId";
    final String testGroups =
        "{\"externalGroups\":["
        + " {\"name\":\"BUILTIN\\\\Administrators\","
        + "  \"members\":[ {\"id\":\"admin-group\","
        + "                 \"referenceIdentitySourceName\":\"no-such-source\"} ]}"
        + "]}";

    Properties config = new Properties();
    config.put("api.identitySourceId", identitySourceId);
    config.put("externalgroups.filename", temporaryFolder.newFile("unused").toString());
    setupConfig.initConfig(config);
    RepositoryContext repositoryContext = RepositoryContext.fromConfiguration();
    ExternalGroupsRepository groupsRepository =
        new ExternalGroupsRepository(() -> fromString(testGroups));
    groupsRepository.init(repositoryContext);

    thrown.expect(InvalidConfigurationException.class);
    groupsRepository.listGroups(null);
  }

  @Test
  public void listUsers_throwsException() throws Exception {
    ExternalGroupsRepository groupsRepository = new ExternalGroupsRepository();
    thrown.expect(UnsupportedOperationException.class);
    groupsRepository.listUsers(null);
  }

  @Test
  public void close_doesNothing() throws Exception {
    ExternalGroupsRepository groupsRepository =
        new ExternalGroupsRepository(() -> fromString("{}"));
    groupsRepository.close();
    // Can still list groups...
    try (CheckpointCloseableIterable<IdentityGroup> iterable = groupsRepository.listGroups(null)) {
      assertNull(iterable.getCheckpoint());
      assertFalse(iterable.hasMore());
      Iterator<IdentityGroup> iterator = iterable.iterator();
      assertNotNull(iterator);
      assertFalse(iterator.hasNext());
    }
  }

  private EntityKey makeEntityKey(String id) {
    return makeEntityKey(id, null);
  }

  private EntityKey makeEntityKey(String id, String namespace) {
    return new EntityKey()
        .setId(id)
        .setNamespace(namespace);
  }

  private IdentityGroup makeIdentityGroup(String identitySourceId,
      String groupName, EntityKey... members) {
    IdentityGroup.Builder builder = new IdentityGroup.Builder()
        .setGroupIdentity(groupName)
        .setGroupKey(new EntityKey()
            .setId(GroupIdEncoder.encodeGroupId(groupName))
            .setNamespace("identitysources/" + identitySourceId))
        .setMembers(Arrays.stream(members)
            .map(member -> new Membership()
                .setPreferredMemberKey(member)
                .setRoles(Collections.singletonList(new MembershipRole().setName("MEMBER"))))
            .collect(ImmutableSet.toImmutableSet()));
    return builder.build();
  }

  private String identityGroupToString(IdentityGroup identityGroup) {
    StringBuilder builder = new StringBuilder()
        .append("groupIdentity (display name): " + identityGroup.getIdentity()).append("\n")
        .append("groupKey: " + identityGroup.getGroupKey()).append("\n")
        .append("members: " + identityGroup.getMembers()).append("\n");
    return builder.toString();
  }

  private List<IdentityGroup> getAll(CheckpointCloseableIterable<IdentityGroup> iterable) {
    List<IdentityGroup> groups = new ArrayList<>();
    iterable.forEach(groups::add);
    return groups;
  }

  private ExternalGroups fromString(String data) throws IOException {
    return JacksonFactory.getDefaultInstance().fromString(data, ExternalGroups.class);
  }
}
