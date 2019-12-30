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

package com.google.enterprise.cloudsearch.sdk;

import static org.junit.Assert.assertEquals;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ExternalGroups}. */
@RunWith(JUnit4.class)
public class ExternalGroupsTest {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final String testGroups =
        "{\"externalGroups\":["
        + " {\"name\":\"Everyone\","
        + "  \"members\":[ {\"id\":\"customer-principal-group@example.com\"} ]},"
        + " {\"name\":\"BUILTIN\\\\Administrators\","
        + "  \"members\":[ {\"id\":\"admin-group1\","
        + "                 \"namespace\":\"identitysources/1234567899\"},"
        + "                {\"id\":\"admin-group2\","
        + "                 \"referenceIdentitySourceName\":\"fileserverIdentitySource\"}"
        + "              ]}"
        + "]}";

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void emptyObject_listIsEmpty() throws Exception {
    ExternalGroups externalGroups = fromString("{}");
    assertEquals(0, externalGroups.getExternalGroups().size());
  }

  @Test
  public void emptyMembers_listIsEmpty() throws Exception {
    ExternalGroups externalGroups = fromString("{\"externalGroups\":[{\"name\":\"groupName\"}]}");
    assertEquals(1, externalGroups.getExternalGroups().size());
    assertEquals(0, externalGroups.getExternalGroups().get(0).getMembers().size());
  }

  @Test
  public void multipleGroups_fromString() throws Exception {
    checkGroups(fromString(testGroups));
  }

  @Test
  public void multipleGroups_fromConfiguration() throws Exception {
    File dataFile = temporaryFolder.newFile("groups.json");
    createFile(dataFile, testGroups);
    Properties config = new Properties();
    config.put("api.identitySourceId", "test-identitySourceId");
    config.put("externalgroups.filename", dataFile.toString());
    setupConfig.initConfig(config);
    checkGroups(ExternalGroups.fromConfiguration());
  }

  private void checkGroups(ExternalGroups externalGroups) {
    assertEquals(2, externalGroups.getExternalGroups().size());
    ExternalGroups.ExternalGroup group = externalGroups.getExternalGroups().get(0);
    assertEquals("Everyone", group.getName());
    List<ExternalGroups.MemberKey> members = group.getMembers();
    assertEquals(1, members.size());
    assertEquals("customer-principal-group@example.com", members.get(0).getId());

    group = externalGroups.getExternalGroups().get(1);
    assertEquals("BUILTIN\\Administrators", group.getName());
    members = group.getMembers();
    assertEquals(2, members.size());
    assertEquals("admin-group1", members.get(0).getId());
    assertEquals("identitysources/1234567899", members.get(0).getNamespace());
    assertEquals(null, members.get(0).getReferenceIdentitySourceName());
    assertEquals("admin-group2", members.get(1).getId());
    assertEquals(null, members.get(1).getNamespace());
    assertEquals("fileserverIdentitySource", members.get(1).getReferenceIdentitySourceName());
  }

  @Test
  public void fromConfiguration_missingFilename_throwsException() throws Exception {
    Properties config = new Properties();
    config.put("api.identitySourceId", "test-identitySourceId");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    ExternalGroups.fromConfiguration();
  }

  @Test
  public void fromConfiguration_missingFile_throwsException() throws Exception {
    Properties config = new Properties();
    config.put("api.identitySourceId", "test-identitySourceId");
    config.put("externalgroups.filename", "no-such-file");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    ExternalGroups.fromConfiguration();
  }

  @Test
  public void fromConfiguration_unreadableFile_throwsException() throws Exception {
    File dataFile = temporaryFolder.newFile("groups.json");
    dataFile.setReadable(false);
    Properties config = new Properties();
    config.put("api.identitySourceId", "test-identitySourceId");
    config.put("externalgroups.filename", dataFile.toString());
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    ExternalGroups.fromConfiguration();
  }

  private ExternalGroups fromString(String data) throws Exception {
    return JSON_FACTORY.fromString(data, ExternalGroups.class);
  }

  private void createFile(File file, String content) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
      pw.write(content);
    }
  }
}
