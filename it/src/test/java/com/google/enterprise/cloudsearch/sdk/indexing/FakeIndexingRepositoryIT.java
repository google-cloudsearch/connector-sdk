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

import static com.google.enterprise.cloudsearch.sdk.TestProperties.SERVICE_KEY_PROPERTY_NAME;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.qualifyTestProperty;
import static com.google.enterprise.cloudsearch.sdk.Util.PUBLIC_ACL;
import static com.google.enterprise.cloudsearch.sdk.Util.getRandomId;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.services.cloudsearch.v1.model.Date;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.Util;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;
import com.google.enterprise.cloudsearch.sdk.sdk.ConnectorStats;
import com.google.enterprise.cloudsearch.sdk.serving.SearchHelper;
import com.google.enterprise.cloudsearch.sdk.serving.SearchTestUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.awaitility.Awaitility;
import org.awaitility.Duration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests to check the integration between the indexing connector SDK and the CloudSearch Indexing
 * API.
 */
@RunWith(JUnit4.class)
public class FakeIndexingRepositoryIT {
  private static final Logger logger = Logger.getLogger(FakeIndexingRepositoryIT.class.getName());
  // The ID of the CloudSearch indexing source where content is stored.
  private static final String DATA_SOURCE_ID_PROPERTY_NAME = qualifyTestProperty("sourceId");
  private static final String ROOT_URL_PROPERTY_NAME = qualifyTestProperty("rootUrl");
  private static final String APPLICATION_ID_PROPERTY_NAME =
      qualifyTestProperty("searchApplicationId");
  private static final String AUTH_INFO_USER1_PROPERTY_NAME =
      qualifyTestProperty("authInfoUser1");
  private static final String AUTH_INFO_USER2_PROPERTY_NAME =
      qualifyTestProperty("authInfoUser2");
  private static final String AUTH_INFO_USER3_PROPERTY_NAME = qualifyTestProperty("authInfoUser3");
  private static final String TEST_GROUP_PREFIX_PROPERTY_NAME = qualifyTestProperty("groupPrefix");
  private static final String TEST_DOMAIN_PROPERTY_NAME = qualifyTestProperty("domain");

  private static final int WAIT_FOR_CONNECTOR_RUN_SECS = 60;
  private static String keyFilePath;
  private static String indexingSourceId;
  private static Optional<String> rootUrl;
  private static CloudSearchService v1Client;
  private static TestUtils testUtils;
  private static SearchTestUtils searchUtilUser1;
  private static SearchTestUtils searchUtilUser2;
  private static SearchTestUtils searchUtilGroupMember;
  private static String testUser1;
  private static String testUser2;
  private static String testUserGroupMember;
  private static String testGroup;

  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public TemporaryFolder configFolder = new TemporaryFolder();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();

  @BeforeClass
  public static void initialize() throws Exception {
    validateInputParams();
    v1Client = new CloudSearchService(keyFilePath, indexingSourceId, rootUrl);
    testUtils = new TestUtils(v1Client);
    String searchApplicationId = System.getProperty(APPLICATION_ID_PROPERTY_NAME);
    String[] authInfoUser1 = System.getProperty(AUTH_INFO_USER1_PROPERTY_NAME).split(",");
    String[] authInfoUser2 = System.getProperty(AUTH_INFO_USER2_PROPERTY_NAME).split(",");

    SearchHelper searchHelperUser1 =
        SearchTestUtils.getSearchHelper(authInfoUser1, searchApplicationId, rootUrl);
    SearchHelper searchHelperUser2 =
        SearchTestUtils.getSearchHelper(authInfoUser2, searchApplicationId, rootUrl);

    testUser1 = authInfoUser1[0];
    testUser2 = authInfoUser2[0];
    assertNotEquals(testUser1, testUser2);
    searchUtilUser1 = new SearchTestUtils(searchHelperUser1);
    searchUtilUser2 = new SearchTestUtils(searchHelperUser2);

    String[] authInfoGroupMember = System.getProperty(AUTH_INFO_USER3_PROPERTY_NAME).split(",");
    testUserGroupMember = authInfoGroupMember[0];
    String domain = System.getProperty(TEST_DOMAIN_PROPERTY_NAME);
    String groupPrefix = System.getProperty(TEST_GROUP_PREFIX_PROPERTY_NAME, "group-connectors3");
    testGroup = String.format("%s@%s", groupPrefix, domain);
    SearchHelper searchHelperGroupMember =
        SearchTestUtils.getSearchHelper(authInfoGroupMember, searchApplicationId, rootUrl);
    searchUtilGroupMember = new SearchTestUtils(searchHelperGroupMember);
  }

  private static void validateInputParams() throws Exception {
    String dataSourceId;
    Path serviceKeyPath;
    logger.log(Level.FINE, "Validate input parameters...");
    try {
      dataSourceId = System.getProperty(DATA_SOURCE_ID_PROPERTY_NAME);
      serviceKeyPath = Paths.get(System.getProperty(SERVICE_KEY_PROPERTY_NAME));
      rootUrl = Optional.ofNullable(System.getProperty(ROOT_URL_PROPERTY_NAME));
      assertTrue(serviceKeyPath.toFile().exists());
      assertFalse(Strings.isNullOrEmpty(dataSourceId));
    } catch (AssertionError error) {
      logger.log(Level.SEVERE,
          "Missing input parameters. Rerun the test as \\\"mvn integration-test"
              + " -DargLine=-Dapi.test.serviceAccountPrivateKeyFile=./path/to/key.json"
              + " -Dapi.test.sourceId=dataSourceId");
      throw error;
    }
    indexingSourceId = dataSourceId;
    keyFilePath = serviceKeyPath.toAbsolutePath().toString();
  }

  private Properties getIncrementalChangesProperties() {
    Properties p = new Properties();
    p.setProperty("connector.runOnce", "false"); // Override createRequiredProperties value
    p.setProperty("schedule.performTraversalOnStart", "true"); // Have getAllDocs happen first
    p.setProperty("schedule.incrementalTraversalIntervalSecs", "5"); // Default is 300
    p.setProperty("batch.batchSize", "1"); // Force batch flush after getChanges returns one item
    return p;
  }

  private Properties createRequiredProperties() throws IOException {
    Properties config = new Properties();
    rootUrl.ifPresent(r -> config.setProperty("api.rootUrl", r));
    config.setProperty("api.sourceId", indexingSourceId);
    config.setProperty("api.serviceAccountPrivateKeyFile", keyFilePath);
    config.setProperty("connector.runOnce", "true");
    config.setProperty("connector.checkpointDirectory",
        configFolder.newFolder().getAbsolutePath());
    config.setProperty("traverse.queueTag", "mockConnectorQueue_" + getRandomId());
    return config;
  }

  private String[] setupConfiguration(Properties additionalConfig) throws IOException {
    Properties config = createRequiredProperties();
    config.putAll(additionalConfig);
    logger.log(Level.INFO, "Config file properties: {0}", config);
    File file = configFolder.newFile();
    try (FileOutputStream output = new FileOutputStream(file)) {
      config.store(output, "properties file");
      output.flush();
    }
    return new String[] {"-Dconfig=" + file.getAbsolutePath()};
  }

  @Test
  public void fullTraversalOnePageTest() throws InterruptedException, IOException {
    String itemId = getItemId("BaseTest");
    MockItem item = new MockItem.Builder(itemId)
        .setTitle("Happy Path")
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item))
        .build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
      testUtils.waitUntilEqual(itemId, item.getItem());
    } finally {
      v1Client.deleteItemsIfExist(asList(itemId));
    }
  }

  @Test
  public void fullTraversalTwoPagesTest() throws InterruptedException, IOException {
    String pdfItemId = getItemId("BaseTraversalPDF");
    String htmItemId = getItemId("BaseTraversalHtm");
    MockItem itemPdf = new MockItem.Builder(pdfItemId)
        .setTitle("Base Traversal")
        .setMimeType("pdf")
        .setContentLanguage("en-fr")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    MockItem itemHtm = new MockItem.Builder(htmItemId)
        .setTitle("Base Traversal Container")
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTAINER_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(itemPdf))
        .addPage(asList(itemHtm))
        .build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
      testUtils.waitUntilEqual(pdfItemId, itemPdf.getItem());
      testUtils.waitUntilEqual(htmItemId, itemHtm.getItem());
    } finally {
      v1Client.deleteItemsIfExist(asList(pdfItemId, htmItemId));
    }
  }

  @Test
  public void updateItemMetadataTest() throws InterruptedException, IOException {
    String servicesItemId = getItemId("Services");
    String accessResourceItemId = getItemId("AccessResource");
    MockItem itemXslt = new MockItem.Builder(servicesItemId)
        .setTitle("Service Container")
        .setMimeType("application/xslt")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTAINER_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    MockItem itemXml = new MockItem.Builder(accessResourceItemId)
        .setTitle("Permissions")
        .setMimeType("application/xml")
        .setContentLanguage("en-fr")
        .setItemType(ItemType.CONTAINER_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(itemXslt, itemXml))
        .build();
    runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
    try {
      testUtils.waitUntilEqual(servicesItemId, itemXslt.getItem());
      testUtils.waitUntilEqual(accessResourceItemId, itemXml.getItem());
      MockItem updateItemXml = new MockItem.Builder(accessResourceItemId)
          .setTitle("Restricted Permissions")
          .setMimeType("application/xml")
          .setContentLanguage("en-us")
          .setItemType(ItemType.CONTAINER_ITEM.toString())
          .setAcl(PUBLIC_ACL)
          .build();
      FakeIndexingRepository mockRepoIterate = new FakeIndexingRepository.Builder()
          .addPage(Collections.singletonList(updateItemXml))
          .build();
      // If there are unfinished operations in the Indexing API, then the connector will exit
      // without completing a full traversal (see b/123352680), so keep running the connector until
      // it completes another traversal.
      final int completedTraversals = ConnectorStats.getSuccessfulFullTraversalsCount();
      Awaitility.await()
          .atMost(Duration.FIVE_MINUTES)
          .pollInSameThread()
          .until(() -> {
            runAwaitFullTraversalConnector(mockRepoIterate, setupConfiguration(new Properties()));
            return ConnectorStats.getSuccessfulFullTraversalsCount() > completedTraversals;
          });
      testUtils.waitUntilEqual(accessResourceItemId, updateItemXml.getItem());
      // servicesItemId should have been deleted after the second full traversal since it no longer
      // exists in the repository.
      testUtils.waitUntilDeleted(servicesItemId);
    } finally {
      v1Client.deleteItemsIfExist(asList(servicesItemId, accessResourceItemId));
    }
  }

  @Test
  public void defaultAcl_modeFallback_itemAcl_verifyServing()
      throws IOException, InterruptedException {
    String itemName = "FallbackAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty("defaultAcl.readers.users", "google:" + testUser1);
    config.setProperty("defaultAcl.public", "false");
    config.setProperty("defaultAcl.mode", DefaultAclMode.FALLBACK.toString());
    config.setProperty("defaultAcl.name", "mocksdk_defaultAcl_" + getRandomId());
    Acl acl =
        new Acl.Builder()
            .setReaders(
                ImmutableList.of(
                    Acl.getGoogleUserPrincipal(testUser2), Acl.getGoogleGroupPrincipal(testGroup)))
            .build();
    MockItem item = new MockItem.Builder(itemId)
        .setTitle(itemName)
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(acl)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(Collections.singletonList(item))
        .build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(config));
      testUtils.waitUntilEqual(itemId, item.getItem());
      searchUtilUser2.waitUntilItemServed(itemName, itemName);
      // Via group membership
      searchUtilGroupMember.waitUntilItemServed(itemName, itemName);
      searchUtilUser1.waitUntilItemNotServed(itemName, itemName);
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void defaultAcl_modeFallback_noItemAcl_verifyServing()
      throws IOException, InterruptedException {
    String itemName = "FallbackAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty("defaultAcl.readers.users", "google:" + testUser1);
    config.setProperty("defaultAcl.readers.groups", "google:" + testGroup);
    config.setProperty("defaultAcl.public", "false");
    config.setProperty("defaultAcl.mode", DefaultAclMode.FALLBACK.toString());
    config.setProperty("defaultAcl.name", "mocksdk_defaultAcl_" + getRandomId());
    // Don't set an ACL on the item; fallback mode will use the default ACL
    MockItem item = new MockItem.Builder(itemId)
        .setTitle(itemName)
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(Collections.singletonList(item))
        .build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(config));
      testUtils.waitUntilEqual(itemId, item.getItem());
      searchUtilUser1.waitUntilItemServed(itemName, itemName);
      searchUtilGroupMember.waitUntilItemServed(itemName, itemName);
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void defaultAcl_modeFallback_noItemAcl_deniedGroup_verifyServing()
      throws IOException, InterruptedException {
    String itemName = "FallbackAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty(
        "defaultAcl.readers.users",
        String.format("google:%s,google:%s", testUser1, testUserGroupMember));
    config.setProperty("defaultAcl.denied.groups", "google:" + testGroup);
    config.setProperty("defaultAcl.public", "false");
    config.setProperty("defaultAcl.mode", DefaultAclMode.FALLBACK.toString());
    config.setProperty("defaultAcl.name", "mocksdk_defaultAcl_" + getRandomId());
    // Don't set an ACL on the item; fallback mode will use the default ACL
    MockItem item =
        new MockItem.Builder(itemId)
            .setTitle(itemName)
            .setMimeType("HTML")
            .setContentLanguage("en-us")
            .setItemType(ItemType.CONTENT_ITEM.toString())
            .build();
    FakeIndexingRepository mockRepo =
        new FakeIndexingRepository.Builder().addPage(Collections.singletonList(item)).build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(config));
      testUtils.waitUntilEqual(itemId, item.getItem());
      searchUtilUser1.waitUntilItemServed(itemName, itemName);
      // While group member is added as reader, group is denied. Effective ACL for member should be
      // denied.
      searchUtilGroupMember.waitUntilItemNotServed(itemName, itemName);
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void defaultAcl_modeFallback_noItemAcl_deniedUser_verifyServing()
      throws IOException, InterruptedException {
    String itemName = "FallbackAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty("defaultAcl.readers.users", String.format("google:%s", testUser1));
    config.setProperty("defaultAcl.readers.group", String.format("google:%s", testGroup));
    config.setProperty("defaultAcl.denied.users", String.format("google:%s", testUserGroupMember));
    config.setProperty("defaultAcl.public", "false");
    config.setProperty("defaultAcl.mode", DefaultAclMode.FALLBACK.toString());
    config.setProperty("defaultAcl.name", "mocksdk_defaultAcl_" + getRandomId());
    // Don't set an ACL on the item; fallback mode will use the default ACL
    MockItem item =
        new MockItem.Builder(itemId)
            .setTitle(itemName)
            .setMimeType("HTML")
            .setContentLanguage("en-us")
            .setItemType(ItemType.CONTENT_ITEM.toString())
            .build();
    FakeIndexingRepository mockRepo =
        new FakeIndexingRepository.Builder().addPage(Collections.singletonList(item)).build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(config));
      testUtils.waitUntilEqual(itemId, item.getItem());
      searchUtilUser1.waitUntilItemServed(itemName, itemName);
      // While group is added as a reader, member is denied. Effective ACL for member should be
      // denied.
      searchUtilGroupMember.waitUntilItemNotServed(itemName, itemName);
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void defaultAcl_modeAppend_verifyServing() throws IOException, InterruptedException {
    String itemName = "AppendAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty("defaultAcl.readers.users", "google:" + testUser2);
    config.setProperty("defaultAcl.public", "false");
    config.setProperty("defaultAcl.mode", DefaultAclMode.APPEND.toString());
    config.setProperty("defaultAcl.name", "mocksdk_appendAcl_" + getRandomId());
    Acl acl = new Acl.Builder()
        .setReaders(Collections
            .singletonList(Acl.getGoogleUserPrincipal(testUser1)))
        .build();
    MockItem item = new MockItem.Builder(itemId)
        .setTitle(itemName)
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(acl)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(Collections.singletonList(item))
        .build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(config));
      testUtils.waitUntilEqual(itemId, item.getItem());
      searchUtilUser2.waitUntilItemServed(itemName, itemName);
      searchUtilUser1.waitUntilItemServed(itemName, itemName);
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void defaultAcl_modeOverride_verifyServing() throws IOException, InterruptedException {
    String itemName = "OverrideAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty("defaultAcl.readers.users", "google:" + testUser2);
    config.setProperty("defaultAcl.public", "false");
    config.setProperty("defaultAcl.mode", DefaultAclMode.OVERRIDE.toString());
    config.setProperty("defaultAcl.name", "mocksdk_overrideAcl_" + getRandomId());
    Acl acl = new Acl.Builder()
        .setReaders(Collections
            .singletonList(Acl.getGoogleUserPrincipal(testUser1)))
        .build();
    MockItem item = new MockItem.Builder(itemId)
        .setTitle(itemName)
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(acl)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(Collections.singletonList(item))
        .build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(config));
      testUtils.waitUntilEqual(itemId, item.getItem());
      searchUtilUser2.waitUntilItemServed(itemName, itemName);
      searchUtilUser1.waitUntilItemNotServed(itemName, itemName);
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void defaultAcl_modeNone_itemAcl_verifyServing() throws IOException, InterruptedException {
    String itemName = "DefaultAclNone_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty("defaultAcl.mode", DefaultAclMode.NONE.toString());
    Acl acl = new Acl.Builder()
        .setReaders(Collections
            .singletonList(Acl.getGoogleUserPrincipal(testUser1)))
        .build();
    MockItem item = new MockItem.Builder(itemId)
        .setTitle(itemName)
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(acl)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(Collections.singletonList(item))
        .build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(config));
      testUtils.waitUntilEqual(itemId, item.getItem());
      searchUtilUser1.waitUntilItemServed(itemName, itemName);
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void defaultAcl_modeNone_noItemAcl_itemNotAdded()
      throws IOException, InterruptedException {
    String itemName = "DefaultAclNone_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty("defaultAcl.mode", DefaultAclMode.NONE.toString());
    config.setProperty("traverse.exceptionHandler", "1"); // Lower number of retry attemps
    MockItem item = new MockItem.Builder(itemId)
        .setTitle(itemName)
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(Collections.singletonList(item))
        .build();
    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(config));
      // Internally, the index request returns "400 Missing Acl in Request.", but we don't
      // have access to that error at this level.
      testUtils.waitUntilDeleted(itemId); // Item should not have been added
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void changedAcl_verifyServing() throws IOException, InterruptedException {
    String itemName = "ChangedAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = getIncrementalChangesProperties();
    config.setProperty("defaultAcl.mode", DefaultAclMode.NONE.toString());
    Acl acl1 = new Acl.Builder()
        .setReaders(Collections.singletonList(Acl.getGoogleUserPrincipal(testUser1)))
        .build();
    Acl acl2 = new Acl.Builder()
        .setReaders(Collections.singletonList(Acl.getGoogleUserPrincipal(testUser2)))
        .build();
    MockItem item1 = new MockItem.Builder(itemId)
        .setTitle(itemName)
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(acl1)
        .build();
    MockItem item2 = new MockItem.Builder(itemId)
        .setTitle(itemName)
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(acl2)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(Collections.singletonList(item1))
        .build();

    String[] args = setupConfiguration(config);
    IndexingApplication application =
        new IndexingApplication.Builder(new FullTraversalConnector(mockRepo), args)
        .build();
    try {
      application.start();
      mockRepo.awaitGetAllDocs(WAIT_FOR_CONNECTOR_RUN_SECS , TimeUnit.SECONDS);
      testUtils.waitUntilEqual(itemId, item1.getItem());
      searchUtilUser1.waitUntilItemServed(itemName, itemName);
      searchUtilUser2.waitUntilItemNotServed(itemName, itemName);

      mockRepo.addChangedItem(item2);
      mockRepo.awaitGetChanges(WAIT_FOR_CONNECTOR_RUN_SECS , TimeUnit.SECONDS);
      testUtils.waitUntilEqual(itemId, item2.getItem());
      searchUtilUser1.waitUntilItemNotServed(itemName, itemName);
      searchUtilUser2.waitUntilItemServed(itemName, itemName);
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
      application.shutdown("test ended");
    }
  }

  @Test
  public void structuredDataBasicDataTypeTest() throws InterruptedException, IOException {
    String itemId = getItemId("BasePropertyTest");
    String schemaObjectType = "myMockDataObject";
    MockItem item = new MockItem.Builder(itemId)
        .setTitle("Happy Path")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .addValue("text", "v1")
        .addValue("text", "v2")
        .addValue("boolean", true)
        .addValue("html", "h1")
        .addValue("html", "h2")
        .addValue("html", "h3")
        .setObjectType(schemaObjectType)
        .setAcl(PUBLIC_ACL)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item))
        .build();
    runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
    verifyStructuredData(itemId, schemaObjectType, item.getItem());
  }

  @Test
  public void structuredDataDatePropertyTest() throws InterruptedException, IOException {
    Date dateValue1 = new Date()
        .setDay(25)
        .setMonth(12)
        .setYear(2017);
    Date dateValue2 = new Date()
        .setDay(5)
        .setMonth(8)
        .setYear(2018);
    String itemId = getItemId("DateObjectTest");
    String schemaObjectType = "myMockDataObject";
    MockItem item = new MockItem.Builder(itemId)
        .setTitle("Validate Date")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .addValue("date", dateValue1)
        .addValue("date", dateValue2)
        .setObjectType(schemaObjectType)
        .setAcl(PUBLIC_ACL)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item))
        .build();
    runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
    verifyStructuredData(itemId, schemaObjectType, item.getItem());
  }

  @Test
  public void structuredDataIntegerTest() throws InterruptedException, IOException {
    String itemId = getItemId("IntegerTest");
    String schemaObjectType = "myMockDataObject";
    MockItem item = new MockItem.Builder(itemId)
        .setTitle("Validate Int Data")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .addValue("integer", 567L)
        .addValue("integer", 9456L)
        .setObjectType(schemaObjectType)
        .setAcl(PUBLIC_ACL)
        .build();

    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item))
        .build();
    runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
    verifyStructuredData(itemId, schemaObjectType, item.getItem());
  }

  @Test
  public void incrementalChanges_updateItem_succeeds() throws Exception {
    String itemId = getItemId("TestItem");
    MockItem item = new MockItem.Builder(itemId)
        .setTitle("Original Title in Incremental Changes Test")
        .setContentLanguage("en-us")
        .setVersion("1")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    MockItem changedItem = new MockItem.Builder(itemId)
        .setTitle("New Title in Incremental Changes Test")
        .setContentLanguage("en-us")
        .setVersion("2")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();

    String[] args = setupConfiguration(getIncrementalChangesProperties());
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item))
        .build();
    IndexingApplication application =
        new IndexingApplication.Builder(new FullTraversalConnector(mockRepo), args)
        .build();
    try {
      application.start();
      mockRepo.awaitGetAllDocs(WAIT_FOR_CONNECTOR_RUN_SECS , TimeUnit.SECONDS);
      testUtils.waitUntilEqual(itemId, item.getItem());
      mockRepo.addChangedItem(changedItem);
      mockRepo.awaitGetChanges(WAIT_FOR_CONNECTOR_RUN_SECS , TimeUnit.SECONDS);
      testUtils.waitUntilEqual(itemId, changedItem.getItem());
    } finally {
      v1Client.deleteItemsIfExist(asList(itemId));
      application.shutdown("test ended");
    }
  }

  @Test
  public void incrementalChanges_addItem_succeeds() throws Exception {
    String itemId1 = getItemId("TestItem1");
    MockItem item1 = new MockItem.Builder(itemId1)
        .setTitle("Item 1 in Incremental Changes Test")
        .setContentLanguage("en-us")
        .setVersion("1")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    String itemId2 = getItemId("TestItem2");
    MockItem item2 = new MockItem.Builder(itemId2)
        .setTitle("Item 2 in Incremental Changes Test")
        .setContentLanguage("en-us")
        .setVersion("1")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();

    String[] args = setupConfiguration(getIncrementalChangesProperties());
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item1))
        .build();
    IndexingApplication application =
        new IndexingApplication.Builder(new FullTraversalConnector(mockRepo), args)
        .build();
    try {
      application.start();
      mockRepo.awaitGetAllDocs(WAIT_FOR_CONNECTOR_RUN_SECS , TimeUnit.SECONDS);
      testUtils.waitUntilEqual(itemId1, item1.getItem());
      mockRepo.addChangedItem(item2);
      mockRepo.awaitGetChanges(WAIT_FOR_CONNECTOR_RUN_SECS , TimeUnit.SECONDS);
      testUtils.waitUntilEqual(itemId1, item1.getItem());
      testUtils.waitUntilEqual(itemId2, item2.getItem());
    } finally {
      v1Client.deleteItemsIfExist(asList(itemId1, itemId2));
      application.shutdown("test ended");
    }
  }

  @Test
  public void incrementalChanges_deleteItem_succeeds() throws Exception {
    String itemId1 = getItemId("TestItem1");
    MockItem item1 = new MockItem.Builder(itemId1)
        .setTitle("Item To Delete in Incremental Changes Test")
        .setContentLanguage("en-us")
        .setVersion("1")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();

    String[] args = setupConfiguration(getIncrementalChangesProperties());
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item1))
        .build();
    IndexingApplication application =
        new IndexingApplication.Builder(new FullTraversalConnector(mockRepo), args)
        .build();
    try {
      application.start();
      mockRepo.awaitGetAllDocs(WAIT_FOR_CONNECTOR_RUN_SECS , TimeUnit.SECONDS);
      testUtils.waitUntilEqual(itemId1, item1.getItem());
      mockRepo.addDeletedItem(itemId1);
      mockRepo.awaitGetChanges(WAIT_FOR_CONNECTOR_RUN_SECS , TimeUnit.SECONDS);
      testUtils.waitUntilDeleted(itemId1);
    } finally {
      v1Client.deleteItemsIfExist(asList(itemId1));
      application.shutdown("test ended");
    }
  }

  @Test
  public void indexing_charactersInId_succeeds() throws InterruptedException, IOException {
    // Test all printable ASCII characters, a Latin-1 character, and a larger code point.
    StringBuilder idBuilder = new StringBuilder("\u00f6\u20ac"); // o-umlaut Euro
    for (int i = 32; i < 127; i++) {
      idBuilder.append((char) i);
    }
    // This id is not qualified with "datasources/<id>/items/". It's intended to match
    // what a typical Repository would use when creating an Item.
    String rawItemId = "ItemIdTest_" + idBuilder.toString() + "_" + getRandomId();
    // Note: not local method getItemId; that one will add another random number to the end
    String escapedFullId = Util.getItemId(indexingSourceId, rawItemId);
    MockItem mockItem = new MockItem.Builder(rawItemId)
        .setTitle("Item Id Test")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(mockItem))
        .build();

    try {
      runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
      testUtils.waitUntilEqual(escapedFullId, mockItem.getItem());
    } finally {
      v1Client.deleteItemsIfExist(asList(escapedFullId));
    }
  }

  private void verifyStructuredData(String itemId, String schemaObjectType,
      Item expectedItem) throws IOException {
    Item actualItem = v1Client.getItem(itemId);
    try {
      StructuredDataHelper.assertStructuredData(actualItem, expectedItem, schemaObjectType);
    } finally {
      v1Client.deleteItem(actualItem.getName(), actualItem.getVersion());
    }
  }

  private void runAwaitFullTraversalConnector(FakeIndexingRepository mockRepo, String[] args)
      throws InterruptedException {
    IndexingApplication application =
        new IndexingApplication.Builder(new FullTraversalConnector(mockRepo), args)
        .build();
    application.start();
    mockRepo.awaitForClose(WAIT_FOR_CONNECTOR_RUN_SECS, TimeUnit.SECONDS);
  }

  private String getItemId(String itemName) {
    // Util.getItemId returns datasources/<source>/items/<itemname>
    return Util.getItemId(indexingSourceId, itemName) + getRandomId();
  }
}
