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
import static com.google.enterprise.cloudsearch.sdk.Util.getRandomId;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.google.api.services.cloudsearch.v1.model.Date;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.base.Strings;
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
  private static final int WAIT_FOR_CONNECTOR_RUN_SECS = 60;
  private static String keyFilePath;
  private static String indexingSourceId;
  private static Optional<String> rootUrl;
  private static CloudSearchService v1Client;
  private static TestUtils testUtils;
  private static SearchTestUtils searchUtilUser1;
  private static SearchTestUtils searchUtilUser2;
  private static String testUser1;
  private static String testUser2;

  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public TemporaryFolder configFolder = new TemporaryFolder();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();

  @BeforeClass
  public static void initialize() throws Exception {
    validateInputParams();
    v1Client = new CloudSearchService(keyFilePath, indexingSourceId, rootUrl);
    testUtils = new TestUtils(v1Client);
    StructuredDataHelper.verifyMockContentDatasourceSchema(v1Client.getSchema());
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
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item))
        .build();
    runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
    testUtils.waitUntilEqual(itemId, item.getItem());
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
        .build();
    MockItem itemHtm = new MockItem.Builder(htmItemId)
        .setTitle("Base Traversal Container")
        .setMimeType("HTML")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTAINER_ITEM.toString())
        .build();
    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(itemPdf))
        .addPage(asList(itemHtm))
        .build();
    runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
    testUtils.waitUntilEqual(pdfItemId, itemPdf.getItem());
    testUtils.waitUntilEqual(htmItemId, itemHtm.getItem());
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
        .build();
    MockItem itemXml = new MockItem.Builder(accessResourceItemId)
        .setTitle("Permissions")
        .setMimeType("application/xml")
        .setContentLanguage("en-fr")
        .setItemType(ItemType.CONTAINER_ITEM.toString())
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
  public void defaultAcl_verifyServing() throws IOException, InterruptedException {
    String itemName = "DefaultAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty(
        "defaultAcl.readers.users", "google:" + testUser1);
    config.setProperty("defaultAcl.public", "false");
    config.setProperty("defaultAcl.mode", DefaultAclMode.FALLBACK.toString());
    config.setProperty("defaultAcl.name", "mocksdk_defaultAcl_" + getRandomId());
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
    } finally {
      v1Client.deleteItemsIfExist(Collections.singletonList(itemId));
    }
  }

  @Test
  public void defaultAcl_modeAppend_verifyServing() throws IOException, InterruptedException {
    String itemName = "AppendAcl_" + getRandomId();
    String itemId = Util.getItemId(indexingSourceId, itemName);
    Properties config = new Properties();
    config.setProperty(
        "defaultAcl.readers.users", "google:" + testUser2);
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
    config.setProperty(
        "defaultAcl.readers.users", "google:" + testUser2);
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
        .build();

    FakeIndexingRepository mockRepo = new FakeIndexingRepository.Builder()
        .addPage(asList(item))
        .build();
    runAwaitFullTraversalConnector(mockRepo, setupConfiguration(new Properties()));
    verifyStructuredData(itemId, schemaObjectType, item.getItem());
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
    return Util.getItemId(indexingSourceId, itemName) + getRandomId();
  }
}