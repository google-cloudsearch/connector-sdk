/*
 * Copyright © 2018 Google Inc.
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
package com.google.enterprise.cloudsearch.csv;

import static com.google.enterprise.cloudsearch.sdk.TestProperties.SERVICE_KEY_PROPERTY_NAME;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.qualifyTestProperty;
import static com.google.enterprise.cloudsearch.sdk.Util.getItemId;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.repackaged.com.google.common.base.Strings;
import com.google.api.services.cloudsearch.v1.model.Date;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.enterprise.cloudsearch.csvconnector.CSVRepository;
import com.google.enterprise.cloudsearch.sdk.Util;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.CloudSearchService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.MockItem;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredDataHelper;
import com.google.enterprise.cloudsearch.sdk.indexing.TestUtils;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;
import com.google.enterprise.cloudsearch.sdk.sdk.ConnectorStats;
import com.google.enterprise.cloudsearch.sdk.serving.SearchHelper;
import com.google.enterprise.cloudsearch.sdk.serving.SearchTestUtils;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests to check the integration between the CSV connector and CloudSearch Indexing API.
 */

@RunWith(JUnit4.class)
public class CsvIT {
  private static final Logger logger = Logger.getLogger(CsvIT.class.getName());
  private static final String DATA_SOURCE_ID_PROPERTY_NAME =
      qualifyTestProperty("sourceId");
  private static final String ROOT_URL_PROPERTY_NAME = qualifyTestProperty("rootUrl");
  private static final String APPLICATION_ID_PROPERTY_NAME =
      qualifyTestProperty("searchApplicationId");
  private static final String AUTH_INFO_PROPERTY_NAME =
      qualifyTestProperty("authInfoUser1");
  private static final Duration CONNECTOR_RUN_TIME = new Duration(45, TimeUnit.SECONDS);
  private static final Duration CONNECTOR_RUN_POLL_INTERVAL = Duration.FIVE_SECONDS;
  private static String keyFilePath;
  private static String indexingSourceId;
  private static SearchHelper searchHelper;
  private static String searchApplicationId;
  private static CloudSearchService v1Client;
  private static Optional<String> rootUrl;
  private static TestUtils util;
  private static SearchTestUtils searchUtil;
  private static String testUser;

  @Rule public TemporaryFolder configFolder = new TemporaryFolder();
  @Rule public TemporaryFolder csvFileFolder = new TemporaryFolder();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();
  @Rule public ExpectedException thrown = ExpectedException.none();

  private static final String TEST_CSV_SINGLE = "empID, empName, Org\n"
      + "1, GoogleCloudSearch1, GCS-Connectors\n";
  private static final String TEST_CSV_STRUCTURED_DATA =
      "intValue, textValue, booleanValue, dateValue, doubleValue, enumValue, timestampValue\n"
          + "2, GoogleCloudSearch1, true, 2017-06-19, 2000.00, 1, 2017-10-10T14:01:23.400Z \n";
  private static final String TEST_CSV_APPEND_BEFORE = "empID, empName, Org\n"
      + "10, GoogleCloudSearch1, GCS-Connectors\n";
  private static final String TEST_CSV_APPEND_AFTER = "empID, empName, Org\n"
      + "10, GoogleCloudSearch1, GCS-Connectors\n"
      + "20, GoogleCloudSearch2, GCS-Connectors\n";
  private static final String TEST_CSV_DELETE_UPDATE_BEFORE = "empID, empName, Org\n"
      + "40, GoogleCloudSearch1, GCS-Connectors\n"
      + "50, GoogleCloudSearch2, GCS-Connectors\n"
      + "60, GoogleCloudSearch3, GCS-Connectors\n";
  private static final String TEST_CSV_DELETE_UPDATE_AFTER = "empID, empName, Org\n"
      + "40, GoogleCloudSearch7, GCS-Connectors\n"
      + "60, GoogleCloudSearch3, GCS-Connectors\n";

  @BeforeClass
  public static void initialize() throws IOException, GeneralSecurityException {
    String dataSourceId;
    Path serviceKeyPath;
    try {
      dataSourceId = System.getProperty(DATA_SOURCE_ID_PROPERTY_NAME);
      serviceKeyPath = Paths.get(System.getProperty(SERVICE_KEY_PROPERTY_NAME));
      assertTrue(serviceKeyPath.toFile().exists());
      assertFalse(Strings.isNullOrEmpty(dataSourceId));
      rootUrl = Optional.ofNullable(System.getProperty(ROOT_URL_PROPERTY_NAME));
    } catch (AssertionError error) {
      logger.log(Level.SEVERE,
          "Missing input parameters. Rerun the test as: mvn integration-test"
              + " -DargLine=-Dapi.test.serviceAccountPrivateKeyFile=./path/to/key.json"
              + " -Dapi.test.sourceId=dataSourceId");
      throw error;
    }
    indexingSourceId = dataSourceId;
    keyFilePath = serviceKeyPath.toAbsolutePath().toString();
    v1Client = new CloudSearchService(keyFilePath, indexingSourceId, rootUrl);
    util = new TestUtils(v1Client);
    String searchApplicationId = System.getProperty(APPLICATION_ID_PROPERTY_NAME);
    String[] authInfo = System.getProperty(AUTH_INFO_PROPERTY_NAME).split(",");
    searchHelper = SearchTestUtils.getSearchHelper(authInfo, searchApplicationId, rootUrl);
    testUser = authInfo[0];
  }

  @Test
  public void testCsvConnector() throws IOException, InterruptedException{
    Properties config = new Properties();
    String mockItemId1 = getItemId(indexingSourceId, "1");
    File csvFile = csvFileFolder.newFile("test.csv");
    try {
      createFile(csvFile, TEST_CSV_SINGLE);
      config.setProperty("csv.filePath", csvFile.getAbsolutePath());
      config.setProperty("csv.skipHeaderRecord", "true");
      config.setProperty("csv.csvColumns", "emp, empName, Org");
      config.setProperty("csv.uniqueKeyColumns", "emp");
      config.setProperty("url.columns", "empName");
      config.setProperty("url.format", "https://www.example.com/viewURL={0}");
      config.setProperty("contentTemplate.csv.title", "CSV-Connector-Testing");
      config.setProperty("itemMetadata.title.field", "empName");
      config.setProperty("connector.runOnce", "true");
      config.setProperty("defaultAcl.public", "true");
      IndexingApplication csvConnector =
          runCsvConnector(setupPropertiesConfigAndRunConnector(config));
      csvConnector.awaitTerminated();
      MockItem expectedItem1 = new MockItem.Builder(mockItemId1)
          .setTitle("GoogleCloudSearch1")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch1")
          .build();
      logger.log(Level.INFO, "Verifying mock item 1 >> {0}...", expectedItem1);
      util.waitUntilEqual(mockItemId1, expectedItem1.getItem());
    } finally {
      try {
        csvFile.delete();
      } finally {
        v1Client.deleteItemsIfExist(Collections.singletonList(mockItemId1));
      }
    }
  }

  @Test
  public void testCSVConnectorStructuredData() throws IOException, InterruptedException{
    Properties config = new Properties();
    String mockItemId1 = getItemId(indexingSourceId, "2");
    Date dateValue = new Date()
        .setDay(19)
        .setMonth(6)
        .setYear(2017);
    String schemaObjectType = "myMockDataObject";
    File csvFile = csvFileFolder.newFile("testStructuredData.csv");
    try {
      createFile(csvFile, TEST_CSV_STRUCTURED_DATA);
      config.setProperty("csv.filePath", csvFile.getAbsolutePath());
      config.setProperty("csv.skipHeaderRecord", "true");
      config.setProperty("csv.csvColumns", "integer, text, boolean, date, double, enum, timestamp");
      config.setProperty("csv.uniqueKeyColumns", "integer");
      config.setProperty("url.columns", "text");
      config.setProperty("url.format", "https://www.example.com/viewURL={0}");
      config.setProperty("contentTemplate.csv.title", "CSV-Connector-Testing");
      config.setProperty("itemMetadata.title.field", "text");
      config.setProperty("itemMetadata.objectType", "myMockDataObject");
      config.setProperty("connector.runOnce", "true");
      config.setProperty("defaultAcl.public", "true");
      IndexingApplication csvConnector =
          runCsvConnector(setupPropertiesConfigAndRunConnector(config));
      csvConnector.awaitTerminated();
      MockItem expectedItem1 = new MockItem.Builder(mockItemId1)
          .setTitle("GoogleCloudSearch1")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch1")
          .addValue("integer", 2)
          .addValue("text", "GoogleCloudSearch1")
          .addValue("boolean", true)
          .addValue("date", dateValue)
          .addValue("double", "2000.00")
          .addValue("enum", 1)
          .addValue("timestamp", "2017-10-10T14:01:23.400Z")
          .setObjectType(schemaObjectType)
          .build();
      Item actualItem1 = v1Client.getItem(mockItemId1);
      StructuredDataHelper
          .assertStructuredData(actualItem1, expectedItem1.getItem(), schemaObjectType);
      util.waitUntilEqual(mockItemId1, expectedItem1.getItem());
    } finally {
      try {
        csvFile.delete();
      } finally {
        v1Client.deleteItemsIfExist(Collections.singletonList(mockItemId1));
      }
    }
  }

  @Test
  public void testCSVConnectorAppendData() throws IOException, InterruptedException {
    Properties config = new Properties();
    List<String> itemLists = new ArrayList<>();
    File csvFile = csvFileFolder.newFile("testAppend.csv");
    try {
      createFile(csvFile, TEST_CSV_APPEND_BEFORE);
      config.setProperty("csv.filePath", csvFile.getAbsolutePath());
      config.setProperty("csv.skipHeaderRecord", "true");
      config.setProperty("csv.csvColumns", "emp, empName, Org");
      config.setProperty("csv.uniqueKeyColumns", "emp");
      config.setProperty("url.columns", "empName");
      config.setProperty("url.format", "https://www.example.com/viewURL={0}");
      config.setProperty("contentTemplate.csv.title", "CSV-Connector-Testing");
      config.setProperty("itemMetadata.title.field", "empName");
      config.setProperty("defaultAcl.public", "true");
      IndexingApplication csvConnector =
          runCsvConnector(setupPropertiesConfigAndRunConnector(config));
      Awaitility.await()
          .atMost(CONNECTOR_RUN_TIME)
          .pollInterval(CONNECTOR_RUN_POLL_INTERVAL)
          .until(() -> ConnectorStats.getSuccessfulFullTraversalsCount() > 0);
      String mockItemId1 = getItemId(indexingSourceId, "10");
      String mockItemId2 = getItemId(indexingSourceId, "20");
      itemLists.addAll(Arrays.asList(mockItemId1, mockItemId2));
      MockItem expectedItem1 = new MockItem.Builder(mockItemId1)
          .setTitle("GoogleCloudSearch1")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch1")
          .build();
      util.waitUntilEqual(mockItemId1, expectedItem1.getItem());
      createFile(csvFile, TEST_CSV_APPEND_AFTER);
      int traversalCount = ConnectorStats.getSuccessfulFullTraversalsCount();
      Awaitility.await()
          .atMost(CONNECTOR_RUN_TIME)
          .pollInterval(CONNECTOR_RUN_POLL_INTERVAL)
          .until(() -> ConnectorStats.getSuccessfulFullTraversalsCount() > traversalCount + 1);
      csvConnector.shutdown("ShutdownHook initiated");
      MockItem expectedItem2 = new MockItem.Builder(mockItemId2)
          .setTitle("GoogleCloudSearch2")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch2")
          .build();
      util.waitUntilEqual(mockItemId1, expectedItem1.getItem());
      util.waitUntilEqual(mockItemId2, expectedItem2.getItem());
    } finally {
      try {
        csvFile.delete();
      } finally {
        v1Client.deleteItemsIfExist(itemLists);
      }
    }
  }

  @Test
  public void testCSVConnectorDeleteUpdate() throws IOException, InterruptedException{
    Properties config = new Properties();
    List<String> itemLists = new ArrayList<>();
    File csvFile = csvFileFolder.newFile("testDeleteUpdate.csv");
    try {
      createFile(csvFile, TEST_CSV_DELETE_UPDATE_BEFORE);
      config.setProperty("csv.filePath", csvFile.getAbsolutePath());
      config.setProperty("csv.skipHeaderRecord", "true");
      config.setProperty("csv.csvColumns", "emp, empName, Org");
      config.setProperty("csv.uniqueKeyColumns", "emp");
      config.setProperty("url.columns", "empName");
      config.setProperty("url.format", "https://www.example.com/viewURL={0}");
      config.setProperty("contentTemplate.csv.title", "CSV-Connector-Testing");
      config.setProperty("itemMetadata.title.field", "empName");
      config.setProperty("defaultAcl.public", "true");
      IndexingApplication csvConnector =
          runCsvConnector(setupPropertiesConfigAndRunConnector(config));
      Awaitility.await()
          .atMost(CONNECTOR_RUN_TIME)
          .pollInterval(CONNECTOR_RUN_POLL_INTERVAL)
          .until(() -> ConnectorStats.getSuccessfulFullTraversalsCount() > 0);
      String mockItemId1 = getItemId(indexingSourceId, "40");
      String mockItemId2 = getItemId(indexingSourceId, "50");
      String mockItemId3 = getItemId(indexingSourceId, "60");
      MockItem expectedItem1 = new MockItem.Builder(mockItemId1)
          .setTitle("GoogleCloudSearch1")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch1")
          .build();
      MockItem expectedItem2 = new MockItem.Builder(mockItemId2)
          .setTitle("GoogleCloudSearch2")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch2")
          .build();
      MockItem expectedItem3 = new MockItem.Builder(mockItemId3)
          .setTitle("GoogleCloudSearch3")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch3")
          .build();
      util.waitUntilEqual(mockItemId1, expectedItem1.getItem());
      util.waitUntilEqual(mockItemId2, expectedItem2.getItem());
      util.waitUntilEqual(mockItemId3, expectedItem3.getItem());
      createFile(csvFile, TEST_CSV_DELETE_UPDATE_AFTER);
      int traversalCount = ConnectorStats.getSuccessfulFullTraversalsCount();
      Awaitility.await()
          .atMost(CONNECTOR_RUN_TIME)
          .pollInterval(CONNECTOR_RUN_POLL_INTERVAL)
          .until(() -> ConnectorStats.getSuccessfulFullTraversalsCount() > traversalCount + 1);
      csvConnector.shutdown("ShutdownHook initiated");
      MockItem updatedItem = new MockItem.Builder(mockItemId1)
          .setTitle("GoogleCloudSearch7")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch7")
          .build();
      itemLists.addAll(Arrays.asList(mockItemId1, mockItemId3));
      util.waitUntilEqual(mockItemId1, updatedItem.getItem());
      util.waitUntilEqual(mockItemId3, expectedItem3.getItem());
      util.waitUntilDeleted(mockItemId2);
    } finally {
      try {
        csvFile.delete();
      } finally {
        v1Client.deleteItemsIfExist(itemLists);
      }
    }
  }

  @Test
  public void testCsvConnectorAclBasedServing()
      throws IOException, InterruptedException, GeneralSecurityException {
    Properties config = new Properties();
    String mockItemId1 = getItemId(indexingSourceId, "1");
    File csvFile = csvFileFolder.newFile("testAcl.csv");
    String query = "GoogleCloudSearch1";
    try {
      createFile(csvFile, TEST_CSV_SINGLE);
      config.setProperty("csv.filePath", csvFile.getAbsolutePath());
      config.setProperty("csv.skipHeaderRecord", "true");
      config.setProperty("csv.csvColumns", "emp, empName, Org");
      config.setProperty("csv.uniqueKeyColumns", "emp");
      config.setProperty("url.columns", "empName");
      config.setProperty("url.format", "https://www.example.com/viewURL={0}");
      config.setProperty("contentTemplate.csv.title", "CSV-Connector-Testing");
      config.setProperty("itemMetadata.title.field", "empName");
      config.setProperty("connector.runOnce", "true");
      config.setProperty(
          "defaultAcl.readers.users", "google:" + testUser);
      config.setProperty("defaultAcl.public", "false");
      IndexingApplication csvConnector =
          runCsvConnector(setupPropertiesConfigAndRunConnector(config));
      csvConnector.awaitTerminated();
      MockItem expectedItem1 = new MockItem.Builder(mockItemId1)
          .setTitle("GoogleCloudSearch1")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.example.com/viewURL=GoogleCloudSearch1")
          .build();
      logger.log(Level.INFO, "Verifying mock item 1 >> {0}...", expectedItem1);
      util.waitUntilEqual(mockItemId1, expectedItem1.getItem());
      searchUtil = new SearchTestUtils(searchHelper);
      searchUtil.waitUntilItemServed("GoogleCloudSearch1", query);
    } finally {
      try {
        csvFile.delete();
      } finally {
        v1Client.deleteItemsIfExist(Collections.singletonList(mockItemId1));
      }
    }
  }

  private String[] setupPropertiesConfigAndRunConnector(Properties testSpecificConfig)
      throws IOException {
    logger.log(Level.FINE, "Setting up properties file.");
    Properties config = new Properties();
    rootUrl.ifPresent(r -> config.setProperty("api.rootUrl", r));
    config.setProperty("api.sourceId", indexingSourceId);
    config.setProperty("api.serviceAccountPrivateKeyFile", keyFilePath);
    config.setProperty("connector.checkpointDirectory", configFolder.newFolder().getAbsolutePath());
    config.setProperty("defaultAcl.mode", "fallback");
    config.setProperty("schedule.traversalIntervalSecs", "10");
    config.put("traverse.queueTag", "mockCsvConnectorQueue-" + Util.getRandomId());
    config.putAll(testSpecificConfig);
    File file = configFolder.newFile();
    String[] args = new String[]{"-Dconfig=" + file.getAbsolutePath()};
    try (FileOutputStream output = new FileOutputStream(file)) {
      config.store(output, "properties file");
      output.flush();
    }
    return args;
  }

  private IndexingApplication runCsvConnector(String[] args) throws InterruptedException{
    IndexingApplication csvConnector =
        new IndexingApplication.Builder(new FullTraversalConnector(new CSVRepository()), args)
            .build();
    csvConnector.start();
    return csvConnector;
  }

  private void createFile(File file, String content) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
      pw.write(content);
    }
  }
}
