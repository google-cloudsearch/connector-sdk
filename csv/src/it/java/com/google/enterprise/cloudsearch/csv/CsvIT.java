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

import static com.google.common.truth.Truth.assertThat;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.SERVICE_KEY_PROPERTY_NAME;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.qualifyTestProperty;
import static com.google.enterprise.cloudsearch.sdk.Util.getItemId;

import com.google.api.services.cloudsearch.v1.model.Date;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.enterprise.cloudsearch.csvconnector.CSVRepository;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.CloudSearchService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.MockItem;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredDataHelper;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests to check the integration between the CSV connector and CloudSearch Indexing API.
 */

@RunWith(JUnit4.class)
public class CsvIT {
  private static final Logger logger = Logger.getLogger(CsvIT.class.getName());
  private static final String DATA_SOURCE_ID_PROPERTY_NAME = qualifyTestProperty("sourceId");
  private static final String ROOT_URL_PROPERTY_NAME = qualifyTestProperty("rootUrl");
  private static String keyFilePath;
  private static String indexingSourceId;
  private static CloudSearchService v1Client;
  private static Optional<String> rootUrl;

  @Rule public TemporaryFolder configFolder = new TemporaryFolder();
  @Rule public TemporaryFolder csvFileFolder = new TemporaryFolder();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();

  private static final String TEST_CSV_SINGLE = "empID, empName, Org\n"
      + "1, GoogleCloudSearch1, GCS-Connectors\n";
  private static final String TEST_CSV_STRUCTURED_DATA =
      "intValue, textValue, booleanValue, dateValue, doubleValue, enumValue\n"
      + "2, GoogleCloudSearch1, true, 2017-06-19, 2000.00, 1\n";

  private void createFile(File file, String content) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))){
      pw.write(content);
    }
  }

  @BeforeClass
  public static void initialize() throws IOException, GeneralSecurityException {
    String dataSourceId;
    Path serviceKeyPath;
    try {
      dataSourceId = System.getProperty(DATA_SOURCE_ID_PROPERTY_NAME);
      serviceKeyPath = Paths.get(System.getProperty(SERVICE_KEY_PROPERTY_NAME));
      assertThat(serviceKeyPath.toFile().exists()).isTrue();
      assertThat(dataSourceId).isNotNull();
      assertThat(dataSourceId).isNotEmpty();
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
    StructuredDataHelper.verifyMockContentDatasourceSchema(v1Client.getSchema());
  }

  @Test
  public void testCsvConnector() throws IOException, InterruptedException{
    Properties config = new Properties();
    File csvFile = csvFileFolder.newFile("test.csv");
    try {
      createFile(csvFile, TEST_CSV_SINGLE);
      config.setProperty("csv.filePath", csvFile.getAbsolutePath());
      config.setProperty("csv.skipHeaderRecord", "true");
      config.setProperty("csv.csvColumns", "emp, empName, Org");
      config.setProperty("csv.uniqueKeyColumns", "emp");
      config.setProperty("url.columns", "empName");
      config.setProperty("url.format", "https://www.mycompany.com/viewURL={0}");
      config.setProperty("contentTemplate.csv.title", "CSV-Connector-Testing");
      config.setProperty("contentTemplate.csv.quality.high", "emp");
      config.setProperty("contentTemplate.csv.quality.medium", "empName");
      config.setProperty("contentTemplate.csv.quality.low", "Org");
      config.setProperty("itemMetadata.title.field", "empName");
      setupPropertiesConfigAndRunConnector(config);
      String mockItemId1 = getItemId(indexingSourceId, "1");
      MockItem expectedItem1 = new MockItem.Builder(mockItemId1)
          .setTitle("GoogleCloudSearch1")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.mycompany.com/viewURL=GoogleCloudSearch1")
          .build();
      logger.log(Level.INFO, "Verifying mock item 1 >> {0}...", expectedItem1);
      Item actualItem1 = v1Client.getItem(mockItemId1);
      assertAndDeleteItem(actualItem1, expectedItem1.getItem());
    } finally {
      csvFile.delete();
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
      config.setProperty("url.format", "https://www.mycompany.com/viewURL={0}");
      config.setProperty("contentTemplate.csv.title", "CSV-Connector-Testing");
      config.setProperty("contentTemplate.csv.quality.high", "integer");
      config.setProperty("contentTemplate.csv.quality.medium", "text");
      config.setProperty("contentTemplate.csv.quality.low", "text");
      config.setProperty("itemMetadata.title.field", "text");
      config.setProperty("itemMetadata.objectType", "myMockDataObject");
      setupPropertiesConfigAndRunConnector(config);
      MockItem expectedItem1 = new MockItem.Builder(mockItemId1)
          .setTitle("GoogleCloudSearch1")
          .setContentLanguage("en")
          .setItemType(ItemType.CONTENT_ITEM.toString())
          .setSourceRepositoryUrl("https://www.mycompany.com/viewURL=GoogleCloudSearch1")
          .addValue("integer", 2)
          .addValue("text", "GoogleCloudSearch1")
          .addValue("boolean", true)
          .addValue("date", dateValue)
          .addValue("double", "2000.00")
          .addValue("enum", 1)
          .setObjectType(schemaObjectType)
          .build();
      Item actualItem1 = v1Client.getItem(mockItemId1);
      StructuredDataHelper
          .assertStructuredData(actualItem1, expectedItem1.getItem(), schemaObjectType);
      assertAndDeleteItem(actualItem1, expectedItem1.getItem());
    } finally {
      csvFile.delete();
    }
  }

  private void assertAndDeleteItem(Item actualItem, Item expectedItem) throws IOException {
    logger.log(Level.INFO, "Expected item {0}...", expectedItem);
    logger.log(Level.INFO, "Actual item {0}...", actualItem);
    logger.log(Level.INFO, "Verifying Actual Vs Expected");
    try {
      assertThat(actualItem.getStatus().getCode()).isEqualTo("ACCEPTED");
      assertThat(actualItem.getItemType()).isEqualTo(expectedItem.getItemType());
      assertThat(actualItem.getMetadata()).isEqualTo(expectedItem.getMetadata());
      assertThat(actualItem.getName()).isEqualTo(expectedItem.getName());
      assertThat(actualItem.getMetadata().getSourceRepositoryUrl())
          .isEqualTo(expectedItem.getMetadata().getSourceRepositoryUrl());
    } finally {
      v1Client.deleteItem(actualItem.getName(), actualItem.getVersion());
    }
  }

  private void setupPropertiesConfigAndRunConnector(Properties testSpecificConfig)
      throws IOException, InterruptedException {
    logger.log(Level.FINE, "Setting up properties file.");
    Properties config = new Properties();
    rootUrl.ifPresent(r -> config.setProperty("api.rootUrl", r));
    config.setProperty("api.sourceId", indexingSourceId);
    config.setProperty("api.serviceAccountPrivateKeyFile", keyFilePath);
    config.setProperty("connector.checkpointDirectory", configFolder.newFolder().getAbsolutePath());
    config.setProperty("defaultAcl.mode", "fallback");
    config.setProperty("defaultAcl.public", "true");
    config.setProperty("schedule.traversalIntervalSecs", "10");
    config.setProperty("connector.runOnce", "true");
    config.putAll(testSpecificConfig);
    File file = configFolder.newFile();
    String[] args = new String[]{"-Dconfig=" + file.getAbsolutePath()};
    try (FileOutputStream output = new FileOutputStream(file)) {
      config.store(output, "properties file");
      output.flush();
    }
    IndexingApplication csvConnector = runCsvConnector(args);
    csvConnector.awaitTerminated();
  }

  private IndexingApplication runCsvConnector(String[] args) throws InterruptedException{
    IndexingApplication csvConnector =
        new IndexingApplication.Builder(new FullTraversalConnector(new CSVRepository()), args)
            .build();
    csvConnector.start();
    return csvConnector;
  }
}
