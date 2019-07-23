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

package com.google.enterprise.cloudsearch.sdk.indexing;

import static com.google.enterprise.cloudsearch.sdk.TestProperties.SERVICE_KEY_PROPERTY_NAME;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.qualifyTestProperty;
import static com.google.enterprise.cloudsearch.sdk.Util.PUBLIC_ACL;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.Util;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ListingConnector;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests to check the integration between the indexing connector SDK's ListingConnector
 * and the CloudSearch Indexing API.
 */
@RunWith(JUnit4.class)
public class ListingConnectorIT {
  private static final Logger logger = Logger.getLogger(ListingConnectorIT.class.getName());
  // The ID of the CloudSearch indexing source where content is stored.
  private static final String DATA_SOURCE_ID_PROPERTY_NAME = qualifyTestProperty("sourceId");
  private static final String ROOT_URL_PROPERTY_NAME = qualifyTestProperty("rootUrl");

  private static String keyFilePath;
  private static String indexingSourceId;
  private static Optional<String> rootUrl;
  private static CloudSearchService v1Client;
  private static TestUtils testUtils;

  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public TemporaryFolder configFolder = new TemporaryFolder();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();

  @BeforeClass
  public static void initialize() throws Exception {
    validateInputParams();
    v1Client = new CloudSearchService(keyFilePath, indexingSourceId, rootUrl);
    testUtils = new TestUtils(v1Client);
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
    p.setProperty("schedule.incrementalTraversalIntervalSecs", "5"); // Default is 300
    return p;
  }

  private Properties createRequiredProperties() throws IOException {
    Properties config = new Properties();
    rootUrl.ifPresent(r -> config.setProperty("api.rootUrl", r));
    config.setProperty("api.sourceId", indexingSourceId);
    config.setProperty("api.serviceAccountPrivateKeyFile", keyFilePath);
    config.setProperty("connector.checkpointDirectory",
        configFolder.newFolder().getAbsolutePath());
    config.setProperty("batch.batchSize", "1");
    config.setProperty("traverse.abortAfterException", "1");
    config.setProperty("traverse.threadPoolSize", "1");
    config.setProperty("schedule.pollQueueIntervalSecs", "3");
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
  public void rootItem_withChildren_succeeds() throws InterruptedException, IOException {
    String rootItemId = "RootItem_" + Util.getRandomId();
    String childItemId1 = "Child1_" + rootItemId;
    String childItemId2 = "Child2_" + rootItemId;
    MockItem root = new MockItem.Builder(rootItemId)
        .setTitle("Root item")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTAINER_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    MockItem child1 = new MockItem.Builder(childItemId1)
        .setTitle("Child item 1")
        .setContainerName(rootItemId)
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    MockItem child2 = new MockItem.Builder(childItemId2)
        .setTitle("Child item 2")
        .setContainerName(rootItemId)
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    ListingRepository testRepository = new ListingRepository()
        .addRootItems(root)
        .addChildItems(child1, child2);
    ListingConnector connector = new ListingConnector(testRepository);
    IndexingApplication application =
        new IndexingApplication.Builder(connector, setupConfiguration(new Properties()))
        .build();
    try {
      try {
        application.start();
        testRepository.awaitGetDoc(60, TimeUnit.SECONDS);
      } finally {
        application.shutdown("test ended");
      }
      testUtils.waitUntilEqual(getFullId(rootItemId), root.getItem());
      testUtils.waitUntilEqual(getFullId(childItemId1), child1.getItem());
      testUtils.waitUntilEqual(getFullId(childItemId2), child2.getItem());
    } finally {
      // Deleting the container deletes the children.
      v1Client.deleteItemsIfExist(getFullId(rootItemId));
    }
  }

  @Test
  public void incrementalChanges_succeeds() throws InterruptedException, IOException {
    String rootItemId = "RootItem_" + Util.getRandomId();
    String childItemId1 = "Child1_" + rootItemId;
    String childItemId2 = "Child2_" + rootItemId;
    MockItem root = new MockItem.Builder(rootItemId)
        .setTitle("Root item")
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTAINER_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    MockItem child1 = new MockItem.Builder(childItemId1)
        .setTitle("Child item 1")
        .setContainerName(rootItemId)
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    MockItem child2 = new MockItem.Builder(childItemId2)
        .setTitle("Child item 2")
        .setContainerName(rootItemId)
        .setContentLanguage("en-us")
        .setItemType(ItemType.CONTENT_ITEM.toString())
        .setAcl(PUBLIC_ACL)
        .build();
    ListingRepository testRepository = new ListingRepository()
        .addRootItems(root);
    ListingConnector connector = new ListingConnector(testRepository);
    Properties config = getIncrementalChangesProperties();
    IndexingApplication application =
        new IndexingApplication.Builder(connector, setupConfiguration(config))
        .build();
    try {
      try {
        application.start();
        testRepository.awaitGetDoc(60, TimeUnit.SECONDS);
        testUtils.waitUntilEqual(getFullId(rootItemId), root.getItem());

        CountDownLatch docCount = new CountDownLatch(2);
        testRepository.notifyGetDoc(docCount::countDown);
        testRepository.addChangedItems(child1, child2);
        testRepository.awaitGetChanges(60, TimeUnit.SECONDS);
        docCount.await(60, TimeUnit.SECONDS);
      } finally {
        application.shutdown("test ended");
      }
      testUtils.waitUntilEqual(getFullId(childItemId1), child1.getItem());
      testUtils.waitUntilEqual(getFullId(childItemId2), child2.getItem());
    } finally {
      // Deleting the container deletes the children.
      v1Client.deleteItemsIfExist(getFullId(rootItemId));
    }
  }

  private String getFullId(String itemName) {
    return Util.getItemId(indexingSourceId, itemName);
  }
}
