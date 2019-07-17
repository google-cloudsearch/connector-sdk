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

package com.google.enterprise.cloudsearch.sdk.indexing.util;

import static com.google.enterprise.cloudsearch.sdk.TestProperties.SERVICE_KEY_PROPERTY_NAME;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.qualifyTestProperty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.Util;
import com.google.enterprise.cloudsearch.sdk.indexing.CloudSearchService;
import com.google.enterprise.cloudsearch.sdk.indexing.TestUtils;
import java.io.File;
import java.io.FileWriter;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

/**
 * Test the Uploader utility.
 */
@RunWith(JUnitParamsRunner.class)
public class UploaderIT {

  private static final String DATA_SOURCE_ID_PROPERTY_NAME = qualifyTestProperty("sourceId");
  private static final String ROOT_URL_PROPERTY_NAME = qualifyTestProperty("rootUrl");

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private static CloudSearchService cloudSearchService;
  private static TestUtils testUtils;
  private static String dataSourceId;
  private static String serviceAccountKeyFile;
  private static Optional<String> rootUrl;

  @BeforeClass
  public static void initialize() throws Exception {
    dataSourceId = System.getProperty(DATA_SOURCE_ID_PROPERTY_NAME);
    assertFalse("Missing " + DATA_SOURCE_ID_PROPERTY_NAME, Strings.isNullOrEmpty(dataSourceId));

    serviceAccountKeyFile = System.getProperty(SERVICE_KEY_PROPERTY_NAME);
    assertFalse("Missing " + SERVICE_KEY_PROPERTY_NAME,
        Strings.isNullOrEmpty(serviceAccountKeyFile));
    Path serviceAccountKeyFilePath = Paths.get(serviceAccountKeyFile);
    assertTrue("No such file: " + serviceAccountKeyFile, Files.exists(serviceAccountKeyFilePath));
    serviceAccountKeyFile = serviceAccountKeyFilePath.toAbsolutePath().toString();

    rootUrl = Optional.ofNullable(System.getProperty(ROOT_URL_PROPERTY_NAME));

    cloudSearchService = new CloudSearchService(serviceAccountKeyFile, dataSourceId, rootUrl);
    testUtils = new TestUtils(cloudSearchService);
  }

  @Test
  @Parameters({"UploaderIT", ""})
  public void indexItem_succeeds(String connectorName) throws Exception {
    String rawItemId = "UploaderTest_" + Util.getRandomId();
    String escapedFullId = Util.getItemId(dataSourceId, rawItemId);

    String itemString = ""
        + "      {\n"
        + "        \"name\": \"" + rawItemId + "\",\n"
        + "        \"itemType\": \"CONTENT_ITEM\",\n"
        + "        \"acl\": "
        + "        { \"readers\": [ { \"gsuitePrincipal\": { \"gsuiteDomain\":\"true\" } } ] },\n"
        + "        \"metadata\": { \"title\": \"UploaderTest Item Title\" }\n"
        + "      }\n";
    Item item = new JsonObjectParser(JacksonFactory.getDefaultInstance()).parseAndClose(
        new StringReader(itemString), Item.class);
    String request =
        "{\n"
        + "  \"sourceId\" : \"" + dataSourceId + "\",\n"
        + "  \"requests\" : [\n"
        + "    {\n"
        + "      \"type\": \"items.indexItem\",\n"
        + "      \"isIncremental\": \"true\",\n"
        + "      \"item\": " + itemString
        + "    }\n"
        + "  ]\n"
        + "}";
    File requestsFile = tempFolder.newFile("requests.json");
    try (FileWriter writer = new FileWriter(requestsFile)) {
      writer.write(request);
    }
    try {
      System.setProperty("contentUpload.connectorName", connectorName);
      System.setProperty("rootUrl", rootUrl.get());
      System.setProperty("payload", requestsFile.getAbsolutePath());
      System.setProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG, serviceAccountKeyFile);

      Uploader.main(null);
      new TestUtils(cloudSearchService).waitUntilEqual(escapedFullId, item);
    } finally {
      System.clearProperty("rootUrl");
      System.clearProperty("contentUpload.connectorName");
      System.clearProperty("payload");
      System.clearProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
      cloudSearchService.deleteItemsIfExist(escapedFullId);
    }
  }
}
