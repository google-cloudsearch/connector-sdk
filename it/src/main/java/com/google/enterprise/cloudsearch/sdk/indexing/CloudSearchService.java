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

import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Builder;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ListItemsResponse;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.Schema;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Service wrapper for Cloud Search API client.
 *
 * Example usage:
 * <pre>
 *   CloudSearchService service = new CloudSearchService(serviceAccountKeyPath, sourceId);
 *   Item item = service.getItem(itemName);
 * </pre>
 */
public class CloudSearchService {
  private static final Logger logger = Logger.getLogger(CloudSearchService.class.getName());
  private static final String APPLICATION_NAME = "Cloud Search Mock Indexing Connector";
  private static final Set<String> API_SCOPES =
      Collections.singleton("https://www.googleapis.com/auth/cloud_search");
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private final CloudSearch service;
  private final String indexingSourceId;

  public CloudSearchService(String serviceAccountKeyPath, String sourceId, Optional<String> rootUrl)
      throws IOException, GeneralSecurityException {
    service = getCloudSearchService(serviceAccountKeyPath, rootUrl);
    indexingSourceId = sourceId;
  }

  private static CloudSearch getCloudSearchService(String keyFile, Optional<String> rootUrl)
      throws IOException, GeneralSecurityException {
    InputStream in = Files.newInputStream(Paths.get(keyFile));
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    Credential credential = GoogleCredential
        .fromStream(in, httpTransport, JSON_FACTORY)
        .createScoped(API_SCOPES);
    Builder builder = new CloudSearch.Builder(httpTransport, JSON_FACTORY, credential)
        .setApplicationName(APPLICATION_NAME);
    rootUrl.ifPresent(r -> builder.setRootUrl(r));
    return builder.build();
  }

  /**
   * Deletes items if they exist in the indexing API.
   *
   * This method attempts to delete the remaining items in the list even if deletion of one fails.
   *
   * @param itemIds - the IDs of the items to delete.
   */
  public void deleteItemsIfExist(List<String> itemIds) {
    for (String itemId : itemIds) {
      logger.log(Level.INFO, "Attempting to delete item {0}...", itemId);
      try {
        Item item = getItem(itemId);
        deleteItem(item.getName(), item.getVersion());
      } catch(GoogleJsonResponseException e) {
        if (e.getStatusCode() != HTTP_NOT_FOUND) {
          logger.log(
              Level.WARNING, "Unexpected exception while deleting item:", e);
        }
        // else the item doesn't exist.
      } catch (IOException e) {
        logger.log(Level.WARNING, "Unexpected exception while deleting item:", e);
      }
    }
  }

  /**
   * Gets an item from indexing service using item name.
   */
  public Item getItem(String itemName) throws IOException {
    logger.log(Level.INFO, "Getting item {0}...", itemName);
    Item response = service
        .indexing()
        .datasources()
        .items()
        .get(itemName)
        .execute();
    if (response != null) {
      logger.log(Level.FINE, "Indexed item response {0} and metadata {1} ",
          new Object[] {response.getName(), response.getMetadata()});
    }
    return response;
  }

  /**
   * Gets all items available in data source.
   */
  ListItemsResponse listItems() throws IOException {
    return service
        .indexing()
        .datasources()
        .items()
        .list("datasources/" + indexingSourceId)
        .execute();
  }

  public Operation deleteItem(String itemName, String version) throws IOException {
    // TODO(lchandramouli): verify whether item has deleted successfully.
    return service
        .indexing()
        .datasources()
        .items()
        .delete(itemName)
        .setVersion(version)
        .setMode("ASYNCHRONOUS")
        .execute();
  }

  public Schema getSchema() throws IOException {
    return service
        .indexing()
        .datasources()
        .getSchema("datasources/" + indexingSourceId)
        .execute();
  }
}
