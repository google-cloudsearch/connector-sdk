/*
 * Copyright © 2019 Google Inc.
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
package com.google.enterprise.cloudsearch.sdk.serving;

import static org.junit.Assert.assertTrue;

import com.google.api.services.cloudsearch.v1.model.SearchResponse;
import com.google.api.services.cloudsearch.v1.model.SearchResult;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.awaitility.Awaitility;
import org.awaitility.Duration;

/**
 * Utility methods to test that indexed items are served correctly.
 */
public class SearchTestUtils {

  private static final Logger logger = Logger.getLogger(SearchTestUtils.class.getName());
  private static final Duration ITEM_EQUAL_TIMEOUT = new Duration(180, TimeUnit.SECONDS);
  private static final Duration ITEM_EQUAL_POLL_INTERVAL = Duration.TWO_SECONDS;

  private final SearchHelper searchHelper;
  private final Duration itemEqualTimeout;
  private final Duration itemEqualPollInterval;

  public SearchTestUtils(SearchHelper searchHelper){
    this(searchHelper, ITEM_EQUAL_TIMEOUT, ITEM_EQUAL_POLL_INTERVAL);
  }

  SearchTestUtils(SearchHelper searchHelper, Duration itemEqualTimeout,
      Duration itemEqualPollInterval) {
    this.searchHelper = searchHelper;
    this.itemEqualTimeout = itemEqualTimeout;
    this.itemEqualPollInterval = itemEqualPollInterval;
  }

  public void waitUntilItemServed(String itemId, String query) throws IOException {
    Awaitility.with()
        .pollInterval(itemEqualPollInterval)
        .await()
        .atMost(itemEqualTimeout)
        .until(() -> resultExists(itemId, query));
  }

  public void waitUntilItemNotServed(String itemId, String query) throws IOException {
    Awaitility.await()
        .atMost(itemEqualTimeout)
        .pollInterval(itemEqualPollInterval)
        .untilFalse(new AtomicBoolean(resultExists(itemId, query)));
  }

  private boolean resultExists(String itemId, String query) throws IOException {
    SearchResponse searchResponse = searchHelper.search(query);
    List<SearchResult> results = searchResponse.getResults();
    if (results != null) {
      for (SearchResult result : results) {
        boolean titlePresent = result.getTitle().equals(itemId);
        boolean isSnippetRight = result.getSnippet().getSnippet().contains(query);
        if (titlePresent && isSnippetRight) {
          logger.log(Level.FINE, "Found expected item {0} in search result: {1}",
              new Object[] { itemId, result });
          return true;
        }
      }
    }
    logger.log(Level.INFO, "Did not find expected item {0} in search response: {1}",
        new Object[] { itemId, searchResponse.toPrettyString() });
    return false;
  }

  /**
   * Utility method to return SearchHelper object.
   *
   * @param authInfo string array containing
   * userEmail of the user to client secrets file,
   * credentialsDirectory path containing the StoredCredential file and
   * clientSecrets path to the client secrets JSON file
   * @param applicationId Id of the serving application
   * @param rootUrl URL of the Indexing API
   */
  public static SearchHelper getSearchHelper(
      String[] authInfo,
      String applicationId,
      Optional<String> rootUrl) throws IOException, GeneralSecurityException {
    if (authInfo.length < 3) {
      throw new IllegalArgumentException("Missing authInfo parameters. Paramaters include "
          + "-Dapi.test.authInfo=user1@domain.com,${credentials_dir},${client_secrets}");
    }
    String authorizedUserEmail = authInfo[0];
    String credDirectory = authInfo[1];
    String clientCredential = authInfo[2];
    File clientCredentials = new File(clientCredential);
    assertTrue(
        String.format("Client credentials file %s does not exist", clientCredentials),
        clientCredentials.exists());
    File credentialsDirectory = new File(credDirectory);
    assertTrue(
        String.format("Credentials directory %s does not exist", credentialsDirectory),
        credentialsDirectory.exists());
    SearchAuthInfo searchAuthInfo =
        new SearchAuthInfo(clientCredentials, credentialsDirectory, authorizedUserEmail);
    return SearchHelper.createSearchHelper(searchAuthInfo, applicationId, rootUrl);
  }
}
