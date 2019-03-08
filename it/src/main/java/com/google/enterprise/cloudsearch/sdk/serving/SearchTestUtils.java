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

import com.google.api.services.cloudsearch.v1.model.SearchResponse;
import com.google.api.services.cloudsearch.v1.model.SearchResult;
import java.io.IOException;
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
  private static final Duration ITEM_EQUAL_TIMEOUT = Duration.TEN_SECONDS;
  private static final Duration ITEM_EQUAL_POLL_INTERVAL = Duration.TWO_SECONDS;
  private final SearchHelper searchHelper;

  public SearchTestUtils(SearchHelper searchHelper){
    this.searchHelper = searchHelper;
  }

  public void waitUntilItemServed(String itemId, String query) throws IOException {
    Awaitility.await()
        .atMost(ITEM_EQUAL_TIMEOUT)
        .pollInterval(ITEM_EQUAL_POLL_INTERVAL)
        .untilTrue(new AtomicBoolean(resultExists(itemId, query)));
  }

  public void waitUntilItemNotServed(String itemId, String query) throws IOException {
    Awaitility.await()
        .atMost(ITEM_EQUAL_TIMEOUT)
        .pollInterval(ITEM_EQUAL_POLL_INTERVAL)
        .untilFalse(new AtomicBoolean(resultExists(itemId, query)));
  }

  private boolean resultExists(String itemId, String query) throws IOException {
    boolean served = false;
    boolean serving;
    SearchResponse searchResponse = searchHelper.search(query);
    long resultCountExact = searchResponse.getResultCountExact();
    logger.log(Level.FINE, "Search response: {0}", searchResponse.toPrettyString());
    if (resultCountExact > 0) {
      for (SearchResult result : searchResponse.getResults()) {
        boolean titlePresent = result.getTitle().equals(itemId);
        boolean isSnippetRight = result.getSnippet().getSnippet().contains(query);
        serving = titlePresent && isSnippetRight;
        if (serving) {
          logger.log(Level.FINE, "Expected Item in Search Result: {0}", result);
          served = true;
        } else {
          served = false;
        }
      }
    }
    return served;
  }
}
