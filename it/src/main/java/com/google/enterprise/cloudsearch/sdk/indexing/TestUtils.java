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
package com.google.enterprise.cloudsearch.sdk.indexing;

import static com.google.enterprise.cloudsearch.sdk.Util.unescapeItemName;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.junit.Assert.assertEquals;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.awaitility.Awaitility;
import org.awaitility.Duration;

/**
 * Utility methods for integration tests for the indexing SDK.
 */
public class TestUtils {
  private static final Logger logger = Logger.getLogger(TestUtils.class.getName());
  private static final Duration ITEM_DELETION_TIMEOUT = new Duration(20, TimeUnit.SECONDS);
  private static final Duration ITEM_DELETION_POLL_INTERVAL = Duration.TWO_SECONDS;
  private static final Duration ITEM_EQUAL_TIMEOUT = Duration.TEN_SECONDS;
  private static final Duration ITEM_EQUAL_POLL_INTERVAL = Duration.TWO_SECONDS;

  private final CloudSearchService service;

  public TestUtils(CloudSearchService service) {
    this.service = service;
  }

  /**
   * Waits until the item with the given ID is equal to the expected item.
   *
   * @throws org.awaitility.core.ConditionTimeoutException - if the items are not equal before the
   *  timeout expires.
   */
  public void waitUntilEqual(String itemId, Item expectedItem) {
    Awaitility.await()
        .atMost(ITEM_EQUAL_TIMEOUT)
        .pollInterval(ITEM_EQUAL_POLL_INTERVAL)
        .untilAsserted(() -> assertItemsMatch(expectedItem, service.getItem(itemId)));
  }

  /**
   * Waits for the item with the given ID to be deleted.
   *
   * @throws org.awaitility.core.ConditionTimeoutException - if the item is not deleted before the
   *  timeout.
   */
  public void waitUntilDeleted(String itemId) {
    Awaitility.await()
        .atMost(ITEM_DELETION_TIMEOUT)
        .pollInterval(ITEM_DELETION_POLL_INTERVAL)
        .until(() -> {
          try {
            service.getItem(itemId);
            return false;
          } catch (GoogleJsonResponseException e) {
            if (e.getStatusCode() == HTTP_NOT_FOUND) {
              return true;
            }
            throw e;
          }
        });
  }

  /**
   * Asserts that the expected and the actual items match. Tests tend to use this by
   * passing in an Item constructed in the test class to compare with an Item from the
   * server. Tests have an option of setting the name to a fully-qualified name
   * (datasources/id/items/name) or just the name. This comparison compares the item
   * names, ignoring any prefixes.
   *
   * @throws AssertionError - if the items don't match.
   */
  private void assertItemsMatch(Item expected, Item actual) {
    logger.log(Level.INFO, "Comparing item \n{0} \nto \n{1}", new Object[] {expected, actual});

    // TODO(lchandramouli): verify all applicable meta data
    assertEquals("ACCEPTED", actual.getStatus().getCode());
    assertEquals("name", getItemName(expected.getName()), getItemName(actual.getName()));
    assertEquals("item type", expected.getItemType(), actual.getItemType());

    ItemMetadata expectedMetadata = expected.getMetadata();
    ItemMetadata actualMetadata = actual.getMetadata();
    if (!expectedMetadata.equals(actualMetadata)) {
      // toString() produces different output (expected does not contain quotes, actual
      // does), so set a JSON factory here so assertEquals can highlight the differences.
      expectedMetadata.setFactory(JacksonFactory.getDefaultInstance());
      assertEquals(expectedMetadata.toString(), actualMetadata.toString());
    }
  }

  private static final Pattern NAME_PATTERN = Pattern.compile("^datasources/[^/]+/items/([^/]+)$");

  /**
   * For a fully-qualified id (datasources/sourceid/items/name), returns the name
   * portion, unescaped, otherwise returns the name.
   *
   * @param name an item name
   */
  private String getItemName(String name) {
    Matcher matcher = NAME_PATTERN.matcher(name);
    if (matcher.matches()) {
      name = matcher.group(1);
      return unescapeItemName(name);
    }
    return name;
  }
}
