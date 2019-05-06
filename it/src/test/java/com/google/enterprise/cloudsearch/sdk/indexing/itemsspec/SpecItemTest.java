/*
 * Copyright 2019 Google LLC
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

package com.google.enterprise.cloudsearch.sdk.indexing.itemsspec;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.collection.IsMapContaining.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

/**
 * Tests for SpecItem.
 */
public class SpecItemTest {

  @Test
  public void getName() {
    SpecItem item = new SpecItem.Builder("MOCK_NAME")
        .build();
    assertEquals("MOCK_NAME", item.getName());
  }

  @Test
  public void getMetadata() {
    SpecMetadata metadata = mock(SpecMetadata.class);
    SpecItem item = new SpecItem.Builder("_")
        .setMetadata(metadata)
        .build();
    assertEquals(metadata, item.getMetadata());
  }

  @Test
  public void getStructuredData() {
    SpecItem item = new SpecItem.Builder("_")
        .setStructuredData(ImmutableMap.of("MOCK_NAME", "MOCK_VALUE"))
        .build();
    assertThat(item.getStructuredData(), hasEntry("MOCK_NAME", "MOCK_VALUE"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getStructuredData_isImmutable() {
    Map<String, Object> map  = new HashMap<String, Object>() {{
      put("MOCK_NAME", "MOCK_VALUE");
    }};
    SpecItem item = new SpecItem.Builder("_")
        .setStructuredData(map)
        .build();
    item.getStructuredData().put("MOCK_KEY", "MOCK_VAL");
  }

  @Test
  public void getReaders() {
    SpecItem item = new SpecItem.Builder("_")
        .setReaders(ImmutableList.of("MOCK_READER1", "MOCK_READER2"))
        .build();
    assertThat(item.getReaders(), hasItems("MOCK_READER1", "MOCK_READER2"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getReaders_isImmutable() {
    SpecItem item = new SpecItem.Builder("_")
        .build();
    item.getReaders().add("MOCK_READER");
  }

  @Test
  public void getPrincipalAccess() {
    SpecUserAccess mockUserAccess = mock(SpecUserAccess.class);
    SpecItem item = new SpecItem.Builder("_")
        .setUserAccess(mockUserAccess)
        .build();
    assertEquals(mockUserAccess, item.getUserAccess());
  }

  @Test
  public void equals_otherIsNull_returnFalse() {
    assertNotEquals(
        new SpecItem.Builder("a").build(),
        null);
  }

  @Test
  public void equals_nameDiffers_returnFalse() {
    assertNotEquals(
        new SpecItem.Builder("a").build(),
        new SpecItem.Builder("b").build());
  }

  @Test
  public void equals_metadataDiffers_returnFalse() {
    SpecMetadata metadata1 = new SpecMetadata.Builder().setTitle("a").build();
    SpecMetadata metadata2 = new SpecMetadata.Builder().setTitle("b").build();
    assertNotEquals(
        new SpecItem.Builder("a").setMetadata(metadata1).build(),
        new SpecItem.Builder("a").setMetadata(metadata2).build());
  }

  @Test
  public void equals_structuredDataDiffers_returnFalse() {
    assertNotEquals(
        new SpecItem.Builder("a").setStructuredData(ImmutableMap.of("a", 1)).build(),
        new SpecItem.Builder("a").setStructuredData(ImmutableMap.of("b", 2)).build());
  }

  @Test
  public void equals_readersDiffers_returnFalse() {
    assertNotEquals(
        new SpecItem.Builder("a").setReaders(ImmutableList.of("a")).build(),
        new SpecItem.Builder("a").setReaders(ImmutableList.of("b")).build());
  }

  @Test
  public void equals_userAccessDiffers_returnFalse() {
    SpecUserAccess userAccess1 =
        new SpecUserAccess.Builder().setAllowed(ImmutableList.of("a")).build();
    SpecUserAccess userAccess2 =
        new SpecUserAccess.Builder().setAllowed(ImmutableList.of("b")).build();
    assertNotEquals(
        new SpecItem.Builder("a").setUserAccess(userAccess1).build(),
        new SpecItem.Builder("a").setUserAccess(userAccess2).build());
  }
}
