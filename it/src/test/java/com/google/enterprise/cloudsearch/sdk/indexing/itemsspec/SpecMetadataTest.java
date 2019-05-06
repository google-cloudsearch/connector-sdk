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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * Tests for SpecMetadata.
 */
public class SpecMetadataTest {

  @Test
  public void builder_noFieldsSet_allAreNull() {
    SpecMetadata metadata = new SpecMetadata.Builder().build();
    assertNull(metadata.getTitle());
    assertNull(metadata.getSourceRepositoryUrl());
    assertNull(metadata.getMimeType());
    assertNull(metadata.getContentLanguage());
  }

  @Test
  public void getTitle() {
    SpecMetadata metadata = new SpecMetadata.Builder()
        .setTitle("MOCK_TITLE")
        .build();
    assertEquals("MOCK_TITLE", metadata.getTitle());
  }

  @Test
  public void getSourceRepositoryUrl() {
    SpecMetadata metadata = new SpecMetadata.Builder()
        .setSourceRepositoryUrl("MOCK_URL")
        .build();
    assertEquals("MOCK_URL", metadata.getSourceRepositoryUrl());
  }

  @Test
  public void getMimeType() {
    SpecMetadata metadata = new SpecMetadata.Builder()
        .setContentLanguage("MOCK_LANGUAGE")
        .build();
    assertEquals("MOCK_LANGUAGE", metadata.getContentLanguage());
  }

  @Test
  public void getContentLanguage() {
    SpecMetadata metadata = new SpecMetadata.Builder()
        .setMimeType("MOCK_TYPE")
        .build();
    assertEquals("MOCK_TYPE", metadata.getMimeType());
  }

  @Test
  public void equals_objectsAreEqual_returnTrue() {
    assertEquals(
        new SpecMetadata.Builder()
            .setTitle("a")
            .setSourceRepositoryUrl("a")
            .setMimeType("a")
            .setContentLanguage("a")
            .build(),
        new SpecMetadata.Builder()
            .setTitle("a")
            .setSourceRepositoryUrl("a")
            .setMimeType("a")
            .setContentLanguage("a")
            .build());
  }

  @Test
  public void equals_otherIsNull_returnFalse() {
    assertNotEquals(
        new SpecMetadata.Builder().build(),
        null);
  }

  @Test
  public void equals_titleDiffers_returnFalse() {
    assertNotEquals(
        new SpecMetadata.Builder().setTitle("a").build(),
        new SpecMetadata.Builder().setTitle("b").build());
  }

  @Test
  public void equals_sourceRepositoryUrlDiffers_returnFalse() {
    assertNotEquals(
        new SpecMetadata.Builder().setSourceRepositoryUrl("a").build(),
        new SpecMetadata.Builder().setSourceRepositoryUrl("b").build());
  }

  @Test
  public void equals_mimeTypeDiffers_returnFalse() {
    assertNotEquals(
        new SpecMetadata.Builder().setMimeType("a").build(),
        new SpecMetadata.Builder().setMimeType("b").build());
  }

  @Test
  public void equals_contentLanguageDiffers_returnFalse() {
    assertNotEquals(
        new SpecMetadata.Builder().setContentLanguage("a").build(),
        new SpecMetadata.Builder().setContentLanguage("b").build());
  }
}
