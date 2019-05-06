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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import org.junit.Test;

/**
 * Tests for SpecUserAccess.
 */
public class SpecUserAccessTest {

  @Test
  public void getAllowedPrincipals() {
    SpecUserAccess userAccess = new SpecUserAccess.Builder()
        .setAllowed(ImmutableList.of("MOCK_PRINCIPAL"))
        .build();
    assertThat(userAccess.getAllowed(), hasItems("MOCK_PRINCIPAL"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getAllowedPrincipals_isImmutable() {
    SpecUserAccess userAccess = new SpecUserAccess.Builder()
        .setAllowed(new ArrayList<>())
        .build();
    userAccess.getAllowed().add("MOCK_PRINCIPAL");
  }

  @Test
  public void getDeniedPrincipals() {
    SpecUserAccess userAccess = new SpecUserAccess.Builder()
        .setDenied(ImmutableList.of("MOCK_PRINCIPAL"))
        .build();
    assertThat(userAccess.getDenied(), hasItems("MOCK_PRINCIPAL"));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void getDeniedPrincipals_isImmutable() {
    SpecUserAccess userAccess = new SpecUserAccess.Builder()
        .setDenied(new ArrayList<>())
        .build();
    userAccess.getDenied().add("MOCK_PRINCIPAL");
  }

  @Test
  public void equals_objectsAreEqual_returnTrue() {
    assertEquals(
        new SpecUserAccess.Builder()
            .setDenied(ImmutableList.of("a"))
            .setAllowed(ImmutableList.of("a"))
            .build(),
        new SpecUserAccess.Builder()
            .setDenied(ImmutableList.of("a"))
            .setAllowed(ImmutableList.of("a"))
            .build());
  }

  @Test
  public void equals_otherIsNull_returnFalse() {
    assertNotEquals(
        new SpecUserAccess.Builder().build(),
        null);
  }

  @Test
  public void equals_allowedDiffers_returnFalse() {
    assertNotEquals(
        new SpecUserAccess.Builder().setAllowed(ImmutableList.of("a")).build(),
        new SpecUserAccess.Builder().setAllowed(ImmutableList.of("b")).build());
  }

  @Test
  public void equals_deniedDiffers_returnFalse() {
    assertNotEquals(
        new SpecUserAccess.Builder().setDenied(ImmutableList.of("a")).build(),
        new SpecUserAccess.Builder().setDenied(ImmutableList.of("b")).build());
  }
}
