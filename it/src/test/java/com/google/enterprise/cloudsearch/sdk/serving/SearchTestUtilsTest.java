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

package com.google.enterprise.cloudsearch.sdk.serving;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudsearch.v1.model.SearchResponse;
import com.google.api.services.cloudsearch.v1.model.SearchResult;
import com.google.api.services.cloudsearch.v1.model.Snippet;
import java.io.IOException;
import org.awaitility.core.ConditionTimeoutException;
import org.awaitility.Duration;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class SearchTestUtilsTest {
  @Rule public MockitoRule rule = MockitoJUnit.rule();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private SearchHelper searchHelper;

  private SearchTestUtils subject;

  @Before
  public void setUp() {
    subject =
        new SearchTestUtils(searchHelper, Duration.ONE_SECOND, Duration.ONE_HUNDRED_MILLISECONDS);
  }

  @Test
  public void waitUntilItemServed_failOnAllTries() throws IOException {
    when(searchHelper.search(any())).thenReturn(new SearchResponse());
    thrown.expect(ConditionTimeoutException.class);
    subject.waitUntilItemServed("foo", "bar");
  }

  @Test
  public void waitUntilItemServed_succeedsOnFirstTry() throws IOException {
    when(searchHelper.search(any()))
        .thenReturn(
            new SearchResponse()
            .setResults(asList(
                new SearchResult().setTitle("foo").setSnippet(new Snippet().setSnippet("bar")))));
    subject.waitUntilItemServed("foo", "bar");
  }

  @Test
  public void waitUntilItemServed_succeedsOnSecondTry() throws IOException {
    when(searchHelper.search(any()))
        .thenReturn(new SearchResponse())
        .thenReturn(
            new SearchResponse()
            .setResults(asList(
                new SearchResult().setTitle("foo").setSnippet(new Snippet().setSnippet("bar")))));
    subject.waitUntilItemServed("foo", "bar");
  }

  @Test
  public void waitUntilItemServed_titleMatchOnlyFails() throws IOException {
    when(searchHelper.search(any()))
        .thenReturn(
            new SearchResponse()
            .setResults(asList(
                new SearchResult().setTitle("foo").setSnippet(new Snippet().setSnippet("other")))));
    thrown.expect(ConditionTimeoutException.class);
    subject.waitUntilItemServed("foo", "bar");
  }

  @Test
  public void waitUntilItemServed_snippetMatchOnlyFails() throws IOException {
    when(searchHelper.search(any()))
        .thenReturn(
            new SearchResponse()
            .setResults(asList(
                new SearchResult().setTitle("other").setSnippet(new Snippet().setSnippet("bar")))));
    thrown.expect(ConditionTimeoutException.class);
    subject.waitUntilItemServed("foo", "bar");
  }
}
