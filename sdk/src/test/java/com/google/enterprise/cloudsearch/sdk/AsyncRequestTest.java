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
package com.google.enterprise.cloudsearch.sdk;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.json.GenericJson;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats.Event;
import com.google.enterprise.cloudsearch.sdk.StatsManager.ResetStatsRule;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link AsyncRequest}. */

@RunWith(MockitoJUnitRunner.class)
public class AsyncRequestTest {

  @Mock private AbstractGoogleJsonClientRequest<GenericJson> testRequest;
  @Mock private RetryPolicy retryPolicy;
  @Mock private OperationStats operationStats;
  @Mock private Event event;

  @Rule public ResetStatsRule resetStats = new ResetStatsRule();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() {
    when(operationStats.event(anyString())).thenReturn(event);
  }

  @Test
  public void testOnStartWasNotPreviouslyCalled() throws IOException {
    AsyncRequest<GenericJson> req = new AsyncRequest<>(testRequest, retryPolicy, operationStats);
    thrown.expect(IllegalStateException.class);
    req.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
  }

  @Test
  public void testSuccess() throws IOException {
    AsyncRequest<GenericJson> req = new AsyncRequest<>(testRequest, retryPolicy, operationStats);
    req.getCallback().onStart();
    verify(operationStats).event(testRequest.getClass().getName());
    verify(event, times(1)).start();
    req.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
    verify(event, times(1)).success();
  }

  @Test
  public void testFailure() throws IOException {
    AsyncRequest<GenericJson> req = new AsyncRequest<>(testRequest, retryPolicy, operationStats);
    req.getCallback().onStart();
    req.getCallback().onFailure(new GoogleJsonError(), new HttpHeaders());
    // Calling failure again should create a new event
    req.getCallback().onFailure(new GoogleJsonError(), new HttpHeaders());
    verify(operationStats, times(2)).event(testRequest.getClass().getName());
    verify(event, times(1)).start();
    verify(event, times(2)).failure();
  }

  @Test
  public void testRetryableFailure() throws IOException {
    AsyncRequest<GenericJson> req = new AsyncRequest<>(testRequest, retryPolicy, operationStats);
    when(retryPolicy.isRetryableStatusCode(anyInt())).thenReturn(true);
    req.getCallback().onStart();
    req.getCallback().onFailure(new GoogleJsonError(), new HttpHeaders());
    req.getCallback().onStart();
    req.getCallback().onFailure(new GoogleJsonError(), new HttpHeaders());
    req.getCallback().onStart();
    req.getCallback().onSuccess(new GenericJson(), new HttpHeaders());
    verify(operationStats, times(3)).event(testRequest.getClass().getName());
    verify(event, times(3)).start();
    verify(event, times(2)).failure();
    verify(event, times(1)).success();
  }

  @Test
  public void testFailureUnstarted() throws IOException {
    AsyncRequest<GenericJson> req = new AsyncRequest<>(testRequest, retryPolicy, operationStats);
    req.getCallback().onFailure(new GoogleJsonError(), new HttpHeaders());
    verify(operationStats).event(testRequest.getClass().getName());
    verify(event, times(1)).failure();
  }
}
