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

import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.api.services.cloudsearch.v1.model.PollItemsResponse;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO(tvartak) : Move this code to TestingHttpTransport.
class IndexingServiceTransport extends MockHttpTransport {
  private static final Logger logger = Logger.getLogger(IndexingServiceTransport.class.getName());
  static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  static final JsonObjectParser JSON_PARSER = new JsonObjectParser(JSON_FACTORY);

  static final List<String> POLL_QUEUE_REQUEST =
      ImmutableList.of(
          "POST",
          "https://cloudsearch.googleapis.com/v1/indexing/datasources/sourceId/items:poll");

  Map<List<String>, BlockingQueue<MockLowLevelHttpRequest>> requestMap;
  AtomicBoolean transportError = new AtomicBoolean(false);
  @SuppressWarnings("rawtypes")
  List<IndexingTransportRequest> allRequests;
  @SuppressWarnings("rawtypes")
  public IndexingServiceTransport() {
    requestMap = new HashMap<List<String>, BlockingQueue<MockLowLevelHttpRequest>>();
    requestMap.put(POLL_QUEUE_REQUEST, new LinkedBlockingQueue<MockLowLevelHttpRequest>());
    allRequests = new ArrayList<IndexingTransportRequest>();
  }

  @Override
  public MockLowLevelHttpRequest buildRequest(final String method, final String url)
      throws IOException {
    List<String> requestKey = ImmutableList.of(method, url);
    boolean hasError =
        !requestMap.containsKey(requestKey) || requestMap.get(requestKey).isEmpty();
    if (hasError) {
      logger.log(Level.INFO, " error for key {0}", requestKey);
      transportError.set(true);
      throw new UnsupportedOperationException("Unexpected call for key " + requestKey);
    }
    return requestMap.get(requestKey).poll();
  }

  public void registerPollRequest(PollItemsRequest expected, PollItemsResponse pollResponse) {
    IndexingTransportRequest<PollItemsRequest> request =
        new IndexingTransportRequest<PollItemsRequest>(
            PollItemsRequest.class, expected, pollResponse);
    assertTrue(requestMap.get(POLL_QUEUE_REQUEST).offer(request));
    allRequests.add(request);
  }

  public void validate() {
    if (transportError.get()) {
      logger.log(Level.INFO, "Validation error for {0}");
    }
    assertFalse(transportError.get());
    for (@SuppressWarnings("rawtypes") IndexingTransportRequest request : allRequests) {
      assertTrue("Request not executed", request.isExecuted.get());
      assertFalse("Error executing request", request.hasError.get());
    }
  }

  static class IndexingTransportRequest<T> extends MockLowLevelHttpRequest {
    final T expected;
    final Class<T> expectedRequestClass;
    final GenericJson expectedResponse;
    AtomicBoolean isExecuted = new AtomicBoolean(false);
    AtomicBoolean hasError = new AtomicBoolean(false);

    public IndexingTransportRequest(
        Class<T> expectedRequestClass, T expected, GenericJson expectedResponse) {
      this.expectedRequestClass = expectedRequestClass;
      this.expected = expected;
      this.expectedResponse = expectedResponse;
    }

    @Override
    public MockLowLevelHttpResponse execute() throws IOException {
      isExecuted.set(true);
      String requestContent = this.getContentAsString();
      T incomingRequest =
          JSON_PARSER.parseAndClose(new StringReader(requestContent), expectedRequestClass);
      boolean isExpected = expected.equals(incomingRequest);
      if (!isExpected) {
        hasError.set(true);
      }
      assertEquals(expected, incomingRequest);
      MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
      response
      .setStatusCode(HTTP_OK)
      .setContentType(Json.MEDIA_TYPE)
      .setContent(JSON_FACTORY.toString(expectedResponse));
      return response;
    }
  }
}
