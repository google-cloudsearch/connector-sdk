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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.Key;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for validating functionality provided by {@link BaseApiService} */
public class BaseApiServiceTest {

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  @Rule public ExpectedException thrown = ExpectedException.none();

  private static class TestingHttpTransport extends MockHttpTransport {

    private final Iterator<MockLowLevelHttpRequest> requests;

    private TestingHttpTransport(Collection<MockLowLevelHttpRequest> requests) {
      this.requests = checkNotNull(requests).iterator();
    }

    @Override
    public MockLowLevelHttpRequest buildRequest(String method, String url) throws IOException {
      if (!requests.hasNext()) {
        throw new UnsupportedOperationException("Unexpected request for URL " + url);
      }
      return requests.next();
    }
  }

  private static class TestApi extends AbstractGoogleJsonClient {
    protected TestApi(Builder builder) {
      super(builder);
    }

    private static class Builder extends AbstractGoogleJsonClient.Builder {

      protected Builder(
          HttpTransport transport,
          JsonFactory jsonFactory,
          HttpRequestInitializer httpRequestInitializer) {
        super(
            transport,
            jsonFactory,
            "https://www.googleapis.com/",
            "mock/v1/",
            httpRequestInitializer,
            false);
      }

      @Override
      public TestApi build() {
        return new TestApi(this);
      }
    }
  }

  private static class TestRequest extends AbstractGoogleJsonClientRequest<TestItem> {

    protected TestRequest(TestApi client) {
      super(client, "GET", "/mock/v1", null, TestItem.class);
    }
  }

  /** Public for json parsing */
  public static class TestItem extends GenericJson {
    @Key private Boolean flag;
    @Key private Integer number;
    @Key private String text;
    @Key private List<String> collection;
    @Key private TestItem child;
  }

  private static class TestApiService extends BaseApiService<TestApi> {

    private final AtomicBoolean isRunning = new AtomicBoolean();

    private TestApiService(Builder builder) {
      super(builder);
    }

    private static class Builder extends BaseApiService.AbstractBuilder<Builder, TestApi> {
      @Override
      public Builder getThis() {
        return this;
      }

      @Override
      public Set<String> getApiScopes() {
        return ImmutableSet.of("https://www.googleapis.com/mock.read");
      }

      @Override
      public TestApi.Builder getServiceBuilder(
          HttpTransport transport,
          JsonFactory jsonFactory,
          HttpRequestInitializer requestInitializer) {
        return new TestApi.Builder(transport, jsonFactory, requestInitializer);
      }

      @Override
      public TestApiService build() throws GeneralSecurityException, IOException {
        setupServiceAndCredentials();
        return new TestApiService(this);
      }
    }

    @Override
    protected void startUp() throws Exception {
      isRunning.compareAndSet(false, true);
    }

    @Override
    protected void shutDown() throws Exception {
      isRunning.compareAndSet(true, false);
    }

    private TestItem get() throws IOException {
      return executeRequest(new TestRequest(service), StatsManager.getComponent("TestApi"), true);
    }
  }

  @Test
  public void testRequestWithInitializePrimitiveTypes() throws Exception {
    HttpTransport transport =
        new TestingHttpTransport(ImmutableList.of(buildRequest(200, new GenericJson())));
    TestApiService apiService =
        new TestApiService.Builder()
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .build();
    validateEmptyItem(apiService.get());
  }

  @Test
  public void testRequestWithNonEmptyResponse() throws Exception {
    TestItem expected = new TestItem();
    expected.number = 10;
    expected.collection = ImmutableList.of("item1");
    expected.flag = true;
    expected.child = new TestItem();
    expected.text = "golden";

    HttpTransport transport =
        new TestingHttpTransport(ImmutableList.of(buildRequest(200, expected)));
    TestApiService apiService =
        new TestApiService.Builder()
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .build();
    TestItem result = apiService.get();
    assertTrue(result.flag);
    assertEquals(expected.number, result.number);
    assertEquals(expected.text, result.text);
    assertEquals(expected.collection, result.collection);
    validateEmptyItem(result.child);
  }

  @Test
  public void testRequestWithNonRetryableError() throws Exception {
    HttpTransport transport =
        new TestingHttpTransport(ImmutableList.of(buildRequest(400, new GenericJson())));
    TestApiService apiService =
        new TestApiService.Builder()
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .build();
    thrown.expect(GoogleJsonResponseException.class);
    apiService.get();
  }

  @Test
  public void testRequestWithRetryableError() throws Exception {
    TestingHttpTransport transport =
        new TestingHttpTransport(
            ImmutableList.of(
                buildRequest(503, new GenericJson()), buildRequest(200, new GenericJson())));
    TestApiService apiService =
        new TestApiService.Builder()
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .build();
    validateEmptyItem(apiService.get());
    assertFalse(transport.requests.hasNext());
  }

  @Test
  public void testRequestWithRetryableErrorFor502() throws Exception {
    TestingHttpTransport transport =
        new TestingHttpTransport(
            ImmutableList.of(
                buildRequest(502, new GenericJson()), buildRequest(200, new GenericJson())));
    TestApiService apiService =
        new TestApiService.Builder()
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .build();
    validateEmptyItem(apiService.get());
    assertFalse(transport.requests.hasNext());
  }

  @Test
  public void testRequestWithTimeouts() throws Exception {
    AtomicInteger connectionTimeoutHolder = new AtomicInteger();
    AtomicInteger readTimeoutHolder = new AtomicInteger();
    MockLowLevelHttpRequest request =
        new MockLowLevelHttpRequest("https://www.googleapis.com/mock/v1") {
          @Override
          public MockLowLevelHttpResponse execute() throws IOException {
            MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
            response
                .setStatusCode(200)
                .setContentType(Json.MEDIA_TYPE)
                .setContent(JSON_FACTORY.toString(new GenericJson()));
            return response;
          }

          @Override
          public void setTimeout(int connectTimeout, int readTimeout) throws IOException {
            connectionTimeoutHolder.set(connectTimeout);
            readTimeoutHolder.set(readTimeout);
          }
        };
    HttpTransport transport =
        new TestingHttpTransport(ImmutableList.of(request));
    TestApiService apiService =
        new TestApiService.Builder()
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .setRequestTimeout(10, 15)
            .build();
    validateEmptyItem(apiService.get());
    assertEquals(10000, connectionTimeoutHolder.get());
    assertEquals(15000, readTimeoutHolder.get());
  }

  @Test
  public void testRequestWithNonDefaultRootUrl() throws Exception {
    MockLowLevelHttpRequest request = buildRequest(200, new GenericJson());
    TestingHttpTransport transport = spy(new TestingHttpTransport(ImmutableList.of(request)));
    TestApiService apiService =
        new TestApiService.Builder()
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .setRootUrl("https://staging.googleapis.com")
            .build();
    validateEmptyItem(apiService.get());
    verify(transport).buildRequest("GET", "https://staging.googleapis.com/mock/v1");
  }

  @Test
  public void testApiServiceWithPrebuiltClient() throws Exception {
    MockLowLevelHttpRequest request = buildRequest(200, new GenericJson());
    HttpTransport transport = new TestingHttpTransport(ImmutableList.of(request));
    TestApi apiClient =
        (TestApi)
            new TestApi.Builder(transport, JSON_FACTORY, null)
                .setApplicationName("bring your own client")
                .build();
    TestApiService apiService =
        new TestApiService.Builder()
            .setService(apiClient)
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .build();
    validateEmptyItem(apiService.get());
    assertThat(
        request.getHeaderValues("user-agent").get(0), containsString("bring your own client"));
  }

  @Test
  public void testIOExceptionRetry() throws Exception {
    MockLowLevelHttpRequest request =
        new MockLowLevelHttpRequest("https://www.googleapis.com/mock/v1") {
          @Override
          public MockLowLevelHttpResponse execute() throws IOException {
            throw new IOException("something is wrong");
          }
        };
    MockLowLevelHttpRequest retryRequest = buildRequest(200, new GenericJson());
    HttpTransport transport = new TestingHttpTransport(ImmutableList.of(request, retryRequest));
    TestApiService apiService =
        new TestApiService.Builder()
            .setTransport(transport)
            .setCredentialFactory(scopes -> new MockGoogleCredential.Builder().build())
            .build();
    validateEmptyItem(apiService.get());
  }

  private static MockLowLevelHttpRequest buildRequest(int responseCode, GenericJson apiResponse) {
    return new MockLowLevelHttpRequest("https://www.googleapis.com/mock/v1") {
      @Override
      public MockLowLevelHttpResponse execute() throws IOException {
        MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
        response
            .setStatusCode(responseCode)
            .setContentType(Json.MEDIA_TYPE)
            .setContent(JSON_FACTORY.toString(apiResponse));
        return response;
      }
    };
  }

  private static void validateEmptyItem(TestItem result) {
    assertFalse(result.flag);
    assertEquals(Integer.valueOf(0), result.number);
    assertNull(result.text);
    assertNull(result.child);
    assertEquals(Collections.emptyList(), result.collection);
  }
}
