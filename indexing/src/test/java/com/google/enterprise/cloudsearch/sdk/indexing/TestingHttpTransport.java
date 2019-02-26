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

import static com.google.api.services.cloudsearch.v1.CloudSearch.DEFAULT_BASE_URL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_OK;
import static junit.framework.TestCase.assertTrue;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.escape.CharEscapers;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.base.Strings;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * HTTP unit testing transport class.
 *
 * <p>The unit test must set up the request method/url with its expected response object prior to
 * making a call. It is then up to the unit test code to verify correct response is returned. This
 * class will only receive the request and compare it to known set up requests to parrot back the
 * associated response.
 */
class TestingHttpTransport extends MockHttpTransport {
  private static final String SCHEMA_TYPE = "schema";
  private static final Logger logger = Logger.getLogger(TestingHttpTransport.class.getName());
  // constants used to build the response method/url string
  private static final String URL_FORMAT = "%sv1/indexing/datasources/%s/%s";
  private static final String ITEM_LIST_BRIEF = "?brief=true";
  private static final String ITEM_LIST_NOT_BRIEF = "?brief=false";
  private static final String ITEM_LIST_TOKEN = "&pageToken=";
  private static final String ITEM_LIST_LIMIT = "&pageSize=";
  private static final String ENABLE_DEBUGGING = "&debugOptions.enableDebugging=";

  private static final String METHOD_GET = "GET";
  private static final String METHOD_POST = "POST";
  private static final String METHOD_DELETE = "DELETE";

  private static final String TYPE_ITEM = "items";

  private static final String ITEM_DELETE_QUEUE = ":deleteQueueItems";
  private static final String ITEM_POLL = ":poll";
  private static final String ITEM_UNRESERVE = ":unreserve";

  // used to create the desired returned response
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  // store the request/response pairs
  private Map<String, ArrayDeque<GenericJson>> requestMap = new HashMap<>();
  private final String connectorName;

  public TestingHttpTransport(String connectorName) {
    requestMap = new HashMap<>();
    checkArgument(!Strings.isNullOrEmpty(connectorName), "connector name can not be null or empty");
    this.connectorName = connectorName;
  }

  @Override
  public MockLowLevelHttpRequest buildRequest(final String method, final String url)
      throws IOException {
    return new TestingHttpRequest(method, url);
  }

  /**
   * Attach a response to a specific request.
   *
   * <p>Normally this method should be used indirectly by calling the helper methods below, however
   * for complete flexibility this can be used explicitly. Though it is considered an error if two
   * responses are attached to the same request, {@code null} responses are allowed and are often
   * used to indicate success.
   *
   * @param method the method of the request ("PUT", "GET", etc.).
   * @param url the URL of the request
   *     ("https://cloudsearch.googleapis.com/v1/indexing/datasources/[source]/items/[id]", etc.)
   * @param response the response object.
   */
  public void setRequestResponse(String method, String url, GenericJson response) {
    assertTrue((method != null) && (url != null));
    assertTrue(!method.isEmpty() && !url.isEmpty());
    String request = makeKey(method, url);
    if (this.requestMap.get(request) == null) {
      this.requestMap.put(request, new ArrayDeque<>());
    }
    this.requestMap.get(request).addLast(response);
  }

  /**
   * Clear the request/response pairs.
   *
   * <p>This should be used between consecutive calls only to give a different responses to the same
   * request.
   */
  public void clearRequestResponse() {
    this.requestMap.clear();
  }

  /**
   * Create a key based on method and url.
   *
   * @param method "PUT", "GET", etc.
   * @param url url containing the full command path.
   * @return a simple merged key.
   */
  private String makeKey(String method, String url) {
    return (method + "*" + url);
  }

  /** Build the correctly formatted URL request string. */
  private class RequestUrlBuilder {
    private String baseUrl;
    private String sourceId;
    private String type;
    private String id; // optional
    private String options; // optional
    private String operation;

    private RequestUrlBuilder() {
      this.id = "";
      this.options = "";
      this.operation = "";
    }

    private RequestUrlBuilder setBaseUrl(String baseUrl) {
      this.baseUrl = baseUrl;
      return this;
    }

    private RequestUrlBuilder setSourceId(String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    private RequestUrlBuilder setType(String type) {
      this.type = type;
      return this;
    }

    private RequestUrlBuilder setOptions(String options) {
      this.options = options;
      return this;
    }

    private RequestUrlBuilder setId(String id) {
      this.id = id;
      return this;
    }

    private RequestUrlBuilder setOperation(String operation) {
      this.operation = operation;
      return this;
    }

    private String build() {
      checkArgument(!Strings.isNullOrEmpty(this.baseUrl));
      checkArgument(!Strings.isNullOrEmpty(this.sourceId));
      checkArgument(!Strings.isNullOrEmpty(this.type));
      // "id", "operations" and "options" are not required
      return String.format(
              URL_FORMAT,
              this.baseUrl,
              this.sourceId,
              this.type
                  + (this.id.isEmpty() ? "" : "/" + CharEscapers.escapeUriPath(this.id))
                  + (operation.isEmpty() ? "" : ":" + operation))
          + this.options;
    }
  }

  /*
   * Item
   */

  /**
   * Add a request/response pair for simulated {@link IndexingService#deleteItem(String)} command.
   *
   * @param sourceId source ID
   * @param id the item ID
   * @param response the desired returned response
   */
  public void addDeleteItemReqResp(String sourceId, String id, GenericJson response) {
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setType(TYPE_ITEM)
            .setId(id)
            .setOptions("?mode=SYNCHRONOUS") // SYNCHRONOUS is the default mode
            .build();
    this.setRequestResponse(METHOD_DELETE, url, response);
  }

  public void addDeleteQueueItemsReqResp(String sourceId, GenericJson response) {
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setType(TYPE_ITEM + ITEM_DELETE_QUEUE)
            .build();
    this.setRequestResponse(METHOD_POST, url, response);
  }

  /**
   * Add a request/response pair for simulated {@link IndexingService#getItem(String)} command.
   *
   * @param sourceId source ID
   * @param id the item ID
   * @param response the desired returned response
   */
  public void addGetItemReqResp(
      String sourceId, String id, boolean enableDebugging, GenericJson response) {
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setId(id)
            .setType(TYPE_ITEM)
            .setOptions("?connectorName=" + connectorName + ENABLE_DEBUGGING + enableDebugging)
            .build();
    this.setRequestResponse(METHOD_GET, url, response);
  }

  /**
   * Add a request/response pair for simulated {@link Get} request.
   *
   * @param sourceId source ID
   * @param response the desired returned response
   */
  public void addGetSchemaReqResp(String sourceId, boolean enableDebugging, GenericJson response) {
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setType(SCHEMA_TYPE)
            .setOptions("?" + ENABLE_DEBUGGING + enableDebugging)
            .build();
    this.setRequestResponse(METHOD_GET, url, response);
  }

  /**
   * Add a request/response pair for simulated {@link IndexingService#listItem(boolean)} command.
   *
   * @param sourceId source ID
   * @param response the desired returned response
   */
  public void addListItemReqResp(String sourceId, String token, GenericJson response) {
    addListItemReqResp(sourceId, true, token, false, response);
  }

  /**
   * Add a request/response pair for simulated {@link IndexingService#listItem(boolean)} command.
   *
   * @param sourceId source ID
   * @param brief {@code true} for shortened item metadata.
   * @param token the continuation token or null/empty if none.
   * @param response the desired returned response
   */
  public void addListItemReqResp(
      String sourceId, boolean brief, String token, boolean enableDebugging, GenericJson response) {
    String options = ""; // many options for list Items, order is important
    options += brief ? ITEM_LIST_BRIEF : ITEM_LIST_NOT_BRIEF;
    options += "&connectorName=" + connectorName;
    options += ENABLE_DEBUGGING + enableDebugging;
    if ((token != null) && !token.isEmpty()) {
      options += ITEM_LIST_TOKEN + token;
    }
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setType(TYPE_ITEM)
            .setOptions(options)
            .build();
    this.setRequestResponse(METHOD_GET, url, response);
  }

  /**
   * Add a request/response pair for simulated {@link IndexingService#indexItem(Item, boolean)}
   * command.
   *
   * <p>This command is unique in that it has a non-standard URL format and has additional options
   * for incremental and content.
   *
   * @param sourceId source ID
   * @param id the item ID
   * @param response the desired returned response
   */
  public void addUpdateItemReqResp(
      String sourceId, String id, boolean enableDebugging, GenericJson response) {
    // TODO(tvartak) : Validate incremental using priority
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setId(id)
            .setType(TYPE_ITEM)
            .setOperation("index")
            .build();
    this.setRequestResponse(METHOD_POST, url, response);
  }

  /**
   * Add a request/response pair for simulated {@link IndexingService#poll(PollItemsRequest)}
   * command. *
   *
   * @param sourceId source ID
   * @param response the desired returned response
   */
  public void addPollItemReqResp(String sourceId, GenericJson response) {
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setType(TYPE_ITEM + ITEM_POLL)
            .build();
    this.setRequestResponse(METHOD_POST, url, response);
  }

  /**
   * Add a request/response pair for simulated {@link IndexingService#push(PushItem)} command.
   *
   * @param itemId Item ID to push
   * @param sourceId source ID
   * @param response the desired returned response
   */
  public void addPushItemReqResp(String itemId, String sourceId, GenericJson response) {
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setType(TYPE_ITEM)
            .setId(itemId)
            .setOperation("push")
            .build();
    this.setRequestResponse(METHOD_POST, url, response);
  }

  /**
   * Add a request/response pair for simulated {@link IndexingService#unreserve(String)} command.
   *
   * @param sourceId source ID
   * @param response the desired returned response
   */
  public void addUnreserveItemsReqResp(String sourceId, GenericJson response) {
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setType(TYPE_ITEM + ITEM_UNRESERVE)
            .build();
    this.setRequestResponse(METHOD_POST, url, response);
  }

  /**
   * Add a request/response pair for simulated {@link IndexingService#startUpload()} command. *
   *
   * @param sourceId source ID
   * @param response the desired returned response
   */
  public void addUploadItemsReqResp(String sourceId, String id, GenericJson response) {
    String url =
        new RequestUrlBuilder()
            .setBaseUrl(DEFAULT_BASE_URL)
            .setSourceId(sourceId)
            .setId(id)
            .setType(TYPE_ITEM)
            .setOperation("upload")
            .build();
    this.setRequestResponse(METHOD_POST, url, response);
  }

  public void addGetOperationReqResp(String name, GenericJson response) {
    String url = String.format("%sv1/%s", DEFAULT_BASE_URL, name);
    this.setRequestResponse(METHOD_GET, url, response);
  }

  /**
   * HTTP unit testing request class is accessed by {@link TestingHttpTransport#buildRequest(String,
   * String)}.
   */
  private class TestingHttpRequest extends MockLowLevelHttpRequest {

    private final String requestMethod;
    private final String requestUrl;
    private final String request;

    /**
     * HTTP request constructor.
     *
     * @param method one of: "GET", "POST", etc.
     * @param url the request URL.
     */
    private TestingHttpRequest(String method, String url) {
      this.requestMethod = method;
      this.requestUrl = url;
      this.request = makeKey(this.requestMethod, this.requestUrl);
    }

    /**
     * Perform the mock request and return a response.
     *
     * @return the response to the request.
     * @throws IOException
     */
    @Override
    public MockLowLevelHttpResponse execute() throws IOException {
      GenericJson result;
      ArrayDeque<GenericJson> results = TestingHttpTransport.this.requestMap.get(this.request);
      if (results != null && results.size() > 0) {
        result = results.removeFirst();
      } else {
        logger.log(Level.INFO, "Invalid request URL {0}", requestMethod + " " + requestUrl);
        result =
            badRequest(
                this.requestMethod, this.requestUrl, "Unsupported request", HTTP_BAD_REQUEST);
      }
      return getResponse(result);
    }

    /**
     * Convert the passed Json "result" to a response.
     *
     * <p>A {@code null} result will be converted to a successful response. An error response will
     * be generated only if the {@code result} is a {@link GoogleJsonError}.
     *
     * @param result the Json execute result.
     * @return the converted response of the result.
     * @throws IOException
     */
    private MockLowLevelHttpResponse getResponse(GenericJson result) throws IOException {
      MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
      if (result instanceof GoogleJsonError) {
        GoogleJsonError error = (GoogleJsonError) result;
        String errorContent = JSON_FACTORY.toString(new GenericJson().set("error", error));
        response
            .setStatusCode(error.getCode())
            .setReasonPhrase(error.getMessage())
            .setContentType(Json.MEDIA_TYPE)
            .setContent(errorContent);
      } else {
        response
            .setStatusCode(HTTP_OK)
            .setContentType(Json.MEDIA_TYPE)
            .setContent(result == null ? "" : JSON_FACTORY.toString(result));
      }
      return response;
    }

    /**
     * Create an error result when invalid parameters are detected during {@link
     * TestingHttpRequest#execute()}.
     *
     * @param method problem method ("GET", "PUT", etc.).
     * @param location problem url.
     * @param message error string.
     * @return an error result.
     */
    private GoogleJsonError badRequest(String method, String location, String message, int code) {
      return new GoogleJsonError()
          .set("code", code)
          .set("message", "Bad Request: " + method)
          .set(
              "errors",
              Collections.singletonList(
                  new ErrorInfo().set("location", location).set("message", message)));
    }
  }
}
