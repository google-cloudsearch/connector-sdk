package com.google.enterprise.cloudsearch.sdk.indexing.util;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.nio.charset.Charset.defaultCharset;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items;
import com.google.api.services.cloudsearch.v1.CloudSearchRequest;
import com.google.api.services.cloudsearch.v1.model.DebugOptions;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemContent;
import com.google.api.services.cloudsearch.v1.model.ListItemsResponse;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.api.services.cloudsearch.v1.model.PollItemsResponse;
import com.google.api.services.cloudsearch.v1.model.PushItemRequest;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.StartUploadItemRequest;
import com.google.api.services.cloudsearch.v1.model.UnreserveItemsRequest;
import com.google.api.services.cloudsearch.v1.model.UpdateSchemaRequest;
import com.google.api.services.cloudsearch.v1.model.UploadItemRef;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.LocalFileCredentialFactory;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentUploadServiceImpl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl.PollItemStatus;
import com.google.enterprise.cloudsearch.sdk.indexing.util.UploadRequest.IndexItemAndContentRequest;
import com.google.enterprise.cloudsearch.sdk.indexing.util.UploadRequest.IndexItemRequest;
import com.google.enterprise.cloudsearch.sdk.indexing.util.UploadRequest.MediaContent;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * This class reads in a json file to upload all the requests to the Cloud Search Indexing API.
 *
 * <pre>
 *    java
 *    -Dpayload={your json file contains all the request} \
 *    -Dapi.serviceAccountPrivateKeyFile={service account private key file} \
 *    -DrootUrl=https://www.googleapis.com/ \
 *    -jar cloudsearch-uploader-{version}.jar
 *  </pre>
 *
 * payload is the json file which contains the source Id and all the requests related to it.
 * api.serviceAccountPrivateKeyFile is the file contains private key information. If it is not a
 * json file then api.serviceAccountId is compulsory.
 * <p>
 * Optional command-line properties:
 * <pre>
 *   -DcontentUpload.requestTimeout=&lt;seconds&gt;
 *   -DcontentUpload.connectorName=customConnectorName
 *   -DcontentUpload.enableDebugging=false
 * </pre>
 * <b>Only set enableDebugging to true if asked by Google to help with debugging.</b>
 * <p>
 * Detailed schema of request json file can be found at {@link UploadRequest}.
 */
public class Uploader {
  private static final int DEFAULT_REQUEST_TIMEOUT_SECONDS = 120;
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final JsonObjectParser JSON_PARSER = new JsonObjectParser(JSON_FACTORY);
  static final String SERVICE_ACCOUNT_KEY_FILE_CONFIG = "api.serviceAccountPrivateKeyFile";
  static final Set<String> API_SCOPES =
      new ImmutableSet.Builder<String>()
          .add("https://www.googleapis.com/auth/cloud_search")
          .build();

  @VisibleForTesting CloudSearch cloudSearchService;
  private ContentUploadServiceImpl contentUploadService;
  private URI baseUri;
  private boolean enableDebugging;
  private String connectorName;

  Uploader(Builder builder) {
    this.cloudSearchService = builder.cloudSearch;
    this.contentUploadService = builder.contentUploadService;
    this.baseUri = builder.baseUri;
    this.enableDebugging = builder.enableDebugging;
    this.connectorName = builder.connectorName;
  }

  static class Builder {

    private UploaderHelper uploaderHelper;
    private CredentialFactory credentialFactory;
    private CloudSearch cloudSearch;
    private HttpRequestInitializer requestInitializer;
    private String rootUrl;
    private HttpTransport transport;
    private ContentUploadServiceImpl contentUploadService;
    private URI baseUri;
    private Path serviceAccountKeyFilePath;
    private int connectTimeoutSeconds = DEFAULT_REQUEST_TIMEOUT_SECONDS;
    private int readTimeoutSeconds = DEFAULT_REQUEST_TIMEOUT_SECONDS;
    private boolean enableDebugging;
    private String connectorName = Uploader.class.getName();

    Builder setRequestInitializer(HttpRequestInitializer requestInitializer) {
      this.requestInitializer = requestInitializer;
      return this;
    }

    Builder setTransport(HttpTransport transport) {
      this.transport = transport;
      return this;
    }

    Builder setRootUrl(String rootUrl) {
      this.rootUrl = rootUrl;
      return this;
    }

    Builder setUploaderHelper(UploaderHelper uploaderHelper) {
      this.uploaderHelper = uploaderHelper;
      return this;
    }

    Builder setBaseUri(URI baseUri) {
      this.baseUri = baseUri;
      return this;
    }

    Builder setServiceAccountKeyFilePath(Path serviceAccountKeyFilePath) {
      this.serviceAccountKeyFilePath = serviceAccountKeyFilePath;
      return this;
    }

    Builder setRequestTimeout(int connectTimeoutSeconds, int readTimeoutSeconds) {
      this.connectTimeoutSeconds = connectTimeoutSeconds;
      this.readTimeoutSeconds = readTimeoutSeconds;
      return this;
    }

    Builder setEnableDebugging(boolean enableDebugging) {
      this.enableDebugging = enableDebugging;
      return this;
    }

    Builder setConnectorName(String connectorName) {
      this.connectorName = connectorName;
      return this;
    }

    public Uploader build() throws IOException, GeneralSecurityException {
      if (credentialFactory == null) {
        credentialFactory = uploaderHelper.createCredentialFactory(serviceAccountKeyFilePath);
      }

      if (transport == null) {
        transport = uploaderHelper.createTransport();
      }

      GoogleCredential credential = credentialFactory.getCredential(API_SCOPES);
      CloudSearch.Builder serviceBuilder =
          new CloudSearch.Builder(
              transport,
              JSON_FACTORY,
              credential);

      if (!Strings.isNullOrEmpty(rootUrl)) {
        serviceBuilder.setRootUrl(rootUrl);
      }
      cloudSearch = serviceBuilder.setApplicationName(this.getClass().getName()).build();

      contentUploadService =
          new ContentUploadServiceImpl.Builder()
              .setCredentialFactory(credentialFactory)
              .setRequestInitializer(requestInitializer)
              .setRootUrl(rootUrl)
              .setRequestTimeout(connectTimeoutSeconds, readTimeoutSeconds)
              .build();
      return new Uploader(this);
    }
  }

  void execute(UploadRequest uploadRequest) throws IOException, InterruptedException {
    String sourceId = uploadRequest.sourceId;
    contentUploadService.startAsync().awaitRunning();
    for (UploadRequest.AbstractRequest request : uploadRequest.requests) {
      GenericJson response = request.accept(getVisitor(sourceId));
      System.out.println(response.toPrettyString());
    }
    contentUploadService.stopAsync().awaitTerminated();
  }

  Visitor getVisitor(String sourceId) {
    return new Visitor(sourceId);
  }

  /**
   * Helper class to make our class more testable: factory and util methods, plus a setter
   * for a test instance to allow testing the main() method.
   */
  static class UploaderHelper {
    private static UploaderHelper helper;

    static void setInstance(UploaderHelper helper) {
      UploaderHelper.helper = helper;
    }

    static UploaderHelper getInstance() {
      return helper == null ? new UploaderHelper() : helper;
    }

    public CredentialFactory createCredentialFactory(Path serviceAccountKeyFilePath) {
      return new LocalFileCredentialFactory.Builder()
          .setServiceAccountKeyFilePath(serviceAccountKeyFilePath.toAbsolutePath().toString())
          .build();
    }

    public HttpTransport createTransport() throws GeneralSecurityException, IOException {
      return GoogleNetHttpTransport.newTrustedTransport();
    }
  }

  /**
   * parse json file to form upload request
   */
  static UploadRequest getUploadRequest(InputStreamReader inputStreamReader) throws IOException {
    return JSON_PARSER.parseAndClose(inputStreamReader, UploadRequest.class);
  }

  //a visitor class to visit each request individually.
  class Visitor {

    private static final String RESOURCE_NAME_FORMAT = "datasources/%s";
    private static final String ITEM_RESOURCE_NAME_FORMAT = RESOURCE_NAME_FORMAT + "/items/%s";
    private String resourcePrefix;
    private final String sourceId;

    private byte[] getVersion() {
      return String.valueOf(System.currentTimeMillis()).getBytes();
    }

    Visitor(String sourceId) {
      this.sourceId = sourceId;
      resourcePrefix = String.format(RESOURCE_NAME_FORMAT, sourceId);
    }

    Operation upload(UploadRequest.DeleteRequest deleteRequest) throws IOException {
      //use cloudSearch index api to delete it
      Items.Delete delete =
          cloudSearchService
              .indexing()
              .datasources()
              .items()
              .delete(getItemResourceName(sourceId, deleteRequest.name));
      delete.setVersion(Base64.getEncoder().encodeToString(getVersion()));
      delete.setMode(RequestMode.SYNCHRONOUS.name());
      delete.setDebugOptionsEnableDebugging(enableDebugging);
      delete.setConnectorName(connectorName);
      return execute(delete);
    }

    Operation upload(UploadRequest.DeleteQueueItemsRequest deleteQueueItemsRequest)
        throws IOException {
      Items.DeleteQueueItems deleteQueueItems =
          cloudSearchService
              .indexing()
              .datasources()
              .items()
              .deleteQueueItems(
                  resourcePrefix,
                  new com.google.api.services.cloudsearch.v1.model.DeleteQueueItemsRequest()
                      .setQueue(deleteQueueItemsRequest.queue)
                      .setDebugOptions(new DebugOptions().setEnableDebugging(enableDebugging))
                      .setConnectorName(connectorName));
      return execute(deleteQueueItems);
    }

    Item upload(UploadRequest.GetRequest getRequest) throws IOException {
      Items.Get get =
          cloudSearchService
              .indexing()
              .datasources()
              .items()
              .get(getItemResourceName(sourceId, getRequest.name));
      get.setDebugOptionsEnableDebugging(enableDebugging);
      get.setConnectorName(connectorName);
      return execute(get);
    }

    Operation upload(IndexItemRequest indexItemRequest) throws IOException {
      String fullVerifiedName =
          getItemResourceName(sourceId, indexItemRequest.getName());
      Items.Index index =
          cloudSearchService
              .indexing()
              .datasources()
              .items()
              .index(
                  fullVerifiedName,
                  new com.google.api.services.cloudsearch.v1.model.IndexItemRequest()
                      .setDebugOptions(new DebugOptions().setEnableDebugging(enableDebugging))
                      .setConnectorName(connectorName)
                      .setIndexItemOptions(indexItemRequest.indexItemOptions)
                      .setItem(indexItemRequest.item.setName(fullVerifiedName))
                      .setMode(indexItemRequest.isIncremental ? "SYNCHRONOUS" : "ASYNCHRONOUS"));

      if (indexItemRequest.item.decodeVersion() == null) {
        indexItemRequest.item.encodeVersion(getVersion());
      }

      return execute(index);
    }

    Operation upload(IndexItemAndContentRequest indexItemAndContentRequest)
        throws IOException, InterruptedException {
      //check if content is null
      if ((indexItemAndContentRequest.mediaContent == null)
          || indexItemAndContentRequest.mediaContent.isEmpty()) {
        return upload((IndexItemRequest) indexItemAndContentRequest);
      }

      if ((indexItemAndContentRequest.mediaContent.url == null)
          && (indexItemAndContentRequest.mediaContent.contentString == null)) {
        throw new IOException(
            "indexItemAndContentRequest.mediaContent object must have one of url or contentString "
                + "fields");
      }
      String fullVerifiedName = getItemResourceName(sourceId, indexItemAndContentRequest.getName());
      ItemContent itemContent =
          getItemContent(fullVerifiedName, indexItemAndContentRequest.mediaContent);
      indexItemAndContentRequest.item.setContent(itemContent);

      Items.Index update =
          cloudSearchService
              .indexing()
              .datasources()
              .items()
              .index(
                  fullVerifiedName,
                  new com.google.api.services.cloudsearch.v1.model.IndexItemRequest()
                      .setDebugOptions(new DebugOptions().setEnableDebugging(enableDebugging))
                      .setConnectorName(connectorName)
                      .setItem(indexItemAndContentRequest.item.setName(fullVerifiedName))
                      .setMode(
                          indexItemAndContentRequest.isIncremental
                              ? "SYNCHRONOUS"
                              : "ASYNCHRONOUS"));
      if (indexItemAndContentRequest.item.decodeVersion() == null) {
        indexItemAndContentRequest.item.encodeVersion(getVersion());
      }
      // If the index request is done, the returned Operation will include the item.
      return execute(update);
    }

    PollItemsResponse upload(UploadRequest.PollItemsRequest pollItemRequest) throws IOException {
      List<String> badStatus = PollItemStatus.getBadStatus(pollItemRequest.statusCodes);
      checkArgument(badStatus.isEmpty(), "Invalid Entry status: " + badStatus.toString());
      Items.Poll pollRequest =
          cloudSearchService
              .indexing()
              .datasources()
              .items()
              .poll(
                  resourcePrefix,
                  new PollItemsRequest()
                      .setDebugOptions(new DebugOptions().setEnableDebugging(enableDebugging))
                      .setConnectorName(connectorName)
                      .setLimit(pollItemRequest.limit)
                      .setQueue(pollItemRequest.queue)
                      .setStatusCodes(pollItemRequest.statusCodes));

      return execute(pollRequest);
    }

    Item upload(UploadRequest.PushItemRequest pushItemRequest) throws IOException {
      checkArgument(pushItemRequest.pushItem != null, "push item can not be null");
      String fullVerifiedName = getItemResourceName(sourceId, pushItemRequest.getName());

      Items.Push pushRequest =
          cloudSearchService
              .indexing()
              .datasources()
              .items()
              .push(
                  fullVerifiedName,
                  new PushItemRequest()
                      .setDebugOptions(new DebugOptions().setEnableDebugging(enableDebugging))
                      .setConnectorName(connectorName)
                      .setItem(pushItemRequest.pushItem));
      return execute(pushRequest);
    }

    Operation upload(UploadRequest.UnreserveRequest unreserveRequest) throws IOException {
      UnreserveItemsRequest unreserveQueueRequest = new UnreserveItemsRequest()
          .setDebugOptions(new DebugOptions().setEnableDebugging(enableDebugging))
          .setConnectorName(connectorName)
          .setQueue(unreserveRequest.queue);
      Items.Unreserve unreserve =
          cloudSearchService
              .indexing()
              .datasources()
              .items()
              .unreserve(resourcePrefix, unreserveQueueRequest);
      return execute(unreserve);
    }

    ListItemsResponse upload(UploadRequest.ListRequest listRequest) throws IOException {
      Items.List request =
          cloudSearchService.indexing().datasources().items().list(resourcePrefix);
      request.setBrief(listRequest.brief);
      if (listRequest.pageToken != null) {
        request.setPageToken(listRequest.pageToken);
      }
      if (listRequest.pageSize > 0) {
        request.setPageSize(listRequest.pageSize);
      }
      request.setDebugOptionsEnableDebugging(enableDebugging);
      request.setConnectorName(connectorName);
      return execute(request);
    }

    GenericJson upload(UploadRequest.DatasourcesListRequest datasourcesListRequest)
        throws IOException {
      CloudSearch.Settings.Datasources.List list =
          cloudSearchService.settings().datasources().list()
          .setDebugOptionsEnableDebugging(enableDebugging)
          .setPageToken(datasourcesListRequest.pageToken);
      if (datasourcesListRequest.pageSize != null) {
        list.setPageSize(datasourcesListRequest.pageSize);
      }
      return execute(list);
    }

    Operation upload(UploadRequest.UpdateSchemaRequest updateSchemaRequest) throws IOException {
      if (Strings.isNullOrEmpty(updateSchemaRequest.schemaJsonFile)) {
        throw new IOException("schema json file is not specified");
      }

      URI schemaPath = baseUri.resolve(updateSchemaRequest.schemaJsonFile);
      if (!Files.exists(Paths.get(schemaPath))) {
        throw new IOException(schemaPath.toString() + " does not exists");
      }

      Schema schema;
      try {
        InputStreamReader inputStreamReader = new InputStreamReader(
            schemaPath.toURL().openStream(), defaultCharset());
        schema = JSON_PARSER.parseAndClose(inputStreamReader, Schema.class);
      } catch (IOException e) {
        throw new StartupException(
            "Failed to parse schema file " + Paths.get(schemaPath).toString() + e);
      }

      Datasources.UpdateSchema updateSchema =
          cloudSearchService
              .indexing()
              .datasources()
              .updateSchema(
                  resourcePrefix,
                  new UpdateSchemaRequest()
                      .setDebugOptions(new DebugOptions().setEnableDebugging(enableDebugging))
                      .setSchema(schema)
                      .setValidateOnly(updateSchemaRequest.validateOnly));
      return execute(updateSchema);
    }

    Schema upload(UploadRequest.GetSchemaRequest getSchemaRequest) throws IOException {
      Datasources.GetSchema getSchema =
          cloudSearchService
              .indexing()
              .datasources()
              .getSchema(resourcePrefix)
              .setDebugOptionsEnableDebugging(enableDebugging);
      return execute(getSchema);
    }

    Operation upload(UploadRequest.DeleteSchemaRequest deleteSchemaRequest) throws IOException {
      Datasources.DeleteSchema deleteSchema =
          cloudSearchService
              .indexing()
              .datasources()
              .deleteSchema(resourcePrefix)
              .setDebugOptionsEnableDebugging(enableDebugging);
      return execute(deleteSchema);
    }

    ItemContent getItemContent(String itemResourceName, MediaContent mediaContent)
        throws IOException, InterruptedException {
      ItemContent itemContent = new ItemContent();
      if (mediaContent.contentFormat == null) {
        mediaContent.contentFormat = "RAW";
      }
      itemContent.setContentFormat(mediaContent.contentFormat);
      AbstractInputStreamContent content;
      if (mediaContent.contentString != null) {
        content = ByteArrayContent.fromString(mediaContent.contentType, mediaContent.contentString);
      } else {
        content =
            new UrlInputStreamContent(
                mediaContent.contentType, baseUri.resolve(mediaContent.url).toURL());
      }

      long length = content.getLength();
      boolean inline =
          (length <= IndexingServiceImpl.DEFAULT_CONTENT_UPLOAD_THRESHOLD_BYTES) && (length > 0);
      if (inline) {
        itemContent.encodeInlineContent(convertStreamToByteArray(content));
      } else {
        // upload content first
        Items.Upload uploadRequest =
            cloudSearchService
                .indexing()
                .datasources()
                .items()
                .upload(itemResourceName,
                    new StartUploadItemRequest()
                    .setDebugOptions(new DebugOptions().setEnableDebugging(enableDebugging))
                    .setConnectorName(connectorName));

        UploadItemRef uploadItemRef = execute(uploadRequest);
        try {
          contentUploadService.uploadContent(uploadItemRef.getName(), content).get();
          itemContent.setContentDataRef(uploadItemRef);
        } catch (ExecutionException e) {
          throw new IOException(e);
        }
      }
      return itemContent;
    }

    <T> T execute(CloudSearchRequest<T> request) throws IOException {
      printHttpRequest(request);
      return request.execute();
    }

    void printHttpRequest(CloudSearchRequest<?> request) {
      String requestMethod = request.getRequestMethod();
      String url = request.buildHttpRequestUrl().buildRelativeUrl();
      System.out.println(requestMethod + " " + url);
    }

    private byte[] convertStreamToByteArray(AbstractInputStreamContent content) throws IOException {
      try (InputStream is = content.getInputStream()) {
        return ByteStreams.toByteArray(is);
      }
    }

    private String getItemResourceName(String sourceId, String name) {
      return String.format(
          ITEM_RESOURCE_NAME_FORMAT, sourceId, escapeResourceName(name));
    }

    private String escapeResourceName(String name) {
      return name.replaceAll("/", "%2F");
    }
  }

  class UrlInputStreamContent extends AbstractInputStreamContent {

    private final URL url;
    private URLConnection urlConnection;
    private final long length;

    public UrlInputStreamContent(String type, URL url) throws IOException {
      super(type);
      this.url = url;
      this.urlConnection = url.openConnection();
      this.length =
          urlConnection.getContentLengthLong() == -1L ? 0L : urlConnection.getContentLengthLong();
    }

    @Override
    public InputStream getInputStream() throws IOException {
      if (urlConnection == null) {
        urlConnection = url.openConnection();
      }

      InputStream inputStream = urlConnection.getInputStream();
      urlConnection = null;
      return inputStream;
    }

    @Override
    public long getLength() throws IOException {
      return length;
    }

    @Override
    public boolean retrySupported() {
      return true;
    }
  }

  public static void main(String[] args)
      throws IOException, GeneralSecurityException, InterruptedException {
    String jsonFile = System.getProperty("payload");
    checkNotNull(jsonFile, "Missing input json file for the requests");
    checkNotNull(System.getProperty(SERVICE_ACCOUNT_KEY_FILE_CONFIG),
        "Missing " + SERVICE_ACCOUNT_KEY_FILE_CONFIG);

    //resolve the relative path of json file
    Path userDir = Paths.get(System.getProperty("user.dir"));
    Path jsonFilePath = userDir.resolve(jsonFile);
    Path serviceAccountKeyFilePath = userDir
        .resolve(System.getProperty(SERVICE_ACCOUNT_KEY_FILE_CONFIG));

    int requestTimeout = DEFAULT_REQUEST_TIMEOUT_SECONDS;
    if (!Strings.isNullOrEmpty(System.getProperty("contentUpload.requestTimeout"))) {
      requestTimeout = Integer.parseInt(System.getProperty("contentUpload.requestTimeout"));
    }

    boolean enableDebugging = Boolean.getBoolean("contentUpload.enableDebugging");
    String connectorName = Uploader.class.getName();
    if (!Strings.isNullOrEmpty(System.getProperty("contentUpload.connectorName"))) {
      connectorName = System.getProperty("contentUpload.connectorName");
    }

    if (!Files.exists(jsonFilePath)) {
      throw new IOException("payload file " + jsonFilePath.toAbsolutePath() + " does not exists");
    } else if (Files.isDirectory(jsonFilePath)) {
      throw new IOException(
          "payload " + jsonFilePath.toAbsolutePath() + " is a directory. A json file is expected.");
    } else if (!Files.isReadable(jsonFilePath)) {
      throw new IOException("payload " + jsonFilePath.toAbsolutePath() + " is not readable");
    }

    URI jsonUri = jsonFilePath.toUri();
    UploadRequest uploadRequest =
        Uploader.getUploadRequest(
            new InputStreamReader(jsonUri.toURL().openStream(), defaultCharset()));
    checkNotNull(uploadRequest, "upload request is null");

    Uploader uploader =
        new Uploader.Builder()
            .setUploaderHelper(UploaderHelper.getInstance())
            .setRootUrl(System.getProperty("rootUrl", ""))
            .setBaseUri(userDir.toUri())
            .setServiceAccountKeyFilePath(serviceAccountKeyFilePath)
            .setRequestTimeout(requestTimeout, requestTimeout)
            .setEnableDebugging(enableDebugging)
            .setConnectorName(connectorName)
            .build();
    //for each request, use cloudSearch service to upload it
    uploader.execute(uploadRequest);
  }
}
