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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.GetSchema;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Delete;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.DeleteQueueItems;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Get;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Index;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Poll;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Push;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Unreserve;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.Upload;
import com.google.api.services.cloudsearch.v1.CloudSearchRequest;
import com.google.api.services.cloudsearch.v1.model.DebugOptions;
import com.google.api.services.cloudsearch.v1.model.DeleteQueueItemsRequest;
import com.google.api.services.cloudsearch.v1.model.IndexItemOptions;
import com.google.api.services.cloudsearch.v1.model.IndexItemRequest;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemContent;
import com.google.api.services.cloudsearch.v1.model.ListItemsResponse;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.api.services.cloudsearch.v1.model.PollItemsResponse;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.PushItemRequest;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.StartUploadItemRequest;
import com.google.api.services.cloudsearch.v1.model.UnreserveItemsRequest;
import com.google.api.services.cloudsearch.v1.model.UploadItemRef;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.escape.Escaper;
import com.google.common.io.ByteStreams;
import com.google.common.net.UrlEscapers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ServiceManager;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.BaseApiService;
import com.google.enterprise.cloudsearch.sdk.BatchPolicy;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.GoogleProxy;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.LocalFileCredentialFactory;
import com.google.enterprise.cloudsearch.sdk.QuotaServer;
import com.google.enterprise.cloudsearch.sdk.RetryPolicy;
import com.google.enterprise.cloudsearch.sdk.StatsManager;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

/**
 * Wrapper object for {@link Indexing}.
 *
 * <p>This is the access point between the connector developer and the indexing service. All calls
 * for updating and retrieving {@link
 * com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items} objects are
 * processed through this object.
 *
 * <p>Configuration parameters:
 *
 * <ul>
 *   <li>{@value #ROOT_URL} - Specifies the indexing service base URL path.
 *   <li>{@value #SOURCE_ID} - Specifies the data source ID (created via the admin console)
 *       indicating the location for the ingested repository documents.
 *   <li>{@value #IDENTITY_SOURCE_ID} - Specifies the identity source ID (created via the admin
 *       console) to use for users and groups that are attached to ACL definitions.
 *   <li>{@value #CONNECTOR_ID} - Specifies the connector identifier for making Indexing API
 *   <li>{@value #UPLOAD_THRESHOLD_BYTES} - Specifies the threshold for content, in number of bytes,
 *       that determines whether it is uploaded "in-line" with the other document info or using a
 *       separate upload.
 *   <li>{@value #INDEXING_SERVICE_REQUEST_MODE} - Specifies the default request mode for index and
 *       delete item requests
 * </ul>
 */
public class IndexingServiceImpl extends BaseApiService<CloudSearch> implements IndexingService {
  private static final Logger logger = Logger.getLogger(IndexingServiceImpl.class.getName());

  public static final String ROOT_URL = "api.rootUrl";
  public static final String SOURCE_ID = "api.sourceId";
  public static final String IDENTITY_SOURCE_ID = "api.identitySourceId";
  public static final String CONNECTOR_ID = "api.connectorId";
  public static final String UPLOAD_THRESHOLD_BYTES = "api.contentUploadThresholdBytes";
  public static final String INDEXING_SERVICE_REQUEST_MODE = "api.defaultRequestMode";
  public static final String REQUEST_CONNECT_TIMEOUT = "indexingService.connectTimeoutSeconds";
  public static final String REQUEST_READ_TIMEOUT = "indexingService.readTimeoutSeconds";
  public static final String ENABLE_API_DEBUGGING = "indexingService.enableDebugging";
  public static final String ALLOW_UNKNOWN_GSUITE_PRINCIPALS =
      "indexingService.allowUnknownGsuitePrincipals";

  private static final OperationStats indexingServiceStats =
      StatsManager.getComponent("IndexingService");
  private static final String RESOURCE_NAME_FORMAT = "datasources/%s";
  private static final String DATA_SOURCES_PREFIX = "datasources";
  private static final String ITEM_RESOURCE_NAME_FORMAT = RESOURCE_NAME_FORMAT + "/items/%s";
  private static final String CONNECTOR_NAME_FORMAT = RESOURCE_NAME_FORMAT + "/connectors/%s";
  private static final int DEFAULT_CONNECT_TIMEOUT_SECONDS = 120;
  private static final int DEFAULT_READ_TIMEOUT_SECONDS = 120;

  public static final int DEFAULT_CONTENT_UPLOAD_THRESHOLD_BYTES = 100000; // ~100KB
  public static final Set<String> API_SCOPES =
      Collections.singleton("https://www.googleapis.com/auth/cloud_search");
  private static final Escaper URL_PATH_SEGMENT_ESCAPER = UrlEscapers.urlPathSegmentEscaper();
  private static final RequestMode DEFAULT_REQUEST_MODE = RequestMode.SYNCHRONOUS;

  private final String sourceId;
  private final String identitySourceId;
  private final String resourcePrefix;
  private final String itemResourcePrefix;
  private final String connectorName;

  private final BatchingIndexingService batchingService;
  private final ContentUploadService contentUploadService;
  private final ServiceManager serviceManager;
  private final ServiceManagerHelper serviceManagerHelper;
  private final int contentUploadThreshold;
  private final VersionProvider versionProvider;
  private final QuotaServer<Operations> quotaServer;
  private final RequestMode requestMode;
  private final boolean enableApiDebugging;
  private final boolean allowUnknownGsuitePrincipals;

  /** API Operations */
  public enum Operations {
    DEFAULT
  }

  /** Valid values for poll {@link Item} status. */
  public enum PollItemStatus {
    /** API has accepted the up-to-date data of this item. */
    ACCEPTED("ACCEPTED"),
    /**
     * {@link Item} has been modified in the repository, and is out of date with the version
     * previously accepted into Cloud Search.
     */
    MODIFIED("MODIFIED"),
    /**
     * {@link Item} is known to exist in the repository, but is not yet accepted by Cloud Search.
     */
    NEW_ITEM("NEW_ITEM"),
    /** Error encountered by Cloud Search while processing this item. */
    SERVER_ERROR("ERROR");

    static final List<String> allStatus =
        ImmutableList.of(
            ACCEPTED.toString(), MODIFIED.toString(), NEW_ITEM.toString(), SERVER_ERROR.toString());

    private String value;

    PollItemStatus(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return this.value;
    }

    /**
     * Verifies status strings.
     *
     * @param status the list of status strings to check.
     * @return a list of any invalid status strings, or empty set if all valid.
     */
    public static List<String> getBadStatus(List<String> status) {
      if (status == null) {
        return ImmutableList.of();
      }
      List<String> badStatus = new ArrayList<String>(status);
      badStatus.removeAll(allStatus);
      return badStatus;
    }
  }

  /**
   * Builder instantiated by the connector framework before allowing access to the indexing methods.
   *
   * @param builder Builder object.
   */
  private IndexingServiceImpl(Builder builder) {
    super(builder);
    this.sourceId = builder.sourceId;
    this.identitySourceId = builder.identitySourceId;
    this.resourcePrefix = String.format(RESOURCE_NAME_FORMAT, this.sourceId);
    this.itemResourcePrefix = resourcePrefix + "/items/";
    this.connectorName =
        String.format(CONNECTOR_NAME_FORMAT, builder.sourceId, builder.connectorId);
    this.batchingService =
        checkNotNull(builder.batchingService, "batching service can not be null");
    this.contentUploadService =
        checkNotNull(builder.contentUploadService, "content upload service can not be null");
    this.serviceManagerHelper = builder.serviceManagerHelper;
    this.serviceManager =
        serviceManagerHelper.getServiceManager(
            Arrays.asList(batchingService, contentUploadService));
    this.contentUploadThreshold = builder.contentUploadThreshold;
    this.versionProvider = builder.versionProvider;
    this.quotaServer = builder.quotaServer;
    this.requestMode = builder.requestMode;
    this.enableApiDebugging = builder.enableApiDebugging;
    this.allowUnknownGsuitePrincipals = builder.allowUnknownGsuitePrincipals;
  }

  public static class Builder extends BaseApiService.AbstractBuilder<Builder, CloudSearch> {
    private String sourceId;
    private String identitySourceId;
    private String connectorId;
    private BatchingIndexingService batchingService;
    private ContentUploadService contentUploadService;
    private ServiceManagerHelper serviceManagerHelper = new ServiceManagerHelper();
    private int contentUploadThreshold = DEFAULT_CONTENT_UPLOAD_THRESHOLD_BYTES;
    private VersionProvider versionProvider =
        () -> String.valueOf(System.currentTimeMillis()).getBytes();
    private QuotaServer<Operations> quotaServer =
        new QuotaServer.Builder<>(Operations.class).build();
    private RequestMode requestMode = DEFAULT_REQUEST_MODE;
    private int contentUploadConnectTimeoutSeconds = DEFAULT_CONNECT_TIMEOUT_SECONDS;
    private int contentUploadReadTimeoutSeconds = DEFAULT_READ_TIMEOUT_SECONDS;
    private boolean enableApiDebugging;
    private boolean allowUnknownGsuitePrincipals;

    public Builder setSourceId(String sourceId) {
      this.sourceId = sourceId;
      return this;
    }

    public Builder setIdentitySourceId(String identitySourceId) {
      this.identitySourceId = identitySourceId;
      return this;
    }

    public Builder setContentUploadThreshold(int thresold) {
      this.contentUploadThreshold = thresold;
      return this;
    }

    public Builder setQuotaServer(QuotaServer<Operations> quotaServer) {
      this.quotaServer = quotaServer;
      return this;
    }

    public Builder setRequestMode(RequestMode requestMode) {
      this.requestMode = requestMode;
      return this;
    }

    public Builder setConnectorId(String connectorId) {
      this.connectorId = connectorId;
      return this;
    }

    public Builder setEnableDebugging(boolean enableDebugging) {
      this.enableApiDebugging = enableDebugging;
      return this;
    }

    public Builder setAllowUnknownGsuitePrincipals(boolean allowUnknownGsuitePrincipals) {
      this.allowUnknownGsuitePrincipals = allowUnknownGsuitePrincipals;
      return this;
    }

    public Builder setContentUploadRequestTimeout(
        int connectTimeoutSeconds, int readTimeoutSeconds) {
      this.contentUploadConnectTimeoutSeconds = connectTimeoutSeconds;
      this.contentUploadReadTimeoutSeconds = readTimeoutSeconds;
      return this;
    }

    // TODO(bmj): revoke public after refactoring sdk.ConnectorTraverser
    @VisibleForTesting
    public Builder setBatchingIndexingService(BatchingIndexingService batchingService) {
      this.batchingService = batchingService;
      return this;
    }

    // TODO(bmj): revoke public after refactoring sdk.ConnectorTraverser
    @VisibleForTesting
    public Builder setContentUploadService(ContentUploadService contentUploadService) {
      this.contentUploadService = contentUploadService;
      return this;
    }

    // TODO(bmj): revoke public after refactoring sdk.ConnectorTraverser
    @VisibleForTesting
    public Builder setServiceManagerHelper(ServiceManagerHelper serviceManagerHelper) {
      this.serviceManagerHelper = serviceManagerHelper;
      return this;
    }

    @VisibleForTesting
    Builder setVersionProvider(VersionProvider versionProvider) {
      this.versionProvider = versionProvider;
      return this;
    }

    @Override
    public IndexingServiceImpl build() throws IOException, GeneralSecurityException {
      checkArgument(!Strings.isNullOrEmpty(sourceId), "Source ID cannot be null.");
      checkArgument(!Strings.isNullOrEmpty(identitySourceId), "Identity Source ID cannot be null.");
      checkNotNull(serviceManagerHelper, "Service Manager Helper can not be null");
      checkNotNull(versionProvider, "Version Provider can not be null");
      checkArgument(contentUploadThreshold >= 0, "Content Upload Threshold can not be less than 0");
      checkNotNull(quotaServer, "quota server can not be null");
      checkNotNull(retryPolicy, "retry policy can not be null");
      checkNotNull(requestMode, "request mode can not be null");
      checkArgument(
          requestMode != RequestMode.UNSPECIFIED, "default request mode can not be UNSPECIFIED");
      checkArgument(!Strings.isNullOrEmpty(connectorId), "Connector ID cannot be null or empty.");

      GoogleCredential credential = setupServiceAndCredentials();
      if (batchingService == null) {
        CloudSearch.Builder serviceBuilder =
            new CloudSearch.Builder(transport, jsonFactory, credential);
        if (!Strings.isNullOrEmpty(rootUrl)) {
          serviceBuilder.setRootUrl(rootUrl);
        }
        batchingService =
            new BatchingIndexingServiceImpl.Builder()
                .setService(
                    serviceBuilder
                        .setApplicationName(BatchingIndexingServiceImpl.class.getName())
                        .build())
                .setBatchPolicy(checkNotNull(batchPolicy, "batch policy can not be null"))
                .setRetryPolicy(checkNotNull(retryPolicy, "retry policy can not be null"))
                .setCredential(credential)
                .build();
      }
      if (contentUploadService == null) {
        contentUploadService =
            new ContentUploadServiceImpl.Builder()
                .setCredentialFactory(credentialFactory)
                .setRequestInitializer(requestInitializer)
                .setRootUrl(rootUrl)
                .setRetryPolicy(retryPolicy)
                .setProxy(googleProxy)
                .setRequestTimeout(
                    contentUploadConnectTimeoutSeconds, contentUploadReadTimeoutSeconds)
                .build();
      }
      return new IndexingServiceImpl(this);
    }

    /**
     * Generates a {@link IndexingServiceImpl.Builder} instance from configuration parameters.
     *
     * <p>This method returns a fully initialized builder object for an {@link IndexingServiceImpl}
     * instance created from defaulted values and configuration parameters. The caller can
     * optionally use setter methods to make changes on the builder before creating the final
     * instance by calling {@link Builder#build()}.
     */
    public static IndexingServiceImpl.Builder fromConfiguration(
        Optional<CredentialFactory> credentialFactory, String defaultConnectorName) {

      RequestMode requestModeConfigured;
      try {
        requestModeConfigured =
            Configuration.getValue(
                    INDEXING_SERVICE_REQUEST_MODE, DEFAULT_REQUEST_MODE, RequestMode::valueOf)
                .get();
      } catch (IllegalArgumentException e) {
        throw new InvalidConfigurationException("Unable to parse configured request mode", e);
      }
      int connectTimeoutSeconds =
          Configuration.getInteger(REQUEST_CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT_SECONDS).get();
      Configuration.checkConfiguration(
          connectTimeoutSeconds >= 0,
          "Invalid connect timeout value [%s] for configuration key [%s]",
          connectTimeoutSeconds,
          REQUEST_CONNECT_TIMEOUT);
      int readTimeoutSeconds =
          Configuration.getInteger(REQUEST_READ_TIMEOUT, DEFAULT_READ_TIMEOUT_SECONDS).get();
      Configuration.checkConfiguration(
          readTimeoutSeconds >= 0,
          "Invalid read timeout value [%s] for configuration key [%s]",
          readTimeoutSeconds,
          REQUEST_READ_TIMEOUT);
      boolean enableApiDebugging = Configuration.getBoolean(ENABLE_API_DEBUGGING, false).get();
      boolean allowUnknownGsuitePrincipals =
          Configuration.getBoolean(ALLOW_UNKNOWN_GSUITE_PRINCIPALS, false).get();

      return new IndexingServiceImpl.Builder()
          .setSourceId(Configuration.getString(SOURCE_ID, null).get())
          .setIdentitySourceId(Configuration.getString(IDENTITY_SOURCE_ID, "NOT_APPLICABLE").get())
          .setCredentialFactory(
              credentialFactory.isPresent()
                  ? credentialFactory.get()
                  : LocalFileCredentialFactory.fromConfiguration())
          .setJsonFactory(JacksonFactory.getDefaultInstance())
          .setQuotaServer(QuotaServer.createFromConfiguration("indexingService", Operations.class))
          .setProxy(GoogleProxy.fromConfiguration())
          .setRootUrl(Configuration.getString(ROOT_URL, "").get())
          .setBatchPolicy(BatchPolicy.fromConfiguration())
          .setRetryPolicy(RetryPolicy.fromConfiguration())
          .setRequestMode(requestModeConfigured)
          .setConnectorId(Configuration.getString("api.connectorId", defaultConnectorName).get())
          .setRequestTimeout(connectTimeoutSeconds, readTimeoutSeconds)
          .setContentUploadRequestTimeout(connectTimeoutSeconds, readTimeoutSeconds)
          .setContentUploadThreshold(
              Configuration.getInteger(
                      UPLOAD_THRESHOLD_BYTES,
                      IndexingServiceImpl.DEFAULT_CONTENT_UPLOAD_THRESHOLD_BYTES)
                  .get())
          .setEnableDebugging(enableApiDebugging)
          .setAllowUnknownGsuitePrincipals(allowUnknownGsuitePrincipals);
    }

    @Override
    public Builder getThis() {
      return this;
    }

    @Override
    public Set<String> getApiScopes() {
      return API_SCOPES;
    }

    @Override
    public com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient.Builder
        getServiceBuilder(
            HttpTransport transport,
            JsonFactory jsonFactory,
            HttpRequestInitializer requestInitializer) {
      return new CloudSearch.Builder(transport, jsonFactory, requestInitializer);
    }
  }

  /*
  Item methods.
  */

  /**
   * Deletes an {@link Item}.
   *
   * <p>Cloud Search won't delete an item if the passed version value is less than the currently
   * indexed item's version.
   *
   * @param id the item id.
   * @param version the item's version used to compare against the previously indexed item's version
   * @param requestMode mode for delete request
   * @return {@link ListenableFuture} that the caller uses to obtain the result of a delete
   *     operation (using {@link ListenableFuture#get()}).
   * @throws IOException when service throws an exception.
   */
  @Override
  public ListenableFuture<Operation> deleteItem(String id, byte[] version, RequestMode requestMode)
      throws IOException {
    validateRunning();
    checkArgument(!Strings.isNullOrEmpty(id), "Item ID cannot be null.");
    Delete deleteRequest =
        this.service
            .indexing()
            .datasources()
            .items()
            .delete(getItemResourceName(id))
            .setMode(getRequestMode(requestMode))
            .setConnectorName(connectorName)
            .setDebugOptionsEnableDebugging(enableApiDebugging);
    deleteRequest.setVersion(
        Base64.getEncoder()
            .encodeToString((version != null) ? version : versionProvider.getVersion()));
    try {
      acquireToken(Operations.DEFAULT);
      return batchingService.deleteItem(deleteRequest);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted while batching delete request", e);
      Thread.currentThread().interrupt();
      return getInterruptedFuture(e);
    }
  }

  /**
   * Deletes items from a queue.
   *
   * @param queueName the queue name
   * @return {@link ListenableFuture} that the caller uses to obtain the result of a delete queue
   *     items operation (using {@link ListenableFuture#get()}).
   * @throws IOException when the service throws an exception
   */
  @Override
  public ListenableFuture<Operation> deleteQueueItems(String queueName) throws IOException {
    validateRunning();
    checkArgument(!Strings.isNullOrEmpty(queueName), "Queue name cannot be null.");

    DeleteQueueItems request =
        this.service
            .indexing()
            .datasources()
            .items()
            .deleteQueueItems(
                resourcePrefix,
                new DeleteQueueItemsRequest()
                    .setQueue(queueName)
                    .setDebugOptions(new DebugOptions().setEnableDebugging(enableApiDebugging)));
    acquireToken(Operations.DEFAULT);
    try {
      return Futures.immediateFuture(
          executeRequest(request, indexingServiceStats, true /* initializeDefaults */));
    } catch (IOException e) {
      return Futures.immediateFailedFuture(e);
    }
  }

  /**
   * Gets an {@link Item}.
   *
   * @param id the item ID
   * @return the item or {@code null} if not found
   * @throws IOException when the service throws an exception
   */
  @Override
  public Item getItem(String id) throws IOException {
    validateRunning();
    checkArgument(!Strings.isNullOrEmpty(id), "Item ID cannot be null.");
    Get getRequest =
        this.service
            .indexing()
            .datasources()
            .items()
            .get(getItemResourceName(id))
            .setConnectorName(connectorName)
            .setDebugOptionsEnableDebugging(enableApiDebugging);
    acquireToken(Operations.DEFAULT);
    return executeRequestReturnNullOnNotFound(getRequest);
  }

  /**
   * Fetches the first of what may be many sets of {@link Item}.
   *
   * @param brief {@code true} to shorten {@link Item} metadata, default: {@code true}
   * @return an iterator for the returned set of {@link Item}
   * @throws IOException when the service throws an exception
   */
  @Override
  public Iterable<Item> listItem(boolean brief) throws IOException {
    validateRunning();
    return new ListItemIterable(brief);
  }

  /**
   * Fetches the first/next set of {@link Item}.
   *
   * @param token used to determine if this is a continuation or a first call
   * @param brief {@code true} to shorten {@link Item} metadata, default: {@code true}
   * @return the response {@link ListItemsResponse} containing the {@link Item} and page token
   * @throws IOException when the service throws an exception
   */
  private ListItemsResponse fetchNextItems(String token, boolean brief) throws IOException {
    validateRunning();
    com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items.List listRequest =
        this.service
            .indexing()
            .datasources()
            .items()
            .list(resourcePrefix)
            .setConnectorName(connectorName)
            .setDebugOptionsEnableDebugging(enableApiDebugging);
    listRequest.setBrief(brief);
    if (token != null) {
      listRequest.setPageToken(token);
    }
    acquireToken(Operations.DEFAULT);
    return executeRequest(listRequest, indexingServiceStats, true /* initializeDefaults */);
  }

  /**
   * Used to iterate over the {@link Item} during a {@link IndexingServiceImpl#listItem(boolean)}
   * call.
   */
  private class ListItemIterable implements Iterable<Item> {

    private ListItemIterator listItemIterator;

    private ListItemIterable(boolean brief) {
      this.listItemIterator = new ListItemIterator(brief);
    }

    @Override
    public java.util.Iterator<Item> iterator() {
      return this.listItemIterator;
    }

    /**
     * Iterator for the list of {@link Item}.
     *
     * <p>Fetches {@link Item} from the service in groups. When the list is exhausted, the next set
     * is fetched using the previously returned page token received from the service.
     */
    private class ListItemIterator implements Iterator<Item> {

      private List<Item> items = new ArrayList<Item>();
      private Iterator<Item> internalIterator = null; // null indicates no fetches yet
      private String pageToken = null; // null indicates non-continuation
      private final boolean brief;

      private ListItemIterator(boolean brief) {
        this.brief = brief;
      }

      private void fetchNext() {
        ListItemsResponse listItemsResponse;
        try {
          listItemsResponse = fetchNextItems(pageToken, brief);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        this.items.clear();
        if (listItemsResponse.getItems() != null) {
          this.items.addAll(listItemsResponse.getItems());
        }
        this.internalIterator = this.items.iterator();
        this.pageToken = listItemsResponse.getNextPageToken();
      }

      @Override
      public boolean hasNext() {
        // first time only to prime the pump
        if (this.internalIterator == null) {
          fetchNext();
          return this.internalIterator.hasNext();
        }
        // all the subsequent fetches (normal case)
        if (this.internalIterator.hasNext()) {
          return true;
        } else if (pageToken == null) {
          return false;
        } else {
          fetchNext();
          return this.internalIterator.hasNext();
        }
      }

      @Override
      public Item next() {
        if (this.hasNext()) {
          return this.internalIterator.next();
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Updates an {@link Item}.
   *
   * @param item the item to update
   * @param requestMode the {@link IndexingService.RequestMode} for the request
   * @return {@link ListenableFuture}. Caller can use {@link ListenableFuture#get()} to obtain a
   *     result of an update operation
   * @throws IOException when the service throws an exception
   */
  @Override
  public ListenableFuture<Operation> indexItem(Item item, RequestMode requestMode)
      throws IOException {
    validateRunning();
    checkArgument(item != null, "Item cannot be null.");
    checkArgument(!Strings.isNullOrEmpty(item.getName()), "Item name cannot be null.");
    addResourcePrefix(item);
    if (item.decodeVersion() == null) {
      item.encodeVersion(versionProvider.getVersion());
    }
    Index updateRequest =
        service
            .indexing()
            .datasources()
            .items()
            .index(
                item.getName(),
                new IndexItemRequest()
                    .setDebugOptions(new DebugOptions().setEnableDebugging(enableApiDebugging))
                    .setIndexItemOptions(new IndexItemOptions()
                        .setAllowUnknownGsuitePrincipals(allowUnknownGsuitePrincipals))
                    .setItem(item)
                    .setMode(getRequestMode(requestMode))
                    .setConnectorName(connectorName));
    try {
      acquireToken(Operations.DEFAULT);
      return batchingService.indexItem(updateRequest);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted while batching update request", e);
      Thread.currentThread().interrupt();
      return getInterruptedFuture(e);
    }
  }

  /**
   * Updates an {@link Item}.
   *
   * <p>The {@code content} parameter should use a concrete implementation of {@code
   * AbstractInputStreamContent} based on the natural source object:
   *
   * <ul>
   *   <li>For {@code InputStream}, use {@code InputStreamContent}. For best results, if the length
   *       of the content (in bytes) is known without reading the stream, call {@code setLength} on
   *       the {@code InputStreamContent}.
   *   <li>For {@code String} or {@code byte[]}, use {@code ByteArrayContent}.
   *   <li>For existing files, use {@code FileContent}.
   * </ul>
   *
   * @param item the item to update
   * @param content the item's content
   * @param contentHash the hash of the item's content
   * @param contentFormat format of the content
   * @param requestMode the {@link IndexingService.RequestMode} for the request
   * @return {@link ListenableFuture}. Caller can use {@link ListenableFuture#get()} to obtain the
   *     result of an update operation
   * @throws IOException when the service throws an exception
   */
  @Override
  public ListenableFuture<Operation> indexItemAndContent(
      Item item,
      AbstractInputStreamContent content,
      @Nullable String contentHash,
      ContentFormat contentFormat,
      RequestMode requestMode)
      throws IOException {
    validateRunning();
    checkArgument(item != null, "Item cannot be null.");
    checkArgument(!Strings.isNullOrEmpty(item.getName()), "Item ID cannot be null.");
    checkNotNull(content, "Item content cannot be null.");
    long length = content.getLength();
    boolean useInline = (length <= contentUploadThreshold) && (length >= 0);
    if (useInline) {
      logger.log(
          Level.FINEST,
          "Inlining content for {0}, length {1} bytes.",
          new Object[] {item.getName(), length});
      item.setContent(
          new ItemContent()
              .encodeInlineContent(convertStreamToByteArray(content))
              .setHash(contentHash)
              .setContentFormat(contentFormat.name()));
      return indexItem(item, requestMode);
    } else {
      UploadItemRef uploadRef = startUpload(item.getName());
      logger.log(
          Level.FINEST,
          "Uploading content for {0}, length {1} bytes, upload ref {2}",
          new Object[] {item.getName(), length, uploadRef.getName()});

      ListenableFuture<Item> itemUploaded =
          Futures.transform(
              contentUploadService.uploadContent(uploadRef.getName(), content),
              voidVal ->
                  item.setContent(
                      new ItemContent()
                          .setContentDataRef(uploadRef)
                          .setHash(contentHash)
                          .setContentFormat(contentFormat.name())),
              MoreExecutors.directExecutor());

      return Futures.transformAsync(
          itemUploaded, i -> indexItem(i, requestMode), MoreExecutors.directExecutor());
    }
  }

  /**
   * Polls the queue using custom API parameters.
   *
   * @param pollQueueRequest the user created and populated poll request
   * @return entries returned from the queue
   * @throws IOException when the service throws an exception
   */
  @Override
  public List<Item> poll(PollItemsRequest pollQueueRequest) throws IOException {
    validateRunning();
    List<String> badStatus = PollItemStatus.getBadStatus(pollQueueRequest.getStatusCodes());
    checkArgument(badStatus.isEmpty(), "Invalid Entry status: " + badStatus.toString());
    // We are modifying input here instead of cloning. GenericJson.clone fails if GenericJson has
    // immutable collections as values. Input pollQueueRequest can have List of status values as
    // ImmutableList.
    pollQueueRequest.setConnectorName(connectorName);
    Poll pollRequest =
        this.service.indexing().datasources().items().poll(resourcePrefix, pollQueueRequest);
    acquireToken(Operations.DEFAULT);
    PollItemsResponse pollResponse =
        executeRequest(pollRequest, indexingServiceStats, true /* intializeDefaults */);
    List<Item> polled =
        pollResponse.getItems() == null ? Collections.emptyList() : pollResponse.getItems();
    polled.forEach(p -> removeResourcePrefix(p));
    return polled;
  }

  private class PollItemIterable implements Iterable<Item> {

    private PollItemIterator pollItemIterator;

    private PollItemIterable(PollItemsRequest pollQueueRequest) {
      this.pollItemIterator = new PollItemIterator(pollQueueRequest);
    }

    @Override
    public java.util.Iterator<Item> iterator() {
      return this.pollItemIterator;
    }

    private class PollItemIterator implements Iterator<Item> {
      private List<Item> currentBatch;
      private Iterator<Item> currentIterator;
      private PollItemsRequest pollRequest;

      public PollItemIterator(PollItemsRequest pollRequest) {
        this.pollRequest = pollRequest;
      }

      private void fetchNext() {
        try {
          currentBatch = poll(pollRequest);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        currentIterator = currentBatch.iterator();
      }

      @Override
      public boolean hasNext() {
        if (this.currentIterator == null) {
          fetchNext();
          return this.currentIterator.hasNext();
        }
        if (currentIterator.hasNext()) {
          return true;
        } else {
          fetchNext();
          return this.currentIterator.hasNext();
        }
      }

      @Override
      public Item next() {
        if (hasNext()) {
          return currentIterator.next();
        } else {
          throw new NoSuchElementException();
        }
      }
    }
  }

  /**
   * Polls the queue repeatedly until the entire queue is polled.
   *
   * @param pollQueueRequest the user created and populated poll request
   * @return an iterator for entries returned from the queue
   * @throws IOException when the service throws an exception
   */
  @Override
  public Iterable<Item> pollAll(PollItemsRequest pollQueueRequest) throws IOException {
    return new PollItemIterable(pollQueueRequest);
  }

  /**
   * Pushes a {@link PushItem} object to the indexing API Queue.
   *
   * @param pushItem the item to push
   * @return {@link ListenableFuture}. Caller can use {@link ListenableFuture#get()} to obtain the
   *     result of a push operation
   * @throws IOException when the service throws an exception
   */
  @Override
  public ListenableFuture<Item> push(String id, PushItem pushItem) throws IOException {
    validateRunning();
    checkArgument(!Strings.isNullOrEmpty(id), "id can not be null or empty");
    checkArgument(pushItem != null, "Push item cannot be null.");
    String resourceName = getItemResourceName(id);
    Push request =
        this.service
            .indexing()
            .datasources()
            .items()
            .push(
                resourceName,
                new PushItemRequest()
                    .setItem(pushItem)
                    .setConnectorName(connectorName)
                    .setDebugOptions(new DebugOptions().setEnableDebugging(enableApiDebugging)));
    try {
      acquireToken(Operations.DEFAULT);
      return batchingService.pushItem(request);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted while batching push request", e);
      Thread.currentThread().interrupt();
      return getInterruptedFuture(e);
    }
  }

  /**
   * Unreserves the polled {@link Item} objects in a specific queue.
   *
   * @param queue the queue to unreserve ({@code null} for the default queue)
   * @return {@link ListenableFuture}. Caller can use {@link ListenableFuture#get()} to obtain the
   *     result of an unreserve operation
   * @throws IOException when the service throws an exception
   */
  @Override
  public ListenableFuture<Operation> unreserve(String queue) throws IOException {
    validateRunning();
    UnreserveItemsRequest unreserveQueueRequest =
        new UnreserveItemsRequest()
            .setQueue(queue)
            .setConnectorName(connectorName)
            .setDebugOptions(new DebugOptions().setEnableDebugging(enableApiDebugging));
    Unreserve unreserveRequest =
        this.service
            .indexing()
            .datasources()
            .items()
            .unreserve(resourcePrefix, unreserveQueueRequest);
    try {
      acquireToken(Operations.DEFAULT);
      return batchingService.unreserveItem(unreserveRequest);
    } catch (InterruptedException e) {
      logger.log(Level.WARNING, "Interrupted while batching unreserve request", e);
      Thread.currentThread().interrupt();
      return getInterruptedFuture(e);
    }
  }

  private void validateRunning() {
    checkState(isRunning(), "Indexing Service should be running to make API calls");
  }

  private double acquireToken(Operations o) {
    return quotaServer.acquire(o);
  }

  private String getRequestMode(RequestMode requestMode) {
    return requestMode == RequestMode.UNSPECIFIED ? this.requestMode.name() : requestMode.name();
  }

  /**
   * Performs an {@link IndexingServiceImpl#executeRequest} but tracks "not found" exceptions to
   * convert them to a {@code null} return value.
   *
   * @param request the request to perform
   * @return the response result from executing the request or {@code null} if "not found" error
   * @throws IOException when the service throws an exception
   */
  private <T> T executeRequestReturnNullOnNotFound(CloudSearchRequest<T> request)
      throws IOException {
    try {
      return executeRequest(request, indexingServiceStats, true /* intializeDefaults */);
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == HTTP_NOT_FOUND) {
        return null;
      }
      throw e;
    }
  }

  private byte[] convertStreamToByteArray(AbstractInputStreamContent content) throws IOException {
    try (InputStream is = content.getInputStream()) {
      return ByteStreams.toByteArray(is);
    }
  }

  private void addResourcePrefix(Item item) {
    item.setName(getItemResourceName(item.getName()));
    if (item.getAcl() != null) {
      if (item.getAcl().getInheritAclFrom() != null) {
        item.getAcl().setInheritAclFrom(getItemResourceName(item.getAcl().getInheritAclFrom()));
      }
      addResourcePrefix(item.getAcl().getReaders());
      addResourcePrefix(item.getAcl().getDeniedReaders());
      addResourcePrefix(item.getAcl().getOwners());
    }
    if ((item.getMetadata() != null) && (item.getMetadata().getContainerName() != null)) {
      item.getMetadata()
          .setContainerName(getItemResourceName(item.getMetadata().getContainerName()));
    }
  }

  private void addResourcePrefix(List<Principal> principals) {
    if (principals == null) {
      return;
    }
    for (Principal p : principals) {
      Acl.PrincipalType type = Acl.getPrincipalType(p);
      switch (type) {
        case USER:
          {
            Acl.addResourcePrefixUser(p, identitySourceId);
            break;
          }

        case GROUP:
          {
            Acl.addResourcePrefixGroup(p, identitySourceId);
            break;
          }

        default:
          {
            break;
          }
      }
    }
  }

  private static <T> SettableFuture<T> getInterruptedFuture(InterruptedException e) {
    SettableFuture<T> interrupted = SettableFuture.create();
    interrupted.setException(e);
    return interrupted;
  }

  private String getItemResourceName(String name) {
    checkArgument(!Strings.isNullOrEmpty(name), "item name can not be empty or null");
    if (name.startsWith(DATA_SOURCES_PREFIX)) {
      return name;
    }
    return String.format(ITEM_RESOURCE_NAME_FORMAT, sourceId, escapeResourceName(name));
  }

  private static String escapeResourceName(String name) {
    return URL_PATH_SEGMENT_ESCAPER.escape(name);
  }

  private static String decodeResourceName(String name) {
    try {
      return URLDecoder.decode(name, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("unable to decode resource name " + name, e);
    }
  }

  private void removeResourcePrefix(Item item) {
    checkNotNull(item);
    checkArgument(!Strings.isNullOrEmpty(item.getName()));
    item.setName(getRawItemResourceName(item.getName()));
  }

  private String getRawItemResourceName(String resourceName) {
    checkArgument(resourceName.startsWith(itemResourcePrefix), "invalid resource name prefix");
    return decodeResourceName(resourceName.substring(itemResourcePrefix.length()));
  }

  @Override
  protected void startUp() throws Exception {
    serviceManagerHelper.startAndAwaitHealthy(serviceManager);
  }

  @Override
  protected void shutDown() throws Exception {
    serviceManagerHelper.stopAndAwaitStopped(serviceManager);
  }

  // TODO(bmj): revoke public after refactoring sdk.ConnectorTraverser
  @VisibleForTesting
  public static class ServiceManagerHelper {
    // TODO(bmj): revoke public after refactoring sdk.ConnectorTraverser
    public ServiceManager getServiceManager(List<Service> services) {
      return new ServiceManager(services);
    }

    void startAndAwaitHealthy(ServiceManager manager) {
      manager.startAsync().awaitHealthy();
    }

    void stopAndAwaitStopped(ServiceManager manager) {
      manager.stopAsync().awaitStopped();
    }
  }

  @Override
  public UploadItemRef startUpload(String itemId) throws IOException {
    Upload uploadRequest =
        service
            .indexing()
            .datasources()
            .items()
            .upload(
                getItemResourceName(itemId),
                new StartUploadItemRequest()
                    .setConnectorName(connectorName)
                    .setDebugOptions(new DebugOptions().setEnableDebugging(enableApiDebugging)));
    acquireToken(Operations.DEFAULT);
    return executeRequest(uploadRequest, indexingServiceStats, true /* intializeDefaults */);
  }

  @Override
  public Schema getSchema() throws IOException {
    GetSchema getSchemaRequest =
        service
            .indexing()
            .datasources()
            .getSchema(resourcePrefix)
            .setDebugOptionsEnableDebugging(enableApiDebugging);
    try {
      acquireToken(Operations.DEFAULT);
      return executeRequest(getSchemaRequest, indexingServiceStats, true /* intializeDefaults */);
    } catch (IOException e) {
      // TODO(tvartak) : Remove once schema endpoint is available
      logger.log(Level.WARNING, "Schema lookup failed. Using empty schema", e);
      return new com.google.api.services.cloudsearch.v1.model.Schema()
          .setObjectDefinitions(Collections.emptyList());
    }
  }

  @Override
  public Operation getOperation(String name) throws IOException {
    validateRunning();
    checkArgument(!Strings.isNullOrEmpty(name), "Operation name cannot be null.");

    CloudSearch.Operations.Get request = this.service.operations().get(name);
    acquireToken(Operations.DEFAULT);
    return executeRequest(request, indexingServiceStats, true /* intializeDefaults */);
  }

  @FunctionalInterface
  interface VersionProvider {
    byte[] getVersion();
  }
}
