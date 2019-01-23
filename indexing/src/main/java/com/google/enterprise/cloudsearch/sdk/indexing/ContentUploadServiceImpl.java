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

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Media.Upload;
import com.google.api.services.cloudsearch.v1.model.Media;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.enterprise.cloudsearch.sdk.BaseApiService;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Concrete class implementing {@link ContentUploadService}
 *
 * <p>Document content uploads normally occur "in-line" as part of the item's upload request.
 * However, when the content is large, this upload service can optimize content uploads by
 * separating the content from the item's upload request. This results in a "two-step" process for
 * indexing a document into Cloud Search.
 *
 * <p>The deterministic threshold for "in-line" versus using this service is settable using the
 * configuration parameter {@value
 * com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl#UPLOAD_THRESHOLD_BYTES}. The
 * SDK default byte value is {@value
 * com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl#DEFAULT_CONTENT_UPLOAD_THRESHOLD_BYTES}.
 */
public class ContentUploadServiceImpl extends BaseApiService<CloudSearch>
    implements ContentUploadService {

  /** API scope for uploading content using Cloud Search API. */
  public static final Set<String> API_SCOPES =
      Collections.singleton("https://www.googleapis.com/auth/cloud_search");

  private final ListeningExecutorService executorService;
  private final MediaUploader mediaUploader;

  private ContentUploadServiceImpl(Builder builder) {
    super(builder);
    executorService = MoreExecutors.listeningDecorator(builder.executorService);
    mediaUploader = builder.mediaUploader;
  }

  @Override
  public ListenableFuture<Void> uploadContent(
      String resourceName, AbstractInputStreamContent content) throws IOException {
    checkArgument(!Strings.isNullOrEmpty(resourceName), "resource name can not be empty");
    checkNotNull(content, "content can not be null");
    checkState(isRunning(), "upload service not running to accept upload requests");
    Upload upload =
        service.media().upload(resourceName, new Media().setResourceName(resourceName), content);
    mediaUploader.enableMediaUploader(upload);
    return executorService.submit(
        () -> {
          upload.executeUnparsed();
          return null;
        });
  }

  /** Builder to create an instance of {@link ContentUploadServiceImpl}. */
  public static class Builder extends BaseApiService.AbstractBuilder<Builder, CloudSearch> {
    private ExecutorService executorService = MoreExecutors.newDirectExecutorService();
    private MediaUploader mediaUploader =
        request -> request.getMediaHttpUploader().setDirectUploadEnabled(true);

    /**
     * Sets {@link ExecutorService} used by {@link ContentUploadServiceImpl} for uploading content
     * asynchronously.
     *
     * @param executorService used by {@link ContentUploadServiceImpl} for uploading content
     *     asynchronously
     * @return this instance of {@link ContentUploadServiceImpl.Builder}
     */
    public Builder setExecutorService(ExecutorService executorService) {
      this.executorService = executorService;
      return this;
    }

    @VisibleForTesting
    Builder setMediaUploader(MediaUploader mediaUploader) {
      this.mediaUploader = mediaUploader;
      return this;
    }

    /**
     * Builder to create a content upload service.
     *
     * <p>The {@link IndexingServiceImpl} creates this content upload service by infusing the
     * required objects obtained during connector initialization.
     *
     * @return a fully instantiated content upload service
     * @throws IOException on errors creating the HTTP transport or credential
     * @throws GeneralSecurityException on security errors creating the HTTP transport or
     * credential
     */
    @Override
    public ContentUploadServiceImpl build() throws IOException, GeneralSecurityException {
      checkNotNull(credentialFactory, "Credential Factory cannot be null.");
      checkNotNull(executorService, "executorService cannot be null.");
      setupServiceAndCredentials();
      return new ContentUploadServiceImpl(this);
    }

    /** Gets this instance of {@link ContentUploadServiceImpl.Builder} */
    @Override
    public Builder getThis() {
      return this;
    }

    /** Gets API scopes to be used for uploading content using Cloud Search API. */
    @Override
    public Set<String> getApiScopes() {
      return API_SCOPES;
    }

    /**
     * Gets {@link CloudSearch.Builder} instance used for creating {@link CloudSearch} API client.
     */
    @Override
    public com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient.Builder
        getServiceBuilder(
            HttpTransport transport,
            JsonFactory jsonFactory,
            HttpRequestInitializer requestInitializer) {
      return new CloudSearch.Builder(transport, jsonFactory, requestInitializer);
    }
  }

  @FunctionalInterface
  interface MediaUploader {
    void enableMediaUploader(Upload request);
  }

  @Override
  protected void startUp() throws Exception {
  }

  @Override
  protected void shutDown() throws Exception {
    MoreExecutors.shutdownAndAwaitTermination(executorService, 10, TimeUnit.SECONDS);
  }
}
