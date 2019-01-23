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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Media.Upload;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentUploadServiceImpl.MediaUploader;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link ContentUploadServiceImpl}. */

@RunWith(MockitoJUnitRunner.class)
public class ContentUploadServiceImplTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock CloudSearch service;
  @Mock CloudSearch.Media serviceMedia;
  @Mock Upload uploadRequest;
  @Mock AbstractInputStreamContent content;
  @Mock MediaUploader mediaUploader;
  @Mock CredentialFactory mockCredentialfactory;
  @Mock GoogleCredential mockGoogleCredential;

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final CredentialFactory credentialFactory =
      scopes ->
          new MockGoogleCredential.Builder()
              .setTransport(GoogleNetHttpTransport.newTrustedTransport())
              .setJsonFactory(JSON_FACTORY)
              .build();
  @Test
  public void testMinBuilder() throws IOException, GeneralSecurityException {
    when(mockCredentialfactory.getCredential(IndexingServiceImpl.API_SCOPES))
        .thenReturn(mockGoogleCredential);
    checkNotNull(
        new ContentUploadServiceImpl.Builder().setCredentialFactory(mockCredentialfactory).build());
  }

  @Test
  public void testNullCredentialFactory() throws IOException, GeneralSecurityException {
    thrown.expect(NullPointerException.class);
    new ContentUploadServiceImpl.Builder().setService(service).setCredentialFactory(null).build();
  }

  @Test
  public void testUploadWithNotStarted() throws IOException, GeneralSecurityException {
    ContentUploadService contentUpload =
        new ContentUploadServiceImpl.Builder().setCredentialFactory(credentialFactory).build();
    assertFalse(contentUpload.isRunning());
    thrown.expect(IllegalStateException.class);
    contentUpload.uploadContent("resource", content);
  }

  @Test
  public void testUploadAfterStarted()
      throws IOException, GeneralSecurityException, InterruptedException, ExecutionException {
    ContentUploadService contentUpload =
        new ContentUploadServiceImpl.Builder()
            .setService(service)
            .setCredentialFactory(credentialFactory)
            .setMediaUploader(mediaUploader)
            .build();
    contentUpload.startAsync().awaitRunning();
    assertTrue(contentUpload.isRunning());
    when(service.media()).thenReturn(serviceMedia);
    when(serviceMedia.upload(
            "resource",
            new com.google.api.services.cloudsearch.v1.model.Media().setResourceName("resource"),
            content))
        .thenReturn(uploadRequest);
    ListenableFuture<Void> uploadResult =
        contentUpload.uploadContent("resource", content);
    uploadResult.get();
    contentUpload.stopAsync().awaitTerminated();
    assertFalse(contentUpload.isRunning());
    verify(mediaUploader).enableMediaUploader(uploadRequest);
  }

  @Test
  public void testUploadAfterStop() throws IOException, GeneralSecurityException {
    ContentUploadService contentUpload =
        new ContentUploadServiceImpl.Builder().setCredentialFactory(credentialFactory).build();
    contentUpload.startAsync().awaitRunning();
    assertTrue(contentUpload.isRunning());
    contentUpload.stopAsync().awaitTerminated();
    assertFalse(contentUpload.isRunning());
    thrown.expect(IllegalStateException.class);
    contentUpload.uploadContent("resource", content);
  }

  @Test
  public void testVerifyShutdown() throws IOException, GeneralSecurityException {
    ExecutorService executorService = MoreExecutors.newDirectExecutorService();
    ContentUploadService contentUpload =
        new ContentUploadServiceImpl.Builder()
            .setCredentialFactory(credentialFactory)
            .setExecutorService(executorService)
            .build();
    contentUpload.startAsync().awaitRunning();
    assertTrue(contentUpload.isRunning());
    contentUpload.stopAsync().awaitTerminated();
    assertFalse(contentUpload.isRunning());
    assertTrue(executorService.isShutdown());
  }
}
