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

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonError.ErrorInfo;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.FileContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException.Builder;
import com.google.api.client.http.InputStreamContent;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items;
import com.google.api.services.cloudsearch.v1.model.DebugOptions;
import com.google.api.services.cloudsearch.v1.model.IndexItemRequest;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemContent;
import com.google.api.services.cloudsearch.v1.model.ItemStatus;
import com.google.api.services.cloudsearch.v1.model.ListItemsResponse;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.api.services.cloudsearch.v1.model.PollItemsResponse;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.PushItemRequest;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.UnreserveItemsRequest;
import com.google.api.services.cloudsearch.v1.model.UploadItemRef;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service.State;
import com.google.common.util.concurrent.ServiceManager;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.BatchPolicy;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.LocalFileCredentialFactory;
import com.google.enterprise.cloudsearch.sdk.QuotaServer;
import com.google.enterprise.cloudsearch.sdk.RetryPolicy;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl.Operations;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl.PollItemStatus;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl.ServiceManagerHelper;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.StreamSupport;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Unit test methods for {@link IndexingServiceImpl}. TODO(tvartak) : Use mockito for all tests */
@RunWith(MockitoJUnitRunner.class)
public class IndexingServiceTest {

  private static final Operation OPERATION_DONE = new Operation().setDone(true);
  private static final String ITEMS_RESOURCE_PREFIX = "datasources/source/items/";
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public TestName testName = new TestName();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock BatchingIndexingService batchingService;
  @Mock ContentUploadService contentUploadService;
  @Mock ServiceManagerHelper serviceManagerHelper;
  @Mock QuotaServer<IndexingServiceImpl.Operations> quotaServer;

  private static final String SOURCE_ID = "source";
  private static final String IDENTITY_SOURCE_ID = "identitySources";
  private static final String GOOD_ID = "goodId";
  private static final String BAD_ID = "badId";
  private static final String ERROR_ID = "errorId";
  private static final String NOTFOUND_ID = "notFound";
  private static final boolean BRIEF = true;

  private static final int CONTENT_UPLOAD_THRESHOLD = 16;

  private CloudSearch cloudSearch;
  private TestingHttpTransport transport;
  private IndexingService indexingService;

  private static final GoogleJsonError NOT_FOUND_ERROR =
      new GoogleJsonError()
          .set("code", HTTP_NOT_FOUND)
          .set("message", "not found")
          .set("errors", Collections.singletonList(new ErrorInfo().set("message", "not found")));

  private static final GoogleJsonError HTTP_FORBIDDEN_ERROR =
      new GoogleJsonError()
          .set("code", HTTP_FORBIDDEN)
          .set("message", "access forbidden")
          .set("errors", Collections.singletonList(new ErrorInfo().set("message", "some error")));

  @Before
  public void createService() throws IOException, GeneralSecurityException {
    createService(false, false);
  }

  private void createService(boolean enableDebugging, boolean allowUnknownGsuitePrincipals)
      throws IOException, GeneralSecurityException {
    this.transport = new TestingHttpTransport("datasources/source/connectors/unitTest");
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    CredentialFactory credentialFactory =
        scopes ->
            new MockGoogleCredential.Builder()
                .setTransport(new MockHttpTransport())
                .setJsonFactory(jsonFactory)
                .build();
    GoogleCredential credential =
        new MockGoogleCredential.Builder()
            .setTransport(this.transport)
            .setJsonFactory(jsonFactory)
            .build();
    CloudSearch.Builder serviceBuilder =
        new CloudSearch.Builder(this.transport, jsonFactory, credential);
    this.cloudSearch = serviceBuilder.setApplicationName("IndexingServiceTest").build();
    when(batchingService.state()).thenReturn(State.NEW);
    when(contentUploadService.state()).thenReturn(State.NEW);
    doAnswer(invocation -> new ServiceManager(invocation.getArgument(0)))
        .when(serviceManagerHelper)
        .getServiceManager(Arrays.asList(batchingService, contentUploadService));
    this.indexingService =
        new IndexingServiceImpl.Builder()
            .setTransport(transport)
            .setJsonFactory(jsonFactory)
            .setCredentialFactory(credentialFactory)
            .setSourceId(SOURCE_ID)
            .setIdentitySourceId(IDENTITY_SOURCE_ID)
            .setService(cloudSearch)
            .setBatchingIndexingService(batchingService)
            .setContentUploadService(contentUploadService)
            .setContentUploadThreshold(CONTENT_UPLOAD_THRESHOLD)
            .setServiceManagerHelper(serviceManagerHelper)
            .setQuotaServer(quotaServer)
            .setConnectorId("unitTest")
            .setEnableDebugging(enableDebugging)
            .setAllowUnknownGsuitePrincipals(allowUnknownGsuitePrincipals)
            .build();
    this.indexingService.startAsync().awaitRunning();
  }

  @Test
  public void testBuilderWithInvalidSource() throws IOException, GeneralSecurityException {
    thrown.expect(IllegalArgumentException.class);
    new IndexingServiceImpl.Builder().setSourceId(null).build();
  }

  @Test
  public void testBuilderWithOutCredentialFactory() throws IOException, GeneralSecurityException {
    thrown.expect(IllegalArgumentException.class);
    new IndexingServiceImpl.Builder().setSourceId(SOURCE_ID).setService(cloudSearch).build();
  }

  @Test
  public void testBuilderWithServices() throws IOException, GeneralSecurityException {
    CredentialFactory credentialFactory =
        scopes ->
            new MockGoogleCredential.Builder()
                .setTransport(new MockHttpTransport())
                .setJsonFactory(JacksonFactory.getDefaultInstance())
                .build();
    assertNotNull(
        new IndexingServiceImpl.Builder()
            .setSourceId(SOURCE_ID)
            .setIdentitySourceId(IDENTITY_SOURCE_ID)
            .setService(cloudSearch)
            .setBatchingIndexingService(batchingService)
            .setCredentialFactory(credentialFactory)
            .setBatchPolicy(new BatchPolicy.Builder().build())
            .setRetryPolicy(new RetryPolicy.Builder().build())
            .setConnectorId("unitTest")
            .build());
  }

  @Test
  public void testBuilderWithConfigurationAndCredentialFactory()
      throws IOException, GeneralSecurityException {
    CredentialFactory credentialFactory =
        scopes ->
            new MockGoogleCredential.Builder()
                .setTransport(new MockHttpTransport())
                .setJsonFactory(JacksonFactory.getDefaultInstance())
                .build();
    Properties config = new Properties();
    config.put(IndexingServiceImpl.SOURCE_ID, "sourceId");
    config.put(IndexingServiceImpl.IDENTITY_SOURCE_ID, "identitySourceId");
    setupConfig.initConfig(config);
    IndexingServiceImpl.Builder.fromConfiguration(Optional.of(credentialFactory), "unitTest")
        .build();
  }

  @Test
  public void testBuilderWithConfigurationAndNoCredentialFactory() throws IOException {
    Properties config = new Properties();
    config.put(IndexingServiceImpl.SOURCE_ID, "sourceId");
    config.put(IndexingServiceImpl.IDENTITY_SOURCE_ID, "identitySourceId");
    File serviceAcctFile = temporaryFolder.newFile("serviceaccount.json");
    config.put(
        LocalFileCredentialFactory.SERVICE_ACCOUNT_KEY_FILE_CONFIG,
        serviceAcctFile.getAbsolutePath());
    setupConfig.initConfig(config);
    assertNotNull(IndexingServiceImpl.Builder.fromConfiguration(Optional.empty(), "unitTest"));
  }

  @Test
  public void testBuilderWithNullQuotaServer() throws IOException, GeneralSecurityException {
    CredentialFactory credentialFactory =
        scopes ->
            new MockGoogleCredential.Builder()
                .setTransport(new MockHttpTransport())
                .setJsonFactory(JacksonFactory.getDefaultInstance())
                .build();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(containsString("quota server can not be null"));
    new IndexingServiceImpl.Builder()
        .setSourceId(SOURCE_ID)
        .setIdentitySourceId(IDENTITY_SOURCE_ID)
        .setService(cloudSearch)
        .setCredentialFactory(credentialFactory)
        .setQuotaServer(null)
        .setBatchPolicy(new BatchPolicy.Builder().build())
        .build();
  }

  /*
   * Item test methods.
   */

  /* delete */
  @Test
  public void testDeleteItemWithVersion()
      throws IOException, InterruptedException, ExecutionException {
    this.transport.addDeleteItemReqResp(SOURCE_ID, GOOD_ID, OPERATION_DONE);
    Operation op = new Operation();
    doAnswer(
            invocation -> {
              Items.Delete deleteRequest = invocation.getArgument(0);
              assertEquals(ITEMS_RESOURCE_PREFIX + GOOD_ID, deleteRequest.getName());
              assertEquals(
                  "abc", new String(Base64.getDecoder().decode(deleteRequest.getVersion())));
              assertEquals(RequestMode.SYNCHRONOUS.name(), deleteRequest.getMode());
              SettableFuture<Operation> result = SettableFuture.create();
              result.set(new Operation());
              return result;
            })
        .when(batchingService)
        .deleteItem(any());
    this.indexingService.deleteItem(GOOD_ID, "abc".getBytes(UTF_8), RequestMode.SYNCHRONOUS).get();
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testDeleteItemWithSlash()
      throws IOException, InterruptedException, ExecutionException {
    String itemName = "http://example.com/item1?admin";
    Operation op = new Operation();
    doAnswer(
            invocation -> {
              Items.Delete deleteRequest = invocation.getArgument(0);
              assertEquals(
                  ITEMS_RESOURCE_PREFIX + "http:%2F%2Fexample.com%2Fitem1%3Fadmin",
                  deleteRequest.getName());
              assertEquals(RequestMode.SYNCHRONOUS.name(), deleteRequest.getMode());
              SettableFuture<Operation> result = SettableFuture.create();
              result.set(op);
              return result;
            })
        .when(batchingService)
        .deleteItem(any());
    assertEquals(
        op, this.indexingService.deleteItem(itemName, null, RequestMode.SYNCHRONOUS).get());
  }

  @Test
  public void testDeleteNotFoundItem()
      throws IOException, InterruptedException, ExecutionException {
    doAnswer(
            invocation -> {
              Items.Delete deleteRequest = invocation.getArgument(0);
              assertEquals(ITEMS_RESOURCE_PREFIX + NOTFOUND_ID, deleteRequest.getName());
              SettableFuture<Operation> result = SettableFuture.create();
              result.setException(new IOException("not found"));
              return result;
            })
        .when(batchingService)
        .deleteItem(any());
    thrown.expect(ExecutionException.class);
    this.indexingService.deleteItem(NOTFOUND_ID, null, RequestMode.UNSPECIFIED).get();
  }

  @Test
  public void testDeleteItemError() throws IOException, InterruptedException {
    doAnswer(
            invocation -> {
              Items.Delete deleteRequest = invocation.getArgument(0);
              assertEquals(ITEMS_RESOURCE_PREFIX + BAD_ID, deleteRequest.getName());
              return getExceptionFuture(HTTP_FORBIDDEN_ERROR);
            })
        .when(batchingService)
        .deleteItem(any());
    try {
      this.indexingService.deleteItem(BAD_ID, null, RequestMode.UNSPECIFIED).get();
      fail("Should have thrown HTTP_FORBIDDEN exception.");
    } catch (ExecutionException e) {
      validateApiError(e, HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testDeleteNullItemId() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.deleteItem(null, null, RequestMode.UNSPECIFIED);
  }

  /* delete queue items */

  @Test
  public void testDeleteQueueItemsNullQueue() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.deleteQueueItems(null);
  }

  @Test
  public void testDeleteQueueItemsEmptyQueue() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.deleteQueueItems("");
  }

  @Test
  public void testDeleteQueueItems() throws Exception {
    this.transport.addDeleteQueueItemsReqResp(SOURCE_ID, new Operation().setName("testOperation"));
    ListenableFuture<Operation> result = this.indexingService.deleteQueueItems("testqueue");
    assertEquals("testOperation", result.get().getName());
  }

  /* get */
  @Test
  public void testGetItem() throws IOException {
    Item goodItem = new Item().setName(GOOD_ID);
    this.transport.addGetItemReqResp(SOURCE_ID, GOOD_ID, false, goodItem);
    Item item = this.indexingService.getItem(GOOD_ID);
    assertNotNull(item);
    assertTrue(item.getName().equals(goodItem.getName()));
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testGetItemWithSlash() throws IOException {
    String itemName = "docs/item1";
    Item goodItem = new Item().setName(itemName);
    this.transport.addGetItemReqResp(SOURCE_ID, "docs%2Fitem1", false, goodItem);
    Item item = this.indexingService.getItem(itemName);
    assertNotNull(item);
    assertTrue(item.getName().equals(goodItem.getName()));
  }

  @Test
  public void testGetNotFoundItem() throws IOException {
    this.transport.addGetItemReqResp(SOURCE_ID, NOTFOUND_ID, false, NOT_FOUND_ERROR);
    Item item = this.indexingService.getItem(NOTFOUND_ID);
    assertNull(item);
  }

  @Test
  public void testGetItemError() throws IOException {
    this.transport.addGetItemReqResp(SOURCE_ID, BAD_ID, false, HTTP_FORBIDDEN_ERROR);
    try {
      this.indexingService.getItem(BAD_ID);
      fail("Should have thrown HTTP_FORBIDDEN exception.");
    } catch (GoogleJsonResponseException e) {
      assertEquals(HTTP_FORBIDDEN, e.getStatusCode());
    }
  }

  @Test
  public void testGetNullItemId() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.getItem(null);
    verifyNoMoreInteractions(quotaServer);
  }

  /* list */
  @Test
  public void testListItem() throws IOException {
    Item goodItem = new Item().setName("goodId");
    ListItemsResponse listResponse =
        new ListItemsResponse().setItems(Collections.singletonList(goodItem));
    this.transport.addListItemReqResp(SOURCE_ID, true, null, false, listResponse);
    for (Item item : this.indexingService.listItem(BRIEF)) {
      assertNotNull(item);
      assertTrue(item.getName().equals(goodItem.getName()));
    }
  }

  @Test
  public void testListItemBrief() throws IOException {
    Item goodItem = new Item().setName("goodId");
    ListItemsResponse listResponse =
        new ListItemsResponse().setItems(Collections.singletonList(goodItem));
    this.transport.addListItemReqResp(SOURCE_ID, true, "", false, listResponse);
    for (Item item : this.indexingService.listItem(BRIEF)) {
      assertNotNull(item);
      assertTrue(item.getName().equals(goodItem.getName()));
    }
  }

  @Test
  public void testListItemNotBrief() throws IOException {
    Item goodItem = new Item().setName("goodId");
    ListItemsResponse listResponse =
        new ListItemsResponse().setItems(Collections.singletonList(goodItem));
    this.transport.addListItemReqResp(SOURCE_ID, false, "", false, listResponse);
    for (Item item : this.indexingService.listItem(!BRIEF)) {
      assertNotNull(item);
      assertTrue(item.getName().equals(goodItem.getName()));
    }
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testListItemMultiFetch() throws IOException {
    List<Item> firstSetItems = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      Item goodItem = new Item().setName("goodId" + i);
      firstSetItems.add(goodItem);
    }
    List<Item> secondSetItems = new ArrayList<>();
    for (int i = 10; i < 15; i++) {
      Item goodItem = new Item().setName("goodId" + i);
      secondSetItems.add(goodItem);
    }
    ListItemsResponse listResponse1 =
        new ListItemsResponse().setItems(firstSetItems).setNextPageToken("somestring");
    ListItemsResponse listResponse2 = new ListItemsResponse().setItems(secondSetItems);
    this.transport.addListItemReqResp(SOURCE_ID, true, "", false, listResponse1);
    this.transport.addListItemReqResp(SOURCE_ID, true, "somestring", false, listResponse2);

    Iterable<Item> listIterator = this.indexingService.listItem(BRIEF);
    assertNotNull(listIterator);
    assertTrue(listIterator.iterator().hasNext());
    assertEquals(
        15, StreamSupport.stream(listIterator.spliterator(), false).filter(i -> i != null).count());
    verify(quotaServer, times(2)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testListItemErrorNext() throws IOException {
    Item goodItem = new Item().setName("goodId");
    ListItemsResponse listResponse =
        new ListItemsResponse().setItems(Collections.singletonList(goodItem));
    this.transport.addListItemReqResp(SOURCE_ID, true, "", false, listResponse);
    Iterable<Item> listIterator = this.indexingService.listItem(BRIEF);
    for (Item item : listIterator) {
      assertNotNull(item);
      assertTrue(item.getName().equals(goodItem.getName()));
    }
    thrown.expect(NoSuchElementException.class);
    assertNotNull(listIterator.iterator().next());
  }

  @Test
  public void testListItemErrorRemove() throws IOException {
    Item goodItem = new Item().setName("goodId");
    ListItemsResponse listResponse =
        new ListItemsResponse().setItems(Collections.singletonList(goodItem));
    this.transport.addListItemReqResp(SOURCE_ID, true, "", false, listResponse);
    Iterable<Item> listIterator = this.indexingService.listItem(BRIEF);
    Item item = listIterator.iterator().next();
    assertTrue(item.getName().equals(goodItem.getName()));
    thrown.expect(UnsupportedOperationException.class);
    listIterator.iterator().remove();
  }

  @Test
  public void testListItemEmpty() throws IOException {
    ListItemsResponse listResponse = new ListItemsResponse();
    this.transport.addListItemReqResp(SOURCE_ID, true, "", false, listResponse);
    Iterable<Item> listIterator = this.indexingService.listItem(BRIEF);
    assertTrue(!listIterator.iterator().hasNext());
  }

  @Test
  public void testListItemError() throws IOException {
    this.transport.addListItemReqResp(SOURCE_ID, "", HTTP_FORBIDDEN_ERROR);
    thrown.expect(RuntimeException.class);
    this.indexingService.listItem(BRIEF).iterator().hasNext();
  }

  /* update */
  @Test
  public void testUpdateItem() throws IOException, InterruptedException {
    doAnswer(
            invocation -> {
              Items.Index updateRequest = invocation.getArgument(0);
              assertEquals(ITEMS_RESOURCE_PREFIX + GOOD_ID, updateRequest.getName());
              IndexItemRequest indexItemRequest = (IndexItemRequest) updateRequest.getJsonContent();
              assertEquals(RequestMode.SYNCHRONOUS.name(), indexItemRequest.getMode());
              SettableFuture<Operation> result = SettableFuture.create();
              result.set(new Operation());
              return result;
            })
        .when(batchingService)
        .indexItem(any());
    Item item = new Item().setName(GOOD_ID);
    this.indexingService.indexItem(item, RequestMode.UNSPECIFIED);
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  /* update */
  @Test
  public void testUpdateItemDebugOptionsEnabled() throws Exception {
    createService(/*debugging*/ true, /*allowUnknownGsuitePrincipals*/ false);
    doAnswer(
            invocation -> {
              Items.Index updateRequest = invocation.getArgument(0);
              IndexItemRequest indexItemRequest = (IndexItemRequest) updateRequest.getJsonContent();
              assertTrue(indexItemRequest.getDebugOptions().getEnableDebugging());
              assertFalse(indexItemRequest.getIndexItemOptions().getAllowUnknownGsuitePrincipals());
              return Futures.immediateFuture(new Operation());
            })
        .when(batchingService)
        .indexItem(any());
    Item item = new Item().setName(GOOD_ID);
    this.indexingService.indexItem(item, RequestMode.UNSPECIFIED);
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemAllowUnknownGsuitePrincipals() throws Exception {
    createService(/*debugging*/ false, /*allowUnknownGsuitePrincipals*/ true);
    doAnswer(
            invocation -> {
              Items.Index updateRequest = invocation.getArgument(0);
              IndexItemRequest indexItemRequest = (IndexItemRequest) updateRequest.getJsonContent();
              assertFalse(indexItemRequest.getDebugOptions().getEnableDebugging());
              assertTrue(indexItemRequest.getIndexItemOptions().getAllowUnknownGsuitePrincipals());
              return Futures.immediateFuture(new Operation());
            })
        .when(batchingService)
        .indexItem(any());
    Item item = new Item().setName(GOOD_ID);
    this.indexingService.indexItem(item, RequestMode.ASYNCHRONOUS);
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemWithContent() throws IOException, InterruptedException {
    doAnswer(
            invocation -> {
              Items.Index updateRequest = invocation.getArgument(0);
              assertEquals(ITEMS_RESOURCE_PREFIX + GOOD_ID, updateRequest.getName());
              IndexItemRequest indexItemRequest = (IndexItemRequest) updateRequest.getJsonContent();
              assertEquals(RequestMode.ASYNCHRONOUS.name(), indexItemRequest.getMode());
              assertEquals(
                  new ItemContent()
                      .encodeInlineContent("Hello World.".getBytes(UTF_8))
                      .setContentFormat("TEXT"),
                  indexItemRequest.getItem().getContent());
              SettableFuture<Operation> result = SettableFuture.create();
              result.set(new Operation());
              return result;
            })
        .when(batchingService)
        .indexItem(any());
    Item item = new Item().setName(GOOD_ID);
    ByteArrayContent content = ByteArrayContent.fromString("text/plain", "Hello World.");
    this.indexingService.indexItemAndContent(
        item, content, null, ContentFormat.TEXT, RequestMode.ASYNCHRONOUS);
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemWithEmptyContent() throws IOException {
    Item item = new Item().setName(GOOD_ID);
    ByteArrayContent content = ByteArrayContent.fromString("text/plain", "");
    this.indexingService.indexItemAndContent(
        item, content, null, ContentFormat.TEXT, RequestMode.ASYNCHRONOUS);
    assertEquals(
        new ItemContent().encodeInlineContent(new byte[0]).setContentFormat("TEXT"),
        item.getContent());
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemWithEmptyInputStreamContent() throws IOException {
    this.transport.addUploadItemsReqResp(
        SOURCE_ID, GOOD_ID, new UploadItemRef().setName(testName.getMethodName()));
    this.transport.addUpdateItemReqResp(SOURCE_ID, GOOD_ID, false, OPERATION_DONE);

    Item item = new Item().setName(GOOD_ID);
    InputStreamContent content =
        new InputStreamContent("text/html", new ByteArrayInputStream(new byte[0]));
    when(contentUploadService.uploadContent(testName.getMethodName(), content))
        .thenReturn(Futures.immediateFuture(null));
    this.indexingService.indexItemAndContent(
        item, content, null, ContentFormat.TEXT, RequestMode.ASYNCHRONOUS);
    assertEquals(
        new ItemContent()
            .setContentDataRef(new UploadItemRef().setName(testName.getMethodName()))
            .setContentFormat("TEXT"),
        item.getContent());
    verify(quotaServer, times(2)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemWithContentUpload() throws IOException {
    this.transport.addUploadItemsReqResp(
        SOURCE_ID, GOOD_ID, new UploadItemRef().setName(testName.getMethodName()));
    this.transport.addUpdateItemReqResp(SOURCE_ID, GOOD_ID, false, OPERATION_DONE);

    Item item = new Item().setName(GOOD_ID);
    InputStreamContent content =
        new InputStreamContent(
            "text/html", new ByteArrayInputStream("Hello World.".getBytes(UTF_8)));
    when(contentUploadService.uploadContent(testName.getMethodName(), content))
        .thenReturn(Futures.immediateFuture(null));
    this.indexingService.indexItemAndContent(
        item, content, null, ContentFormat.TEXT, RequestMode.ASYNCHRONOUS);
    assertEquals(
        new ItemContent()
            .setContentDataRef(new UploadItemRef().setName(testName.getMethodName()))
            .setContentFormat("TEXT"),
        item.getContent());
    verify(quotaServer, times(2)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemWithEmptyFileContent() throws IOException {
    File emptyFile = temporaryFolder.newFile();
    Item item = new Item().setName(GOOD_ID);
    FileContent content = new FileContent("text/html", emptyFile);
    this.indexingService.indexItemAndContent(
        item, content, null, ContentFormat.TEXT, RequestMode.ASYNCHRONOUS);
    assertEquals(
        new ItemContent().encodeInlineContent(new byte[0]).setContentFormat("TEXT"),
        item.getContent());
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemWithFileContent() throws IOException {
    this.transport.addUploadItemsReqResp(
        SOURCE_ID, GOOD_ID, new UploadItemRef().setName(testName.getMethodName()));
    this.transport.addUpdateItemReqResp(SOURCE_ID, GOOD_ID, false, OPERATION_DONE);

    File largeFile = temporaryFolder.newFile();
    Files.asCharSink(largeFile, UTF_8).write("Longer text that triggers an upload");
    Item item = new Item().setName(GOOD_ID);
    FileContent content = new FileContent("text/html", largeFile);
    when(contentUploadService.uploadContent(testName.getMethodName(), content))
        .thenReturn(Futures.immediateFuture(null));
    this.indexingService.indexItemAndContent(
        item, content, null, ContentFormat.TEXT, RequestMode.ASYNCHRONOUS);
    assertEquals(
        new ItemContent()
            .setContentDataRef(new UploadItemRef().setName(testName.getMethodName()))
            .setContentFormat("TEXT"),
        item.getContent());
    verify(quotaServer, times(2)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemWithContentHash() throws IOException {
    this.transport.addUpdateItemReqResp(SOURCE_ID, GOOD_ID, false, OPERATION_DONE);
    Item item = new Item().setName(GOOD_ID);
    ByteArrayContent content = ByteArrayContent.fromString("text/plain", "Hello World.");
    String hash = Integer.toString(Objects.hash(content));
    this.indexingService.indexItemAndContent(
        item, content, hash, ContentFormat.TEXT, RequestMode.ASYNCHRONOUS);
    assertEquals(
        new ItemContent()
            .encodeInlineContent("Hello World.".getBytes(UTF_8))
            .setHash(hash)
            .setContentFormat("TEXT"),
        item.getContent());
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUpdateItemError() throws IOException, InterruptedException {
    doAnswer(
            invocation -> {
              Items.Index updateRequest = invocation.getArgument(0);
              assertEquals(ITEMS_RESOURCE_PREFIX + ERROR_ID, updateRequest.getName());
              return getExceptionFuture(HTTP_FORBIDDEN_ERROR);
            })
        .when(batchingService)
        .indexItem(any());
    Item item = new Item().setName(ERROR_ID);
    try {
      this.indexingService.indexItem(item, RequestMode.SYNCHRONOUS).get();
    } catch (ExecutionException e) {
      validateApiError(e, HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testUpdateItemWithContentError() throws IOException, InterruptedException {
    Item item = new Item().setName(ERROR_ID);
    ByteArrayContent content = ByteArrayContent.fromString("text/plain", "Hello World.");
    doAnswer(
            invocation -> {
              Items.Index updateRequest = invocation.getArgument(0);
              assertEquals(ITEMS_RESOURCE_PREFIX + ERROR_ID, updateRequest.getName());
              return getExceptionFuture(HTTP_FORBIDDEN_ERROR);
            })
        .when(batchingService)
        .indexItem(any());
    try {
      indexingService
          .indexItemAndContent(item, content, null, ContentFormat.TEXT, RequestMode.SYNCHRONOUS)
          .get();
    } catch (ExecutionException e) {
      validateApiError(e, HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testUpdateNullItem() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.indexItem(null, RequestMode.SYNCHRONOUS);
    verifyNoMoreInteractions(quotaServer);
  }

  @Test
  public void testUpdateNullItemWithContent() throws IOException {
    ByteArrayContent content = ByteArrayContent.fromString("text/plain", "Hello World.");
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.indexItemAndContent(
        null, content, null, ContentFormat.TEXT, RequestMode.SYNCHRONOUS);
  }

  @Test
  public void testUpdateNullItemId() throws IOException {
    Item item = new Item();
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.indexItem(item, RequestMode.SYNCHRONOUS);
  }

  @Test
  public void testUpdateNullItemIdWithContent() throws IOException {
    Item item = new Item();
    ByteArrayContent content = ByteArrayContent.fromString("text/plain", "Hello World.");
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.indexItemAndContent(
        item, content, null, ContentFormat.TEXT, RequestMode.SYNCHRONOUS);
  }

  @Test
  public void testUpdateItemWithNullContent() throws IOException {
    Item item = new Item().setName(GOOD_ID);
    thrown.expect(NullPointerException.class);
    this.indexingService.indexItemAndContent(
        item, null, null, ContentFormat.TEXT, RequestMode.SYNCHRONOUS);
  }

  @Test
  public void testGetSchema() throws IOException {
    // BaseApiService.setDefaultValuesForPrimitiveTypes assigns the empty lists.
    Schema schema = new Schema().setObjectDefinitions(Collections.emptyList());
    this.transport.addGetSchemaReqResp(SOURCE_ID, false, schema);
    assertEquals(schema, indexingService.getSchema());
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  /* poll */
  @Test
  public void testPollEntries() throws IOException {
    PollItemsRequest pollRequest = new PollItemsRequest();
    PollItemsResponse pollResponse = new PollItemsResponse();
    List<Item> initEntries = new ArrayList<>();
    initEntries.add(new Item().setName(ITEMS_RESOURCE_PREFIX + GOOD_ID));
    pollResponse.setItems(initEntries);
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse);
    List<Item> entries = this.indexingService.poll(pollRequest);
    assertTrue(entries.size() == 1);
    assertTrue(entries.get(0).getName().equals(GOOD_ID));
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testPollEntriesEmpty() throws IOException {
    PollItemsRequest pollRequest = new PollItemsRequest();
    PollItemsResponse pollResponse = new PollItemsResponse();
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse);
    List<Item> entries = this.indexingService.poll(pollRequest);
    assertTrue(entries.size() == 0);
  }

  @Test
  public void testPollEntriesMulti() throws IOException {
    PollItemsRequest pollRequest = new PollItemsRequest();
    PollItemsResponse pollResponse = new PollItemsResponse();
    List<Item> initEntries = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {
      initEntries.add(new Item().setName(ITEMS_RESOURCE_PREFIX + "id" + String.valueOf(i)));
    }
    pollResponse.setItems(initEntries);
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse);
    List<Item> entries = this.indexingService.poll(pollRequest);
    assertTrue(entries.size() == 10);
  }

  @Test
  public void testPollEntriesCustom() throws IOException {
    PollItemsRequest pollRequest =
        new PollItemsRequest()
            .setLimit(10)
            .setQueue("myQueue")
            .setStatusCodes(Collections.singletonList("MODIFIED"));
    PollItemsResponse pollResponse = new PollItemsResponse();
    List<Item> initEntries = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      initEntries.add(
          new Item()
              .setName(ITEMS_RESOURCE_PREFIX + "id" + String.valueOf(i))
              .setQueue("myQueue")
              .setStatus(new ItemStatus().setCode("MODIFIED")));
    }
    pollResponse.setItems(initEntries);
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse);
    List<Item> entries = this.indexingService.poll(pollRequest);
    assertTrue(entries.size() == 5);
  }

  @Test
  public void testPollEntriesError() throws IOException {
    PollItemsRequest pollRequest = new PollItemsRequest();
    this.transport.addPollItemReqResp(SOURCE_ID, HTTP_FORBIDDEN_ERROR);
    try {
      this.indexingService.poll(pollRequest);
      fail("Should have thrown HTTP_FORBIDDEN exception.");
    } catch (GoogleJsonResponseException e) {
      assertEquals(e.getStatusCode(), HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testPollEntriesStatus() throws IOException {
    PollItemsResponse pollResponse = new PollItemsResponse();
    List<Item> initEntries = new ArrayList<>();
    initEntries.add(
        new Item()
            .setName(ITEMS_RESOURCE_PREFIX + GOOD_ID)
            .setStatus(new ItemStatus().setCode(PollItemStatus.ACCEPTED.toString())));
    pollResponse.setItems(initEntries);
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse);
    PollItemsRequest pollRequest =
        new PollItemsRequest()
            .setStatusCodes(
                Arrays.asList(
                    PollItemStatus.ACCEPTED.toString(),
                    PollItemStatus.MODIFIED.toString(),
                    PollItemStatus.SERVER_ERROR.toString(),
                    PollItemStatus.NEW_ITEM.toString()));
    List<Item> entries = this.indexingService.poll(pollRequest);
    assertTrue(entries.size() == 1);
    assertTrue(entries.get(0).getName().equals(GOOD_ID));
    assertEquals(PollItemStatus.ACCEPTED.toString(), entries.get(0).getStatus().getCode());
  }

  @Test
  public void testPollEntriesDecodeResourceName() throws IOException {
    PollItemsResponse pollResponse = new PollItemsResponse();
    List<Item> initEntries = new ArrayList<>();
    initEntries.add(
        new Item()
            .setName(ITEMS_RESOURCE_PREFIX + "http:%2F%2Fwww.google.com")
            .setStatus(new ItemStatus().setCode(PollItemStatus.ACCEPTED.toString())));
    pollResponse.setItems(initEntries);
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse);
    PollItemsRequest pollRequest =
        new PollItemsRequest()
            .setStatusCodes(
                Arrays.asList(
                    PollItemStatus.ACCEPTED.toString(),
                    PollItemStatus.MODIFIED.toString(),
                    PollItemStatus.SERVER_ERROR.toString(),
                    PollItemStatus.NEW_ITEM.toString()));
    List<Item> entries = this.indexingService.poll(pollRequest);
    assertTrue(entries.size() == 1);
    assertTrue(entries.get(0).getName().equals("http://www.google.com"));
    assertEquals(PollItemStatus.ACCEPTED.toString(), entries.get(0).getStatus().getCode());
  }

  @Test
  public void testPollEntriesNoStatus() throws IOException {
    PollItemsResponse pollResponse = new PollItemsResponse();
    List<Item> initEntries = new ArrayList<Item>();
    initEntries.add(
        new Item()
            .setName(ITEMS_RESOURCE_PREFIX + GOOD_ID)
            .setStatus(new ItemStatus().setCode(PollItemStatus.ACCEPTED.toString())));
    pollResponse.setItems(initEntries);
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse);
    PollItemsRequest pollRequest = new PollItemsRequest();
    List<Item> entries = this.indexingService.poll(pollRequest);
    assertTrue(entries.size() == 1);
    assertTrue(entries.get(0).getName().equals(GOOD_ID));
    assertEquals(PollItemStatus.ACCEPTED.toString(), entries.get(0).getStatus().getCode());
  }

  @Test
  public void testPollEntriesStatusError() throws IOException {
    PollItemsRequest pollRequest =
        new PollItemsRequest().setStatusCodes(Arrays.asList("indexd", "modifiied"));
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.poll(pollRequest);
  }

  @Test
  public void testPollAllEntriesEmpty() throws IOException {
    PollItemsRequest pollQueueRequest = new PollItemsRequest().setLimit(3);
    PollItemsResponse pollResponse = new PollItemsResponse();
    pollResponse.setItems(Arrays.asList());
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse);

    Iterable<Item> queueEntryIterator = this.indexingService.pollAll(pollQueueRequest);
    assertNotNull(queueEntryIterator);
    assertNotNull(queueEntryIterator.iterator());
    assertFalse(queueEntryIterator.iterator().hasNext());
  }

  private static List<Item> getPollResponseItems(int count, int id) {
    List<Item> items = new ArrayList<>();
    for (int i = 0; i < count; i++, id++) {
      items.add(new Item().setName(ITEMS_RESOURCE_PREFIX + "id" + id));
    }
    return items;
  }

  @Test
  public void testPollAllEntries() throws IOException {
    PollItemsRequest pollQueueRequest = new PollItemsRequest().setLimit(3);
    PollItemsResponse pollResponse1 = new PollItemsResponse().setItems(getPollResponseItems(3, 1));
    PollItemsResponse pollResponse2 = new PollItemsResponse().setItems(getPollResponseItems(3, 4));
    PollItemsResponse emptyResponse = new PollItemsResponse().setItems(Arrays.asList());
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse1);
    this.transport.addPollItemReqResp(SOURCE_ID, pollResponse2);
    this.transport.addPollItemReqResp(SOURCE_ID, emptyResponse);

    Iterable<Item> queueEntryIterator = this.indexingService.pollAll(pollQueueRequest);
    assertNotNull(queueEntryIterator);
    assertNotNull(queueEntryIterator.iterator());
    assertTrue(queueEntryIterator.iterator().hasNext());
    int i = 1;
    for (Item entry : queueEntryIterator) {
      assertNotNull(entry);
      assertEquals("id" + i, entry.getName());
      i++;
    }
    assertEquals(pollResponse1.getItems().size() + pollResponse2.getItems().size(), (i - 1));
  }

  /* push */
  @Test
  public void testPushItem() throws IOException {
    this.transport.addPushItemReqResp(GOOD_ID, SOURCE_ID, new Item());
    this.indexingService.push(GOOD_ID, new PushItem());
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testPushItemError() throws IOException, InterruptedException {
    doAnswer(
            invocation -> {
              Items.Push pushRequest = invocation.getArgument(0);
              assertEquals(
                  new PushItemRequest()
                      .setItem(new PushItem())
                      .setConnectorName("datasources/source/connectors/unitTest")
                      .setDebugOptions(new DebugOptions().setEnableDebugging(false)),
                  pushRequest.getJsonContent());
              return getExceptionFuture(HTTP_FORBIDDEN_ERROR);
            })
        .when(batchingService)
        .pushItem(any());
    try {
      this.indexingService.push(BAD_ID, new PushItem()).get();
      fail("Should have thrown HTTP_FORBIDDEN exception.");
    } catch (ExecutionException e) {
      validateApiError(e, HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testPushItemNull() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    this.indexingService.push(GOOD_ID, null);
  }

  /* unreserve */
  @Test
  public void testUnreserveItem() throws IOException {
    this.transport.addUnreserveItemsReqResp(SOURCE_ID, OPERATION_DONE);
    this.indexingService.unreserve("queueName");
    verify(quotaServer, times(1)).acquire(Operations.DEFAULT);
  }

  @Test
  public void testUnreserveItemError() throws IOException, InterruptedException {
    doAnswer(
            invocation -> {
              Items.Unreserve unreserveRequest = invocation.getArgument(0);
              assertEquals(
                  new UnreserveItemsRequest()
                      .setQueue("queueName")
                      .setConnectorName("datasources/source/connectors/unitTest")
                      .setDebugOptions(new DebugOptions().setEnableDebugging(false)),
                  unreserveRequest.getJsonContent());
              return getExceptionFuture(HTTP_FORBIDDEN_ERROR);
            })
        .when(batchingService)
        .unreserveItem(any());
    try {
      this.indexingService.unreserve("queueName").get();
      fail("Should have thrown HTTP_FORBIDDEN exception.");
    } catch (ExecutionException e) {
      validateApiError(e, HTTP_FORBIDDEN);
    }
  }

  @Test
  public void testGetOperation() throws IOException {
    String operationName = "operations/testName";
    this.transport.addGetOperationReqResp(operationName, new Operation().setName(operationName));
    Operation result = this.indexingService.getOperation(operationName);
    assertEquals(operationName, result.getName());
  }

  private void validateApiError(ExecutionException e, int errorCode) {
    assertTrue(e.getCause() instanceof GoogleJsonResponseException);
    GoogleJsonResponseException jsonError = (GoogleJsonResponseException) (e.getCause());
    assertEquals(jsonError.getStatusCode(), errorCode);
  }

  private static <T> ListenableFuture<T> getExceptionFuture(GoogleJsonError e) {
    SettableFuture<T> settable = SettableFuture.create();
    GoogleJsonResponseException exception =
        new GoogleJsonResponseException(
            new Builder(e.getCode(), e.getMessage(), new HttpHeaders()), e);
    settable.setException(exception);
    return settable;
  }
}
