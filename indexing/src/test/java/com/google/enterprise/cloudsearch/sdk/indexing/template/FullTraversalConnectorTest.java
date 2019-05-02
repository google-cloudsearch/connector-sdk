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
package com.google.enterprise.cloudsearch.sdk.indexing.template;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.json.GenericJson;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl.InheritanceType;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingConnectorContext;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link FullTraversalConnector}. */

@RunWith(MockitoJUnitRunner.class)
public class FullTraversalConnectorTest {

  private static final String TEST_USER_READERS = "google:user1@mydomain.com,user2";
  private static final String TEST_GROUP_READERS = "google:group1@mydomain.com,group2";
  private static final String TEST_USER_DENIED = "google:user3@mydomain.com,user4";
  private static final String TEST_GROUP_DENIED = "google:group3@mydomain.com,group4";
  private static final String QUEUE_A = "FullTraversal||A: testQueue";
  private static final String QUEUE_B = "FullTraversal||B: testQueue";
  private static final QueueCheckpoint.QueueData QUEUE_A_CHECKPOINT =
      new QueueCheckpoint.QueueData().setQueueName(QUEUE_A);
  private static final QueueCheckpoint.QueueData QUEUE_B_CHECKPOINT =
      new QueueCheckpoint.QueueData().setQueueName(QUEUE_B);
  private static final byte[] QUEUE_A_CHECKPOINT_BYTES = QUEUE_A_CHECKPOINT.get();
  private static final byte[] QUEUE_B_CHECKPOINT_BYTES = QUEUE_B_CHECKPOINT.get();
  private static final ItemAcl DOMAIN_PUBLIC_ACL =
      new Acl.Builder()
          .setReaders(ImmutableList.of(Acl.getCustomerPrincipal()))
          .build()
          .applyTo(new Item())
          .getAcl();

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock private IndexingService indexingServiceMock;
  @Mock private Repository repositoryMock;
  @Mock private IndexingConnectorContext connectorContextMock;
  @Mock private CheckpointHandler checkpointHandlerMock;

  @Captor private ArgumentCaptor<Item> itemCaptor;
  @Captor private ArgumentCaptor<Item> itemListCaptor;
  @Captor private ArgumentCaptor<ByteArrayContent> contentCaptor;
  @Captor private ArgumentCaptor<RepositoryContext> repositoryContextCaptor;

  // Mocks don't call default interface methods, so use an implementation here, wrapped
  // with spy() to allow for verify calls.
  @Spy private ApiOperation errorOperation = new ApiOperation() {
      @Override
      public List<GenericJson> execute(IndexingService service) throws IOException {
        throw new IOException();
      }
    };
  @Spy private ApiOperation successOperation = new ApiOperation() {
      @Override
      public List<GenericJson> execute(IndexingService service) throws IOException {
        return Lists.newArrayList();
      }
    };

  private List<byte[]> checkpointHolder = new ArrayList<>();

  private enum DefaultAclChoices {
    PUBLIC,
    COMMON,
    INDIVIDUAL
  }

  @Before
  public void setUp() throws IOException {
    when(connectorContextMock.getIndexingService()).thenReturn(indexingServiceMock);

    // Mimic the CheckpointHandler behavior for queues. The queue data can be read
    // multiple times, and mocking the correct return value for each call is error-prone.
    checkpointHolder.clear();
    checkpointHolder.add(QUEUE_A_CHECKPOINT_BYTES);
    doAnswer(invocation -> checkpointHolder.get(0))
        .when(checkpointHandlerMock).readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE);
    doAnswer(invocation -> {
          checkpointHolder.set(0, invocation.getArgument(1));
          return null;
        })
        .when(checkpointHandlerMock)
        .saveCheckpoint(eq(FullTraversalConnector.CHECKPOINT_QUEUE), any());
  }

  private byte[] defaultCheckPoint = "checkpoint".getBytes();

  /**
   * Set the parameter values that would be normally set by the configuration file.
   *
   * @param numToAbort provides number of failure to abort the traverse process
   * @param aclChoice specifies which Acl mode to invoke
   */
  private void setConfig(String numToAbort, DefaultAclChoices aclChoice) {
    setConfig(numToAbort, aclChoice, null);
  }

  private void setConfig(String numToAbort, DefaultAclChoices aclChoice, Properties properties) {
    Properties configProperties = properties == null ? new Properties() : properties;
    switch (aclChoice) {
      case PUBLIC:
        configProperties.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
        configProperties.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
        break;
      case COMMON:
        configProperties.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
        configProperties.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "false");
        configProperties.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, TEST_USER_READERS);
        configProperties.put(DefaultAcl.DEFAULT_ACL_READERS_GROUPS, TEST_GROUP_READERS);
        configProperties.put(DefaultAcl.DEFAULT_ACL_DENIED_USERS, TEST_USER_DENIED);
        configProperties.put(DefaultAcl.DEFAULT_ACL_DENIED_GROUPS, TEST_GROUP_DENIED);
        break;
      case INDIVIDUAL:
        configProperties.put(DefaultAcl.DEFAULT_ACL_MODE, "none");
        break;
    }

    configProperties.put(TraverseExceptionHandlerFactory.TRAVERSE_EXCEPTION_HANDLER, numToAbort);
    configProperties.put(FullTraversalConnector.NUM_THREADS, "5");
    configProperties.put(FullTraversalConnector.TRAVERSE_QUEUE_TAG, "testQueue");
    setupConfig.initConfig(configProperties);
  }

  @Test
  public void testDefaultId() throws Exception {
    FullTraversalConnector connector = new FullTraversalConnector(repositoryMock);
    assertEquals(repositoryMock.getClass().getName(), connector.getDefaultId());
  }

  @Test
  public void testEventBusIsInitialized() throws Exception {
    setConfig("ignore", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector = new FullTraversalConnector(repositoryMock);
    connector.init(connectorContextMock);
    verify(repositoryMock).init(repositoryContextCaptor.capture());
    RepositoryContext rc = repositoryContextCaptor.getAllValues().get(0);
    // This will throw an IllegalArgumentException if the connector wasn't registered.
    rc.getEventBus().unregister(connector);
  }

  @Test
  public void testDefaultAclModeIsInitialized() throws Exception {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "override");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "google:user1@example.com");
    setupConfig.initConfig(config);
    SettableFuture<Operation> updateFuture = SettableFuture.create();
    updateFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.indexItem(any(), any())).thenReturn(updateFuture);
    FullTraversalConnector connector = new FullTraversalConnector(repositoryMock);
    connector.init(connectorContextMock);
    verify(repositoryMock).init(repositoryContextCaptor.capture());
    RepositoryContext rc = repositoryContextCaptor.getAllValues().get(0);
    assertEquals(DefaultAclMode.OVERRIDE, rc.getDefaultAclMode());
  }

  @Test
  public void testHandleAsyncOperation() throws Exception {
    AsyncApiOperation pushAsyncOperation = new AsyncApiOperation(errorOperation);
    AsyncApiOperation deleteAsyncOperation = new AsyncApiOperation(successOperation);
    setConfig("ignore", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    connector.handleAsyncOperation(pushAsyncOperation);
    connector.handleAsyncOperation(deleteAsyncOperation);
    connector.destroy();
    assertNotNull(deleteAsyncOperation.getResult().get());
    verify(errorOperation).execute(indexingServiceMock);
    verify(successOperation).execute(indexingServiceMock);
    thrown.expect(ExecutionException.class);
    pushAsyncOperation.getResult().get();
  }

  @Test
  public void testTraversePublicAcl() throws Exception {
    // target targetItems
    final int numberOfItems = 5;
    Map<String, String> targetItems = new HashMap<>();
    List<ApiOperation> docs = new ArrayList<>();
    for (int i = 0; i < numberOfItems; i++) {
      String id = "Test" + i;
      String content = "Test content " + i;
      targetItems.put(id, content);
      Item item = new Item();
      item.setName(id);
      docs.add(
          new RepositoryDoc.Builder()
              .setItem(item)
              .setRequestMode(RequestMode.SYNCHRONOUS)
              .setContent(ByteArrayContent.fromString("text/html", content), ContentFormat.HTML)
              .build());
      SettableFuture<Item> updateFuture = SettableFuture.create();
      doAnswer(
              invocation -> {
                updateFuture.set(new Item());
                return updateFuture;
              })
          .when(indexingServiceMock)
          .indexItemAndContent(
              any(), any(), any(), eq(ContentFormat.HTML), eq(RequestMode.SYNCHRONOUS));
    }
    docs.add(
        ApiOperations.deleteItem("delete id", "myVersion".getBytes(), RequestMode.SYNCHRONOUS));

    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    deleteFuture.set(new Operation());
    doAnswer(invocation -> deleteFuture).when(indexingServiceMock)
        .deleteItem("delete id", "myVersion".getBytes(), RequestMode.SYNCHRONOUS);
    CheckpointCloseableIterable<ApiOperation> opIterableSpy =
        Mockito.spy(new CheckpointCloseableIterableImpl.Builder<>(docs).build());
    when(repositoryMock.getAllDocs(null)).thenReturn(opIterableSpy);
    when(connectorContextMock.getIndexingService()).thenReturn(indexingServiceMock);
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);

    // full traversal test
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    setConfig("0", DefaultAclChoices.PUBLIC);
    connector.init(connectorContextMock);
    connector.traverse();

    InOrder inOrderCheck = inOrder(indexingServiceMock);
    // one update item with content for each of five targetItems in the db
    inOrderCheck
        .verify(indexingServiceMock, times(numberOfItems))
        .indexItemAndContent(
            itemListCaptor.capture(),
            contentCaptor.capture(),
            eq(null),
            eq(ContentFormat.HTML),
            eq(RequestMode.SYNCHRONOUS));
    // cannot manage the order of updateItemAndContent and delete id
    verify(indexingServiceMock)
        .deleteItem("delete id", "myVersion".getBytes(), RequestMode.SYNCHRONOUS);
    List<Item> allItems = itemListCaptor.getAllValues();
    List<ByteArrayContent> allContent = contentCaptor.getAllValues();
    assertEquals(allItems.size(), numberOfItems);
    for (int i = 0; i < numberOfItems; i++) {
      assertThat(targetItems.keySet(), hasItem(allItems.get(i).getName())); // verify item
      assertEquals(QUEUE_A, allItems.get(i).getQueue());
      String html =
          CharStreams.toString(
              new InputStreamReader(allContent.get(i).getInputStream(), UTF_8));
      assertEquals(targetItems.get(allItems.get(i).getName()), html); // verify content
      assertEquals(DOMAIN_PUBLIC_ACL, allItems.get(i).getAcl());
    }
    // closable iterable closed
    verify(opIterableSpy).close();
    verify(indexingServiceMock).deleteQueueItems(QUEUE_B);
  }

  @Test
  public void testTraversePublicAclNoContainers() throws Exception {
    // target targetItems
    final int numberOfItems = 5;
    Map<String, String> targetItems = new HashMap<>();
    List<ApiOperation> docs = new ArrayList<>();

    for (int i = 0; i < numberOfItems; i++) {
      String id = "Test" + i;
      String content = "Test content " + i;
      targetItems.put(id, content);
      Item item = new Item();
      item.setName(id);
      docs.add(
          new RepositoryDoc.Builder()
              .setItem(item)
              .setRequestMode(RequestMode.SYNCHRONOUS)
              .setContent(ByteArrayContent.fromString("text/html", content), ContentFormat.HTML)
              .build());
    }
    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
        invocation -> {
          updateFuture.set(new Item());
          return updateFuture;
        })
        .when(indexingServiceMock)
        .indexItemAndContent(any(), any(), any(), eq(ContentFormat.HTML),
            eq(RequestMode.SYNCHRONOUS));
    docs.add(ApiOperations.deleteItem("delete id"));
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
        invocation -> {
          deleteFuture.set(new Operation());
          return deleteFuture;
        })
    .when(indexingServiceMock)
    .deleteItem("delete id", null, RequestMode.UNSPECIFIED);
    CheckpointCloseableIterable<ApiOperation> opIterableSpy =
        Mockito.spy(new CheckpointCloseableIterableImpl.Builder<>(docs).build());
    when(repositoryMock.getAllDocs(null)).thenReturn(opIterableSpy);
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);

    // full traversal test
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    Properties configProperties = new Properties();
    configProperties.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    configProperties.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    configProperties.put(TraverseExceptionHandlerFactory.TRAVERSE_EXCEPTION_HANDLER, "0");
    configProperties.put(FullTraversalConnector.NUM_THREADS, "50");
    configProperties.put(FullTraversalConnector.TRAVERSE_QUEUE_TAG, "testQueue");
    setupConfig.initConfig(configProperties);
    connector.init(connectorContextMock);
    connector.traverse();

    // one update item with content for each of five targetItems in the db
    verify(indexingServiceMock, times(numberOfItems))
        .indexItemAndContent(
            itemListCaptor.capture(),
            contentCaptor.capture(),
            eq(null),
            eq(ContentFormat.HTML),
            eq(RequestMode.SYNCHRONOUS));
    verify(indexingServiceMock).deleteItem("delete id", null, RequestMode.UNSPECIFIED);
    List<Item> allItems = itemListCaptor.getAllValues();
    List<ByteArrayContent> allContent = contentCaptor.getAllValues();
    assertEquals(allItems.size(), numberOfItems);
    for (int i = 0; i < numberOfItems; i++) {
      assertThat(targetItems.keySet(), hasItem(allItems.get(i).getName())); // verify item
      assertEquals(null, allItems.get(i).getMetadata().getContainerName()); // no container
      String html =
          CharStreams.toString(
              new InputStreamReader(allContent.get(i).getInputStream(), UTF_8));
      assertEquals(targetItems.get(allItems.get(i).getName()), html); // verify content
      assertEquals(DOMAIN_PUBLIC_ACL, allItems.get(i).getAcl());
    }
    // closable iterable closed
    verify(opIterableSpy).close();
    verify(indexingServiceMock).deleteQueueItems(QUEUE_B);
    verifyNoMoreInteractions(indexingServiceMock);
  }

  @Test
  public void testTraverseNotIncrementalCommonAcl() throws Exception {
    // target targetItems
    int numberOfItems = 5;
    Map<String, String> targetItems = new HashMap<>();
    List<ApiOperation> docs = new ArrayList<>();
    for (int i = 0; i < numberOfItems; i++) {
      String id = "Test" + i;
      String content = "Test content " + i;
      targetItems.put(id, content);
      Item item = new Item();
      item.setName(id);
      docs.add(
          new RepositoryDoc.Builder()
              .setItem(item)
              .setRequestMode(RequestMode.ASYNCHRONOUS)
              .setContent(ByteArrayContent.fromString("text/html", content), ContentFormat.HTML)
              .build());
    }

    // DefaultAcl
    doAnswer(
        invocation -> {
          SettableFuture<Operation> result = SettableFuture.create();
          result.set(new Operation().setDone(true));
          return result;
        })
        .when(indexingServiceMock)
        .indexItem(
            argThat(item -> item.getName().equals(DefaultAcl.DEFAULT_ACL_NAME_DEFAULT)),
            eq(RequestMode.SYNCHRONOUS));
    // Documents
    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
        invocation -> {
          updateFuture.set(new Item());
          return updateFuture;
        })
        .when(indexingServiceMock)
        .indexItemAndContent(
            any(), any(), any(), eq(ContentFormat.HTML), eq(RequestMode.ASYNCHRONOUS));
    when(repositoryMock.getAllDocs(null))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(docs).build());
    // Delete other queue
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);

    // full traversal test
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);

    setConfig("0", DefaultAclChoices.COMMON);

    connector.init(connectorContextMock);
    connector.traverse();

    InOrder inOrderCheck = inOrder(indexingServiceMock);
    // one update item with content for each of five targetItems in the db
    inOrderCheck
        .verify(indexingServiceMock, times(numberOfItems))
        .indexItemAndContent(
            itemListCaptor.capture(),
            contentCaptor.capture(),
            eq(null),
            eq(ContentFormat.HTML),
            eq(RequestMode.ASYNCHRONOUS));
    List<Item> allItems = itemListCaptor.getAllValues();
    List<ByteArrayContent> allContent = contentCaptor.getAllValues();
    assertEquals(allItems.size(), numberOfItems);
    for (int i = 0; i < numberOfItems; i++) {
      assertThat(targetItems.keySet(), hasItem(allItems.get(i).getName())); // verify item
      String html =
          CharStreams.toString(
              new InputStreamReader(allContent.get(i).getInputStream(), UTF_8));
      assertEquals(html, targetItems.get(allItems.get(i).getName())); // verify content
      ItemAcl acl = allItems.get(i).getAcl();
      assertEquals(DefaultAcl.DEFAULT_ACL_NAME_DEFAULT, acl.getInheritAclFrom());
      assertEquals(InheritanceType.PARENT_OVERRIDE.name(), acl.getAclInheritanceType());
    }
  }

  @Test
  public void testTraverseException() throws Exception {
    Item item = new Item().setName("Test id");
    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(item)
            .setContent(
                ByteArrayContent.fromString("text/html", "Test content"), ContentFormat.HTML)
            .build();
    when(repositoryMock.getAllDocs(null))
        .thenReturn(
            new CheckpointCloseableIterableImpl.Builder<ApiOperation>(
                    Collections.singletonList(doc))
                .build());

    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.setException(new IOException("Testing Error"));
              return updateFuture;
            })
        .when(indexingServiceMock)
        .indexItemAndContent(any(), any(), any(), eq(ContentFormat.HTML), any());

    setConfig("0", DefaultAclChoices.PUBLIC);

    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    thrown.expect(IOException.class);
    thrown.expectMessage(containsString("Testing Error"));
    connector.traverse();
  }

  @Test
  public void testGetAllDocsException() throws Exception {
    when(repositoryMock.getAllDocs(null)).thenThrow(RepositoryException.class);
    setConfig("ignore", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    thrown.expect(RepositoryException.class);
    connector.traverse();
  }

  @Test
  public void testTraverseIgnoreHandler() throws Exception {
    setConfig("ignore", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    List<ApiOperation> docs = new ArrayList<>();
    List<Item> items = new ArrayList<>();
    List<ByteArrayContent> contents = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      Item item = new Item().setName("Test id" + i);
      ByteArrayContent content = ByteArrayContent.fromString("text/html", "Test content" + i);
      RepositoryDoc doc =
          new RepositoryDoc.Builder().setItem(item).setContent(content, ContentFormat.HTML).build();
      docs.add(doc);
      items.add(item);
      contents.add(content);
    }
    when(repositoryMock.getAllDocs(null))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(docs).build());

    for (int i = 0; i < 4; i++) {
      SettableFuture<Item> exceptionFuture = SettableFuture.create();
      StringBuilder sb = new StringBuilder();
      sb.append("Test exception");
      sb.append(i + 1);
      doAnswer(
              invocation -> {
                exceptionFuture.setException(new IOException(sb.toString()));
                return exceptionFuture;
              })
          .when(indexingServiceMock)
          .indexItemAndContent(
              eq(items.get(i)),
              eq(contents.get(i)),
              eq(null),
              eq(ContentFormat.HTML),
              eq(RequestMode.UNSPECIFIED));
    }

    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(indexingServiceMock)
        .indexItemAndContent(
            eq(items.get(4)),
            eq(contents.get(4)),
            eq(null),
            eq(ContentFormat.HTML),
            eq(RequestMode.UNSPECIFIED));
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);

    connector.traverse();
  }

  @Test
  public void testTraverseSkipHandlerSucceed() throws Exception {
    setConfig("3", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    List<ApiOperation> docs = new ArrayList<>();
    List<Item> items = new ArrayList<>();
    List<ByteArrayContent> contents = new ArrayList<>();
    for (int i = 1; i <= 10; i++) {

      Item item = new Item().setName("Test id" + i);
      items.add(item);
      ByteArrayContent byteArrayContent =
          ByteArrayContent.fromString("text/html", "Test " + "content" + i);
      RepositoryDoc doc =
          new RepositoryDoc.Builder()
              .setItem(item)
              .setContent(byteArrayContent, ContentFormat.HTML)
              .build();
      contents.add(byteArrayContent);
      docs.add(doc);
    }
    CheckpointCloseableIterable<ApiOperation> opIterableSpy =
        Mockito.spy(new CheckpointCloseableIterableImpl.Builder<>(docs).build());
    when(repositoryMock.getAllDocs(null)).thenReturn(opIterableSpy);

    for (int i = 0; i < 3; i++) {
      SettableFuture<Item> exceptionFuture = SettableFuture.create();
      StringBuilder sb = new StringBuilder();
      sb.append("Test exception");
      sb.append(i + 1);
      doAnswer(
              invocation -> {
                exceptionFuture.setException(new IOException(sb.toString()));
                return exceptionFuture;
              })
          .when(indexingServiceMock)
          .indexItemAndContent(
              eq(items.get(i)),
              eq(contents.get(i)),
              eq(null),
              eq(ContentFormat.HTML),
              eq(RequestMode.UNSPECIFIED));
    }

    for (int i = 3; i < 10; i++) {
      SettableFuture<Item> updateFuture = SettableFuture.create();
      doAnswer(
              invocation -> {
                updateFuture.set(new Item());
                return updateFuture;
              })
          .when(indexingServiceMock)
          .indexItemAndContent(
              eq(items.get(i)),
              eq(contents.get(i)),
              eq(null),
              eq(ContentFormat.HTML),
              eq(RequestMode.UNSPECIFIED));
    }
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);

    connector.traverse();
    verify(opIterableSpy).close();
    verify(indexingServiceMock, times(10))
        .indexItemAndContent(any(), any(), any(), eq(ContentFormat.HTML), any());
  }

  @Test
  public void testTraverseSkipHandlerAbort() throws Exception {
    setConfig("3", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    List<ApiOperation> docs = new ArrayList<>();
    List<Item> items = new ArrayList<>();
    List<ByteArrayContent> contents = new ArrayList<>();
    for (int i = 1; i <= 4; i++) {
      Item item = new Item().setName("Test id" + i);
      ByteArrayContent content = ByteArrayContent.fromString("text/html", "Test content" + i);
      RepositoryDoc doc =
          new RepositoryDoc.Builder().setItem(item).setContent(content, ContentFormat.HTML).build();
      docs.add(doc);
      items.add(item);
      contents.add(content);
    }
    when(repositoryMock.getAllDocs(null))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(docs).build());

    for (int i = 0; i < 4; i++) {
      SettableFuture<Item> updateFuture = SettableFuture.create();
      StringBuilder sb = new StringBuilder();
      sb.append("Test exception");
      sb.append(i + 1);
      doAnswer(
              invocation -> {
                updateFuture.setException(new IOException(sb.toString()));
                return updateFuture;
              })
          .when(indexingServiceMock)
          .indexItemAndContent(
              eq(items.get(i)),
              eq(contents.get(i)),
              eq(null),
              eq(ContentFormat.HTML),
              eq(RequestMode.UNSPECIFIED));
    }
    thrown.expect(IOException.class);
    thrown.expectMessage(containsString("Test exception"));
    connector.traverse();
  }

  @Test
  public void testTraverseSkipHandlerAbortWithDeleteItem() throws Exception {
    setConfig("3", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    List<ApiOperation> docs = new ArrayList<>();
    List<Item> items = new ArrayList<>();
    List<ByteArrayContent> contents = new ArrayList<>();
    // Delete[fail], Update[fail], Update[success], Delete[fail], Update[fail], Update[success]
    for (int i = 0; i < 6; i++) { // extra docs may not be fetched
      if ((i % 3) == 0) {
        docs.add(ApiOperations.deleteItem("Delete id"));
        items.add(new Item());
        contents.add(new ByteArrayContent("test", new byte[] {}));
      } else {
        Item item = new Item().setName("Test id" + i);
        items.add(item);
        ByteArrayContent content = ByteArrayContent.fromString("text/html", "Test content" + i);
        contents.add(content);
        ApiOperation doc =
            new RepositoryDoc.Builder()
                .setItem(item)
                .setContent(content, ContentFormat.HTML)
                .build();
        docs.add(doc);
      }
    }
    when(repositoryMock.getAllDocs(null))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(docs).build());

    for (int i = 0; i < 6; i++) {
      final int ii = i;
      if ((i % 3) == 0) {
        SettableFuture<Operation> deleteFuture = SettableFuture.create();
        doAnswer(
                invocation -> {
                  deleteFuture.setException(new IOException("Test exception" + ii));
                  return deleteFuture;
                })
            .when(indexingServiceMock)
            .deleteItem("Delete id", null, RequestMode.UNSPECIFIED);
      } else {
        SettableFuture<Item> updateFuture = SettableFuture.create();
        doAnswer(
                invocation -> {
                  if ((ii % 3) == 1) {
                    updateFuture.setException(new IOException("Test exception" + ii));
                  } else {
                    updateFuture.set(new Item());
                  }
                  return updateFuture;
                })
            .when(indexingServiceMock)
            .indexItemAndContent(
                items.get(i),
                contents.get(i),
                null,
                ContentFormat.HTML,
                RequestMode.UNSPECIFIED);
      }
    }

    thrown.expect(IOException.class);
    thrown.expectMessage(containsString("Test exception"));
    connector.traverse();
  }

  @Test
  public void testTraverseExceptionInDeleteQueueItems() throws Exception {
    SettableFuture<Operation> result = SettableFuture.create();
    result.setException(new ExecutionException("outer", new IOException("inner")));
    doAnswer(invocation -> result).when(indexingServiceMock).deleteQueueItems(any());
    when(repositoryMock.getAllDocs(null))
        .thenReturn(
            new CheckpointCloseableIterableImpl.Builder<>(Collections.<ApiOperation>emptyList())
            .build());
    setConfig("0", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    // No exception thrown.
    connector.traverse();
  }

  @Test
  public void testChangeHandlerPublicAcl() throws Exception {
    // target operations
    final int numberOfItems = 5;
    Map<String, String> targetItems = new HashMap<>();
    List<ApiOperation> docs = new ArrayList<>();

    for (int i = 0; i < numberOfItems; i++) {
      String id = "Test" + i;
      String content = "Test content " + i;
      targetItems.put(id, content);
      Item item = new Item();
      item.setName(id);
      ByteArrayContent byteArrayContent = ByteArrayContent.fromString("text/html", content);
      docs.add(
          new RepositoryDoc.Builder()
              .setItem(item)
              .setRequestMode(RequestMode.SYNCHRONOUS)
              .setContent(byteArrayContent, ContentFormat.HTML)
              .build());

      SettableFuture<Item> updateFuture = SettableFuture.create();
      doAnswer(
              invocation -> {
                updateFuture.set(new Item());
                return updateFuture;
              })
          .when(indexingServiceMock)
          .indexItemAndContent(
              eq(item),
              eq(byteArrayContent),
              eq(null),
              eq(ContentFormat.HTML),
              eq(RequestMode.SYNCHRONOUS));
    }

    docs.add(ApiOperations.deleteItem("delete id"));
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    deleteFuture.set(new Operation());
    doAnswer(invocation -> deleteFuture)
        .when(indexingServiceMock)
        .deleteItem("delete id", null, RequestMode.UNSPECIFIED);

    byte[] payload = "Payload Value".getBytes();
    byte[] newPayload = "New Payload Value".getBytes();
    when(checkpointHandlerMock
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_INCREMENTAL))
        .thenReturn(payload);
    CheckpointCloseableIterable<ApiOperation> incrementalChanges =
        new CheckpointCloseableIterableImpl.Builder<>(docs).setCheckpoint(newPayload).build();
    when(repositoryMock.getChanges(any())).thenReturn(incrementalChanges);

    // incremental handler test
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    setConfig("0", DefaultAclChoices.PUBLIC);
    connector.init(connectorContextMock);
    connector.handleIncrementalChanges();

    // verify
    InOrder inOrderCheck = inOrder(indexingServiceMock, checkpointHandlerMock);
    // one get for the check point entry
    inOrderCheck.verify(checkpointHandlerMock)
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE);
    inOrderCheck.verify(checkpointHandlerMock)
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_INCREMENTAL);
    // one update item with content for each of five targetItems in the db
    inOrderCheck
        .verify(indexingServiceMock, times(numberOfItems))
        .indexItemAndContent(
            itemListCaptor.capture(),
            contentCaptor.capture(),
            eq(null),
            eq(ContentFormat.HTML),
            eq(RequestMode.SYNCHRONOUS));
    // one delete operation
    verify(indexingServiceMock).deleteItem("delete id", null, RequestMode.UNSPECIFIED);
    inOrderCheck.verify(checkpointHandlerMock)
        .saveCheckpoint(FullTraversalConnector.CHECKPOINT_INCREMENTAL, newPayload);
    // verify objects in parameters
    List<Item> allItems = itemListCaptor.getAllValues();
    List<ByteArrayContent> allContent = contentCaptor.getAllValues();
    assertEquals(allItems.size(), numberOfItems);
    for (int i = 0; i < numberOfItems; i++) {
      assertThat(targetItems.keySet(), hasItem(allItems.get(i).getName())); // verify item
      String html =
          CharStreams.toString(
              new InputStreamReader(allContent.get(i).getInputStream(), UTF_8));
      assertEquals(targetItems.get(allItems.get(i).getName()), html); // verify content
      assertEquals(DOMAIN_PUBLIC_ACL, allItems.get(i).getAcl());
    }
    verifyNoMoreInteractions(indexingServiceMock, checkpointHandlerMock);
  }

  @Test
  public void testChangeHandlerIndividualAcl() throws Exception {
    // target operations
    List<ApiOperation> docs = new ArrayList<>();
    Item item = new Item();
    item.setName("Test1");
    docs.add(
        new RepositoryDoc.Builder()
            .setItem(item)
            .setRequestMode(RequestMode.SYNCHRONOUS)
            .setContent(
                ByteArrayContent.fromString("text/html", "Test content 1"), ContentFormat.HTML)
            .build());

    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(indexingServiceMock)
        .indexItemAndContent(any(), any(), any(), eq(ContentFormat.HTML),
            eq(RequestMode.SYNCHRONOUS));

    String payload = "Payload Value";
    when(checkpointHandlerMock
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_INCREMENTAL))
        .thenReturn(payload.getBytes());
    CheckpointCloseableIterable<ApiOperation> incrementalChanges =
        new CheckpointCloseableIterableImpl.Builder<>(docs).build();
    when(repositoryMock.getChanges(any())).thenReturn(incrementalChanges);

    // incremental handler test
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    Properties configProperties = new Properties();
    configProperties.put(DefaultAcl.DEFAULT_ACL_MODE, "none");
    configProperties.put(TraverseExceptionHandlerFactory.TRAVERSE_EXCEPTION_HANDLER, "0");
    configProperties.put(FullTraversalConnector.NUM_THREADS, "50");
    setupConfig.initConfig(configProperties);
    connector.init(connectorContextMock);
    connector.handleIncrementalChanges();

    // verify
    InOrder inOrderCheck = inOrder(indexingServiceMock, checkpointHandlerMock);
    // one get for the check point entry
    inOrderCheck.verify(checkpointHandlerMock)
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE);
    inOrderCheck.verify(checkpointHandlerMock)
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_INCREMENTAL);
    // one update item with content
    verify(indexingServiceMock)
        .indexItemAndContent(
            itemCaptor.capture(),
            contentCaptor.capture(),
            eq(null),
            eq(ContentFormat.HTML),
            eq(RequestMode.SYNCHRONOUS));
    verify(checkpointHandlerMock)
        .saveCheckpoint(FullTraversalConnector.CHECKPOINT_INCREMENTAL, null);
    assertEquals(item.getName(), itemCaptor.getValue().getName());
    assertNull(itemCaptor.getValue().getAcl());
    verifyNoMoreInteractions(indexingServiceMock, checkpointHandlerMock);
  }

  @Test
  public void testChangeHandlerNoChanges() throws Exception {
    // targets and mocks
    byte[] payload = "Payload Value".getBytes();
    when(checkpointHandlerMock
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_INCREMENTAL))
        .thenReturn(payload);
    when(repositoryMock.getChanges(any())).thenReturn(null);

    // incremental handler test
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    setConfig("0", DefaultAclChoices.INDIVIDUAL);
    connector.init(connectorContextMock);
    connector.handleIncrementalChanges();

    // verify
    InOrder inOrderCheck = inOrder(checkpointHandlerMock);
    inOrderCheck.verify(checkpointHandlerMock)
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE);
    inOrderCheck.verify(checkpointHandlerMock)
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_INCREMENTAL);
    verifyNoMoreInteractions(checkpointHandlerMock);
  }

  @Test
  public void testChangeHandlerSkipHandlerAbortWithDeleteItem() throws Exception {
    setConfig("3", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    List<ApiOperation> docs = new ArrayList<>();
    List<Item> items = new ArrayList<>();
    List<ByteArrayContent> contents = new ArrayList<>();
    // Delete[fail], Update[fail], Update[success], Delete[fail], Update[fail], Update[success]
    for (int i = 0; i < 6; i++) { // extra docs may not be fetched
      if ((i % 3) == 0) {
        docs.add(ApiOperations.deleteItem("Delete id"));
        items.add(new Item());
        contents.add(new ByteArrayContent("test", new byte[] {}));
      } else {
        Item item = new Item().setName("Test id" + i);
        items.add(item);
        ByteArrayContent content = ByteArrayContent.fromString("text/html", "Test content" + i);
        contents.add(content);
        ApiOperation doc =
            new RepositoryDoc.Builder()
                .setItem(item)
                .setContent(content, ContentFormat.HTML)
                .build();
        docs.add(doc);
      }
    }
    CheckpointCloseableIterable<ApiOperation> incrementalChanges =
        new CheckpointCloseableIterableImpl.Builder<>(docs)
            .setCheckpoint(defaultCheckPoint)
            .build();
    when(repositoryMock.getChanges(any())).thenReturn(incrementalChanges);

    for (int i = 0; i < 6; i++) {
      final int ii = i;
      if ((i % 3) == 0) {
        SettableFuture<Operation> deleteFuture = SettableFuture.create();
        doAnswer(
                invocation -> {
                  deleteFuture.setException(new IOException("Test exception" + ii));
                  return deleteFuture;
                })
            .when(indexingServiceMock)
            .deleteItem("Delete id", null, RequestMode.UNSPECIFIED);
      } else {
        SettableFuture<Item> updateFuture = SettableFuture.create();
        doAnswer(
                invocation -> {
                  if ((ii % 3) == 1) {
                    updateFuture.setException(new IOException("Test exception" + ii));
                  } else {
                    updateFuture.set(new Item());
                  }
                  return updateFuture;
                })
            .when(indexingServiceMock)
            .indexItemAndContent(
                items.get(i),
                contents.get(i),
                null,
                ContentFormat.HTML,
                RequestMode.UNSPECIFIED);
      }
    }

    thrown.expect(IOException.class);
    thrown.expectMessage(containsString("Test exception"));
    connector.handleIncrementalChanges();
  }

  @Test
  public void testChangeHandlerWithInvalidBatchedOperation() throws Exception {
    setConfig("3", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    List<ApiOperation> docs = new ArrayList<>();
    docs.add(ApiOperations.deleteItem("id1"));
    docs.add(ApiOperations.deleteItem("id2"));
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    deleteFuture.set(new Operation());
    doAnswer(invocation -> deleteFuture).when(indexingServiceMock)
        .deleteItem(any(), any(), any());
    ApiOperation batchOps = ApiOperations.batch(docs.iterator());
    CheckpointCloseableIterable<ApiOperation> incrementalChanges =
        new CheckpointCloseableIterableImpl.Builder<>(Collections.singletonList(batchOps))
            .setCheckpoint(defaultCheckPoint)
            .build();
    when(repositoryMock.getChanges(any())).thenReturn(incrementalChanges);
    connector.handleIncrementalChanges();
  }

  @Test
  public void testChangeHandlerFallbackAclOnItem() throws Exception {
    // When an item has its own acl, the default acl is not applied.
    List<ApiOperation> docs = createRepositoryDocsAndResponses(1);
    assertEquals(1, docs.size());
    Item item = ((RepositoryDoc) docs.get(0)).getItem();
    Acl acl = new Acl.Builder()
        .setReaders(Lists.newArrayList(Acl.getUserPrincipal("testuser")))
        .build();
    acl.applyTo(item);
    ItemAcl originalAcl = item.getAcl().clone();

    setConfig("0", DefaultAclChoices.COMMON);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    connector.handleIncrementalChanges();
    // verify (first indexed item is default acl container)
    verify(indexingServiceMock, times(2)).indexItem(itemListCaptor.capture(), any());
    assertEquals(originalAcl, itemListCaptor.getAllValues().get(1).getAcl());
  }

  @Test
  public void testSaveCheckPoint() throws IOException, InterruptedException {
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.saveCheckpoint(true);
  }

  @Test
  public void testDestroy() throws Exception {
    setupConfig.initConfig(new Properties());
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    connector.destroy();
    verify(repositoryMock).close();
    verify(repositoryMock).init(repositoryContextCaptor.capture());
    RepositoryContext rc = repositoryContextCaptor.getAllValues().get(0);
    // This will throw an IllegalArgumentException if the connector is no longer registered.
    thrown.expect(IllegalArgumentException.class);
    rc.getEventBus().unregister(connector);
  }

  @Test
  public void testStartAndShutDown() throws Exception {
    setConfig("3", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    verify(repositoryMock).init(any());
    connector.destroy();
    verify(repositoryMock).close();
  }

  @Test
  public void testConfigurationIsUninitialized() throws Exception {
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    try {
      connector.init(connectorContextMock);
      fail();
    } catch (IllegalStateException ex) {
      verify(repositoryMock, never()).init(any());
    }
  }

  @Test
  public void testInvalidAbortNum() throws Exception {
    setConfig("test", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    thrown.expect(InvalidConfigurationException.class);
    // TODO(tvartak): Full error message here is internal details for Configuration object.
    // We may want to make configuration key and value to be available as attributes on
    // InvalidConfigurationException. Then tests can specifically validate key and value.
    thrown.expectMessage(
        containsString(
            "Failed to parse configured value [test] for ConfigKey [traverse.exceptionHandler]"));
    connector.init(connectorContextMock);
    connector.destroy();
  }

  @Test
  public void testInitNoSavedQueueCheckpoint() throws Exception {
    setConfig("0", DefaultAclChoices.PUBLIC);
    when(checkpointHandlerMock.readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE))
        .thenReturn(null);

    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    assertEquals(QUEUE_A, connector.queueCheckpoint.getCurrentQueueName());
    verify(checkpointHandlerMock)
        .saveCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE, QUEUE_A_CHECKPOINT_BYTES);
  }

  @Test
  public void testInitWithSavedQueueCheckpoint() throws Exception {
    setConfig("0", DefaultAclChoices.PUBLIC);
    when(checkpointHandlerMock.readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE))
        .thenReturn(QUEUE_B_CHECKPOINT_BYTES);

    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    assertEquals(QUEUE_B, connector.queueCheckpoint.getCurrentQueueName());
    verify(checkpointHandlerMock).readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE);
    verifyNoMoreInteractions(checkpointHandlerMock);
  }

  @Test
  public void testTraverseWithQueueNames() throws Exception {
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);
    List<ApiOperation> docs = createRepositoryDocsAndResponses(3);
    FullTraversalConnector connector = createConnector(/* useQueues */ true);
    verifyQueueValue(docs, null);
    connector.traverse();
    verifyQueueValue(docs, QUEUE_A);
    verifyQueueCheckpointHandler(2, 1, QUEUE_B_CHECKPOINT_BYTES);
    verify(indexingServiceMock).deleteQueueItems(QUEUE_B);
  }

  @Test
  public void testTraverseRepeatedlyWithQueueNames() throws Exception {
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);
    FullTraversalConnector connector = createConnector(/* useQueues */ true);

    List<ApiOperation> docs1 = createRepositoryDocsAndResponses(3);
    connector.traverse();
    List<ApiOperation> docs2 = createRepositoryDocsAndResponses(3);
    connector.traverse();
    List<ApiOperation> docs3 = createRepositoryDocsAndResponses(3);
    connector.traverse();

    verifyQueueValue(docs1, QUEUE_A);
    verifyQueueValue(docs2, QUEUE_B);
    verifyQueueValue(docs3, QUEUE_A);
    InOrder inOrderCheck = inOrder(indexingServiceMock);
    inOrderCheck.verify(indexingServiceMock).deleteQueueItems(QUEUE_B);
    inOrderCheck.verify(indexingServiceMock).deleteQueueItems(QUEUE_A);
    inOrderCheck.verify(indexingServiceMock).deleteQueueItems(QUEUE_B);
  }

  @Test
  public void testTraverseWithPendingDeleteQueueItems() throws Exception {
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(false).setName("deleteQueueItemsName"));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);
    FullTraversalConnector connector = createConnector(/* useQueues */ true);

    List<ApiOperation> docs1 = createRepositoryDocsAndResponses(3);
    connector.traverse();

    verifyQueueValue(docs1, QUEUE_A);
    verify(indexingServiceMock).deleteQueueItems(QUEUE_B);
    verifyQueueCheckpointHandler(2, 1,
        new QueueCheckpoint.QueueData()
        .setQueueName(QUEUE_A) // checkpoint was saved with same queue id since delete isn't done
        .setOperationName("deleteQueueItemsName")
        .get());
  }

  @Test
  public void traverse_previousDeleteQueueItems_notDone() throws Exception {
    Operation deleteQueueItemsOperation = new Operation().setDone(false);
    when(indexingServiceMock.getOperation("nonempty")).thenReturn(deleteQueueItemsOperation);
    when(checkpointHandlerMock.readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE))
        .thenReturn(new QueueCheckpoint.QueueData().setOperationName("nonempty").get());

    FullTraversalConnector connector = createConnector(/* useQueues */ true);
    List<ApiOperation> docs = createRepositoryDocsAndResponses(3);
    connector.traverse();

    verify(repositoryMock, never()).getAllDocs(null);
  }

  @Test
  public void traverse_previousDeleteQueueItems_done() throws Exception {
    // Start of traversal checking delete queue operation.
    Operation deleteQueueItemsOperation = new Operation().setDone(true);
    when(indexingServiceMock.getOperation("nonempty")).thenReturn(deleteQueueItemsOperation);
    when(checkpointHandlerMock.readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE))
        .thenReturn(new QueueCheckpoint.QueueData().setOperationName("nonempty").get());

    // End of traversal running delete queue operation.
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(false).setName("deleteQueueItemsName"));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);

    FullTraversalConnector connector = createConnector(/* useQueues */ true);
    List<ApiOperation> docs = createRepositoryDocsAndResponses(3);
    connector.traverse();

    verify(repositoryMock).getAllDocs(null);
    verifyQueueValue(docs, QUEUE_B);
    verifyQueueCheckpointHandler(2, 3, QUEUE_B_CHECKPOINT_BYTES);
    verify(indexingServiceMock).deleteQueueItems(QUEUE_A);
  }

  @Test
  public void testTraverseWithoutQueueNames() throws Exception {
    List<ApiOperation> docs = createRepositoryDocsAndResponses(3);
    FullTraversalConnector connector = createConnector(/* useQueues */ false);
    verifyQueueValue(docs, null);
    connector.traverse();
    verifyQueueValue(docs, null);
    verifyQueueCheckpointHandler(0, 0, null);
    verify(indexingServiceMock, never()).deleteQueueItems(any());
  }

  @Test
  public void testHandleIncrementalChangesWithQueueNames() throws Exception {
    List<ApiOperation> docs = createRepositoryDocsAndResponses(3);
    FullTraversalConnector connector = createConnector(/* useQueues */ true);
    verifyQueueValue(docs, null);
    connector.handleIncrementalChanges();
    verifyQueueValue(docs, QUEUE_A);
    verifyQueueCheckpointHandler(1, 0, null);
  }

  @Test
  public void testHandleIncrementalChangesWithoutQueueNames() throws Exception {
    List<ApiOperation> docs = createRepositoryDocsAndResponses(3);
    FullTraversalConnector connector = createConnector(/* useQueues */ false);
    verifyQueueValue(docs, null);
    connector.handleIncrementalChanges();
    verifyQueueValue(docs, null);
    verifyQueueCheckpointHandler(0, 0, null);
  }

  private List<ApiOperation> createRepositoryDocsAndResponses(int count) throws IOException {
    List<ApiOperation> docs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      docs.add(new RepositoryDoc.Builder().setItem(new Item().setName("Test id" + i)).build());
    }
    when(repositoryMock.getAllDocs(null))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(docs).build());
    when(repositoryMock.getChanges(any()))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(docs).build());
    SettableFuture<Operation> updateFuture = SettableFuture.create();
    updateFuture.set(new Operation().setDone(true));
    doAnswer(invocation -> updateFuture)
        .when(indexingServiceMock)
        .indexItem(any(), any());
    return docs;
  }

  private FullTraversalConnector createConnector(boolean useQueues) throws Exception {
    Properties p = new Properties();
    p.put(FullTraversalConnector.TRAVERSE_USE_QUEUES, Boolean.toString(useQueues));
    setConfig("0", DefaultAclChoices.PUBLIC, p);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);
    return connector;
  }

  private void verifyQueueValue(List<ApiOperation> docs, String expected) {
    for (ApiOperation op : docs) {
      assertEquals(expected, ((RepositoryDoc) op).getItem().getQueue());
    }
  }

  private void verifyQueueCheckpointHandler(int timesRead, int timesSaved, byte[] savedValue)
      throws IOException {
    verify(checkpointHandlerMock, times(timesRead))
        .readCheckpoint(FullTraversalConnector.CHECKPOINT_QUEUE);
    ArgumentCaptor<byte[]> captor = ArgumentCaptor.forClass(byte[].class);
    verify(checkpointHandlerMock, times(timesSaved))
        .saveCheckpoint(eq(FullTraversalConnector.CHECKPOINT_QUEUE), captor.capture());
    if (savedValue != null) {
      QueueCheckpoint.QueueData expected =
          JsonCheckpoint.parse(savedValue, QueueCheckpoint.QueueData.class);
      QueueCheckpoint.QueueData actual =
          JsonCheckpoint.parse(captor.getValue(), QueueCheckpoint.QueueData.class);
      assertEquals(expected.getQueueName(), actual.getQueueName());
    }
  }

  @Test
  public void testHandleAsyncOperationWithQueueNames() throws Exception {
    RepositoryDoc doc = new RepositoryDoc.Builder().setItem(new Item().setName("Test id")).build();
    AsyncApiOperation docAsyncOperation = new AsyncApiOperation(doc);

    setConfig("ignore", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);

    assertNull(doc.getItem().getQueue());
    connector.handleAsyncOperation(docAsyncOperation);
    connector.destroy();
    assertEquals(QUEUE_A, doc.getItem().getQueue());
  }

  @Test
  public void testHandleAsyncOperationWithPreviousQueueName() throws Exception {
    RepositoryDoc doc = new RepositoryDoc.Builder().setItem(
        new Item().setName("Test id").setQueue("test queue")).build();
    AsyncApiOperation docAsyncOperation = new AsyncApiOperation(doc);

    setConfig("ignore", DefaultAclChoices.PUBLIC);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);

    assertEquals("test queue", doc.getItem().getQueue());
    connector.handleAsyncOperation(docAsyncOperation);
    connector.destroy();
    assertEquals(QUEUE_A, doc.getItem().getQueue());
  }

  @Test
  public void testHandleAsyncOperationWithoutQueueNames() throws Exception {
    RepositoryDoc doc = new RepositoryDoc.Builder().setItem(new Item().setName("Test id")).build();
    AsyncApiOperation docAsyncOperation = new AsyncApiOperation(doc);

    Properties p = new Properties();
    p.put(FullTraversalConnector.TRAVERSE_USE_QUEUES, "false");
    setConfig("0", DefaultAclChoices.PUBLIC, p);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    connector.init(connectorContextMock);

    assertNull(doc.getItem().getQueue());
    connector.handleAsyncOperation(docAsyncOperation);
    connector.destroy();
    assertNull(doc.getItem().getQueue());
  }

  @Test
  public void testInvalidPartitionSize() throws Exception {
    Properties p = new Properties();
    p.put(FullTraversalConnector.TRAVERSE_PARTITION_SIZE, "0");
    setConfig("0", DefaultAclChoices.PUBLIC, p);
    FullTraversalConnector connector = new FullTraversalConnector(repositoryMock);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("Partition size"));
    connector.init(connectorContextMock);
  }

  @Test
  public void testPartitionsAreProcessedTraverse() throws Exception {
    Properties p = new Properties();
    p.put(FullTraversalConnector.TRAVERSE_PARTITION_SIZE, "3");
    setConfig("0", DefaultAclChoices.PUBLIC, p);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    AtomicInteger counter1 = new AtomicInteger();
    ApiOperation fromPartition1 = new ApiOperation() {
        @Override
        public List<GenericJson> execute(IndexingService service) throws IOException {
          counter1.incrementAndGet();
          return Collections.emptyList();
        }
      };
    AtomicInteger counter2 = new AtomicInteger();
    ApiOperation fromPartition2 = new ApiOperation() {
        @Override
        public List<GenericJson> execute(IndexingService service) throws IOException {
          assertEquals(
              "partition 2 should not be processed before partition 1 is all done",
              3,
              counter1.get());
          counter2.incrementAndGet();
          return Collections.emptyList();
        }
      };
    ImmutableList<ApiOperation> allDocs =
        ImmutableList.of(
            fromPartition1,
            fromPartition1,
            fromPartition1,
            fromPartition2,
            fromPartition2,
            fromPartition2);
    connector.init(connectorContextMock);
    when(repositoryMock.getAllDocs(null))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(allDocs).build());
    SettableFuture<Operation> deleteQueueFuture = SettableFuture.create();
    deleteQueueFuture.set(new Operation().setDone(true));
    when(indexingServiceMock.deleteQueueItems(any())).thenReturn(deleteQueueFuture);
    connector.traverse();
    assertEquals(3, counter1.get());
    assertEquals(3, counter2.get());
  }

  @Test
  public void testPartitionsAreProcessedGetChanges() throws Exception {
    Properties p = new Properties();
    p.put(FullTraversalConnector.TRAVERSE_PARTITION_SIZE, "3");
    setConfig("0", DefaultAclChoices.PUBLIC, p);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    AtomicInteger counter1 = new AtomicInteger();
    ApiOperation fromPartition1 = new ApiOperation() {
        @Override
        public List<GenericJson> execute(IndexingService service) throws IOException {
          counter1.incrementAndGet();
          return Collections.emptyList();
        }
      };
    AtomicInteger counter2 = new AtomicInteger();
    ApiOperation fromPartition2 = new ApiOperation() {
        @Override
        public List<GenericJson> execute(IndexingService service) throws IOException {
          assertEquals(
              "partition 2 should not be processed before partition 1 is all done",
              3,
              counter1.get());
          counter2.incrementAndGet();
          return Collections.emptyList();
        }
      };
    ImmutableList<ApiOperation> allDocs =
        ImmutableList.of(
            fromPartition1,
            fromPartition1,
            fromPartition1,
            fromPartition2,
            fromPartition2,
            fromPartition2);
    connector.init(connectorContextMock);
    when(repositoryMock.getChanges(null))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(allDocs).build());
    connector.handleIncrementalChanges();
    assertEquals(3, counter1.get());
    assertEquals(3, counter2.get());
  }

  @Test
  public void testFurtherPartitionsAreNotProcessedWithAbort() throws Exception {
    Properties p = new Properties();
    p.put(FullTraversalConnector.TRAVERSE_PARTITION_SIZE, "2");
    // Traversal is expected to be aborted after 2nd error.
    setConfig("1", DefaultAclChoices.PUBLIC, p);
    FullTraversalConnector connector =
        new FullTraversalConnector(repositoryMock, checkpointHandlerMock);
    ApiOperation goodOperation = successOperation;
    ApiOperation badOperation = new ApiOperation() {
        @Override
        public List<GenericJson> execute(IndexingService service) throws IOException {
          throw new IOException("error processing test item");
          }
        };
    ApiOperation dontExecute = spy(new ApiOperation() {
        @Override
        public List<GenericJson> execute(IndexingService service) throws IOException {
          return null;
        }
      });
    ImmutableList<ApiOperation> allDocs =
        ImmutableList.of(
            goodOperation, badOperation, badOperation, goodOperation, dontExecute, dontExecute);
    connector.init(connectorContextMock);
    when(repositoryMock.getAllDocs(null))
        .thenReturn(new CheckpointCloseableIterableImpl.Builder<>(allDocs).build());
    try {
      connector.traverse();
      fail("should have thrown IOException");
    } catch (IOException e) {
      assertThat(e.getMessage(), containsString("error processing test item"));
    }
    verifyNoMoreInteractions(dontExecute);
  }
}
