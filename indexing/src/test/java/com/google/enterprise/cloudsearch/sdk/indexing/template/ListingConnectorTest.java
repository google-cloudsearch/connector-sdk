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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.json.GenericJson;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.CloseableIterable;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl.InheritanceType;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingConnectorContext;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc.Builder;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
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
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link ListingConnector}. */
@RunWith(MockitoJUnitRunner.class)
public class ListingConnectorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  private static final ItemAcl DOMAIN_PUBLIC_ACL =
      new Acl.Builder()
          .setReaders(ImmutableList.of(Acl.getCustomerPrincipal()))
          .build()
          .applyTo(new Item())
          .getAcl();

  @Mock private IndexingService mockIndexingService;
  @Mock private Repository mockRepository;
  @Mock private IndexingConnectorContext mockConnectorContext;
  @Mock private CheckpointHandler mockCheckpointHandler;
  @Captor private ArgumentCaptor<Item> itemListCaptor;
  @Captor private ArgumentCaptor<RepositoryContext> repositoryContextCaptor;
  // Mocks don't call default interface methods, so use an implementation here, wrapped
  // with spy() to allow for verify calls.
  @Spy private ApiOperation errorOperation = new ApiOperation() {
      @Override
      public List<GenericJson> execute(IndexingService service) throws IOException {
        throw new IOException();
      }
    };
  private ListenableFuture<Operation> completedOperationFuture =
      Futures.immediateFuture(new Operation().setDone(true));

  @Before
  public void init() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(mockConnectorContext.getIndexingService()).thenReturn(mockIndexingService);
  }

  class TestCloseableIterable implements CloseableIterable<ApiOperation> {

    final Collection<ApiOperation> operations;
    boolean closed = false;

    TestCloseableIterable(Collection<ApiOperation> operations) {
      this.operations = operations;
    }

    boolean isClosed() {
      return this.closed;
    }

    @Override
    public void close() {
      this.closed = true;
    }

    @Override
    public Iterator<ApiOperation> iterator() {
      return this.operations.iterator();
    }
  }

  @Test
  public void testNullRepository() {
    thrown.expect(NullPointerException.class);
    new ListingConnector(null);
  }

  @Test
  public void testConstructor() {
    new ListingConnector(mockRepository);
  }

  @Test
  public void testDefaultId() throws Exception {
    ListingConnector connector = new ListingConnector(mockRepository);
    assertEquals(mockRepository.getClass().getName(), connector.getDefaultId());
  }

  @Test
  public void testEventBusIsInitialized() throws Exception {
    setDefaultConfig();
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    verify(mockRepository).init(repositoryContextCaptor.capture());
    RepositoryContext rc = repositoryContextCaptor.getAllValues().get(0);
    // This will throw an IllegalArgumentException if the connector wasn't registered.
    rc.getEventBus().unregister(connector);
  }

  @Test
  public void testStartAndShutDown() throws Exception {
    setDefaultConfig();
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    verify(mockRepository).init(any());
    connector.destroy();
    verify(mockRepository).close();
  }

  @Test
  public void testConfigurationIsUninitialized() throws Exception {
    ListingConnector connector = new ListingConnector(mockRepository);
    try {
      connector.init(mockConnectorContext);
      fail();
    } catch (IllegalStateException ex) {
      verifyNoMoreInteractions(mockRepository);
    }
  }

  @Test
  public void testDefaultAclModeIsInitialized() throws Exception {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "override");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "google:user1@example.com");
    overrideDefaultConfig(config);
    SettableFuture<Operation> updateFuture = SettableFuture.create();
    updateFuture.set(new Operation().setDone(true));
    when(mockIndexingService.indexItem(any(), any())).thenReturn(updateFuture);
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    verify(mockRepository).init(repositoryContextCaptor.capture());
    RepositoryContext rc = repositoryContextCaptor.getAllValues().get(0);
    assertEquals(DefaultAclMode.OVERRIDE, rc.getDefaultAclMode());
  }

  @Test
  public void testTraverseConfiguration() throws Exception {
    Properties properties = new Properties();
    properties.put(ListingConnector.CONFIG_TRAVERSER, "checkpoint, test");
    overrideDefaultConfig(properties);
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    verify(mockConnectorContext, times(2)).registerTraverser(any());
  }

  @Test
  public void testTraverseConfigurationDefault() throws Exception {
    overrideDefaultConfig(new Properties());
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    verify(mockConnectorContext, times(1)).registerTraverser(any());
  }

  @Test
  public void testTraverse() throws Exception {
    setDefaultConfig();
    PushItem pushItem = new PushItem();
    ApiOperation pushOperation = new PushItems.Builder().addPushItem("pushedId", pushItem).build();
    ApiOperation deleteOperation = ApiOperations.deleteItem("deletedItem");
    Collection<ApiOperation> operations = Arrays.asList(pushOperation, deleteOperation);
    TestCloseableIterable delegate = new TestCloseableIterable(operations);
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate).build();
    when(mockRepository.getIds(any())).thenReturn(testIterable);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    SettableFuture<Item> pushFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              pushFuture.set(new Item());
              return pushFuture;
            })
        .when(mockIndexingService)
        .push(eq("pushedId"), eq(pushItem));
    connector.traverse();
    InOrder inOrder = Mockito.inOrder(mockIndexingService);
    inOrder.verify(mockIndexingService).push("pushedId", pushItem);
    inOrder.verify(mockIndexingService).deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    assertTrue(delegate.isClosed());
  }

  @Test
  public void testTraverseBatches() throws Exception {
    setDefaultConfig();
    PushItem pushItem = new PushItem();
    ApiOperation pushOperation = new PushItems.Builder().addPushItem("pushedId", pushItem).build();
    ApiOperation deleteOperation = ApiOperations.deleteItem("deletedItem");
    Collection<ApiOperation> operations = Arrays.asList(pushOperation, deleteOperation);
    TestCloseableIterable delegate1 = new TestCloseableIterable(operations);
    TestCloseableIterable delegate2 = new TestCloseableIterable(Collections.emptyList());
    byte[] batchCheckpoint = "someCheckpoint".getBytes();
    when(mockCheckpointHandler.readCheckpoint(ListingConnector.CHECKPOINT_FULL))
        .thenReturn(null, batchCheckpoint);
    CheckpointCloseableIterable<ApiOperation> testIterable1 =
        new CheckpointCloseableIterableImpl.Builder<>(delegate1)
            .setCheckpoint(batchCheckpoint)
            .setHasMore(true)
            .build();
    when(mockRepository.getIds(null)).thenReturn(testIterable1);
    CheckpointCloseableIterable<ApiOperation> testIterable2 =
        new CheckpointCloseableIterableImpl.Builder<>(delegate2).build();
    when(mockRepository.getIds(batchCheckpoint)).thenReturn(testIterable2);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    SettableFuture<Item> pushFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              pushFuture.set(new Item());
              return pushFuture;
            })
        .when(mockIndexingService)
        .push(eq("pushedId"), eq(pushItem));
    connector.traverse();
    InOrder inOrder = Mockito.inOrder(mockIndexingService, mockCheckpointHandler);
    inOrder.verify(mockCheckpointHandler).readCheckpoint(ListingConnector.CHECKPOINT_FULL);
    inOrder.verify(mockIndexingService).push("pushedId", pushItem);
    inOrder.verify(mockIndexingService).deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    inOrder
        .verify(mockCheckpointHandler)
        .saveCheckpoint(ListingConnector.CHECKPOINT_FULL, batchCheckpoint);
    inOrder.verify(mockCheckpointHandler).saveCheckpoint(ListingConnector.CHECKPOINT_FULL, null);
    assertTrue(delegate1.isClosed());
    assertTrue(delegate2.isClosed());
  }

  @Test
  public void testIncrementalChangesBatches() throws Exception {
    setDefaultConfig();
    PushItem pushItem = new PushItem();
    ApiOperation pushOperation = new PushItems.Builder().addPushItem("pushedId", pushItem).build();
    ApiOperation deleteOperation = ApiOperations.deleteItem("deletedItem");
    Collection<ApiOperation> operations = Arrays.asList(pushOperation, deleteOperation);
    TestCloseableIterable delegate1 = new TestCloseableIterable(operations);
    TestCloseableIterable delegate2 = new TestCloseableIterable(Collections.emptyList());
    byte[] batchCheckpoint = "someCheckpoint".getBytes();
    when(mockCheckpointHandler.readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL))
        .thenReturn(null, batchCheckpoint);
    CheckpointCloseableIterable<ApiOperation> testIterable1 =
        new CheckpointCloseableIterableImpl.Builder<>(delegate1)
            .setCheckpoint(batchCheckpoint)
            .setHasMore(true)
            .build();
    when(mockRepository.getChanges(null)).thenReturn(testIterable1);
    CheckpointCloseableIterable<ApiOperation> testIterable2 =
        new CheckpointCloseableIterableImpl.Builder<>(delegate2).build();
    when(mockRepository.getChanges(batchCheckpoint)).thenReturn(testIterable2);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    SettableFuture<Item> pushFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              pushFuture.set(new Item());
              return pushFuture;
            })
        .when(mockIndexingService)
        .push(eq("pushedId"), eq(pushItem));
    connector.handleIncrementalChanges();
    InOrder inOrder = Mockito.inOrder(mockIndexingService, mockCheckpointHandler);
    inOrder.verify(mockCheckpointHandler).readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL);
    inOrder.verify(mockIndexingService).push("pushedId", pushItem);
    inOrder.verify(mockIndexingService).deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    inOrder
        .verify(mockCheckpointHandler)
        .saveCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL, batchCheckpoint);
    inOrder
        .verify(mockCheckpointHandler)
        .saveCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL, null);
    assertTrue(delegate1.isClosed());
    assertTrue(delegate2.isClosed());
  }

  @Test
  public void testHandleAsyncOperation() throws Exception {
    setDefaultConfig();

    ApiOperation mockPush = errorOperation;
    AsyncApiOperation pushAsyncOperation = new AsyncApiOperation(mockPush);
    ApiOperation mockDelete = spy(new ApiOperation() {
        @Override
        public List<GenericJson> execute(IndexingService service) throws IOException {
          return Lists.newArrayList();
        }
      });
    AsyncApiOperation deleteAsyncOperation = new AsyncApiOperation(mockDelete);
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    connector.handleAsyncOperation(pushAsyncOperation);
    connector.handleAsyncOperation(deleteAsyncOperation);
    assertNotNull(deleteAsyncOperation.getResult().get());
    connector.destroy();

    verify(mockPush).execute(eq(mockIndexingService), any());
    verify(mockPush).execute(mockIndexingService);
    verify(mockDelete).execute(eq(mockIndexingService), any());
    verify(mockDelete).execute(mockIndexingService);
    verifyNoMoreInteractions(mockIndexingService);
    thrown.expect(ExecutionException.class);
    pushAsyncOperation.getResult().get();
  }

  @Test
  public void testTraverseAbortOnError() throws Exception {
    setDefaultConfig();
    PushItem pushItem = new PushItem();
    ApiOperation pushOperation = new PushItems.Builder().addPushItem("pushedId", pushItem).build();
    ApiOperation deleteOperation = ApiOperations.deleteItem("deletedItem");
    Collection<ApiOperation> operations =
        Arrays.asList(pushOperation, errorOperation, deleteOperation);
    TestCloseableIterable delegate = new TestCloseableIterable(operations);
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate).build();
    when(mockRepository.getIds(any())).thenReturn(testIterable);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    SettableFuture<Item> pushFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              pushFuture.set(new Item());
              return pushFuture;
            })
        .when(mockIndexingService)
        .push(eq("pushedId"), eq(pushItem));
    try {
      connector.traverse();
      fail("missing IOException");
    } catch (IOException expected) {
    }
    InOrder inOrder = Mockito.inOrder(mockIndexingService);
    inOrder.verify(mockIndexingService).push("pushedId", pushItem);
    assertTrue(delegate.isClosed());
  }

  @Test
  public void testTraverseIgnoreError() throws Exception {
    Properties properties = new Properties();
    properties.put(TraverseExceptionHandlerFactory.TRAVERSE_EXCEPTION_HANDLER, "ignore");
    overrideDefaultConfig(properties);
    PushItem pushItem = new PushItem();
    ApiOperation pushOperation = new PushItems.Builder().addPushItem("pushedId", pushItem).build();
    ApiOperation deleteOperation = ApiOperations.deleteItem("deletedItem");
    Collection<ApiOperation> operations =
        Arrays.asList(pushOperation, errorOperation, deleteOperation);
    TestCloseableIterable delegate = new TestCloseableIterable(operations);
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate).build();
    when(mockRepository.getIds(any())).thenReturn(testIterable);
    SettableFuture<Item> pushFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              pushFuture.set(new Item());
              return pushFuture;
            })
        .when(mockIndexingService)
        .push(eq("pushedId"), eq(pushItem));
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    connector.traverse();
    InOrder inOrder = Mockito.inOrder(mockIndexingService);
    inOrder.verify(mockIndexingService).push("pushedId", pushItem);
    inOrder.verify(mockIndexingService).deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    assertTrue(delegate.isClosed());
  }

  @Test
  public void testTraverseIgnoreThreeErrors() throws Exception {
    Properties properties = new Properties();
    properties.put(TraverseExceptionHandlerFactory.TRAVERSE_EXCEPTION_HANDLER, "3");
    overrideDefaultConfig(properties);
    PushItem pushItem = new PushItem();
    ApiOperation pushOperation = new PushItems.Builder().addPushItem("pushedId", pushItem).build();
    ApiOperation deleteOperation = ApiOperations.deleteItem("deletedItem");
    Collection<ApiOperation> operations =
        Arrays.asList(
            pushOperation, errorOperation, errorOperation, errorOperation, deleteOperation);
    TestCloseableIterable delegate = new TestCloseableIterable(operations);
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate).build();
    SettableFuture<Item> pushFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              pushFuture.set(new Item());
              return pushFuture;
            })
        .when(mockIndexingService)
        .push(eq("pushedId"), eq(pushItem));
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    when(mockRepository.getIds(any())).thenReturn(testIterable);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    connector.traverse();
    verify(mockIndexingService).push("pushedId", pushItem);
    verify(errorOperation, times(3)).execute(mockIndexingService);
    verify(mockIndexingService).deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    assertTrue(delegate.isClosed());
  }

  @Test
  public void testTraverseAbortAfterThreeErrors() throws Exception {
    Properties properties = new Properties();
    properties.put(TraverseExceptionHandlerFactory.TRAVERSE_EXCEPTION_HANDLER, "3");
    overrideDefaultConfig(properties);
    PushItem pushItem = new PushItem();
    ApiOperation pushOperation = new PushItems.Builder().addPushItem("pushedId", pushItem).build();
    ApiOperation deleteOperation = ApiOperations.deleteItem("deletedItem");
    SettableFuture<Item> pushFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              pushFuture.set(new Item());
              return pushFuture;
            })
        .when(mockIndexingService)
        .push(eq("pushedId"), eq(pushItem));
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deletedItem", null, RequestMode.UNSPECIFIED);
    Collection<ApiOperation> operations =
        Arrays.asList(
            pushOperation,
            errorOperation,
            errorOperation,
            deleteOperation,
            errorOperation,
            errorOperation,
            deleteOperation);
    TestCloseableIterable delegate = new TestCloseableIterable(operations);
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate).build();
    when(mockRepository.getIds(any())).thenReturn(testIterable);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    try {
      connector.traverse();
      fail("missing IOException");
    } catch (IOException expected) {
      // Expected exception
    }
    assertTrue(delegate.isClosed());
  }

  @Test
  public void testGetDoc() throws Exception {
    setDefaultConfig();
    Item polledItem = new Item().setName("deleteThis");
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deleteThis", null, RequestMode.UNSPECIFIED);
    when(mockRepository.getDoc(polledItem)).thenReturn(ApiOperations.deleteItem("deleteThis"));
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    connector.process(polledItem);
    verify(mockIndexingService).deleteItem("deleteThis", null, RequestMode.UNSPECIFIED);
  }

  @Test
  public void testGetDocDefaultAclOverride() throws Exception {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "override");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    overrideDefaultConfig(config);
    Item polledItem = new Item().setName("PollItem");
    RepositoryDoc.Builder repositoryDoc = new Builder();
    repositoryDoc
        .setItem(
            new IndexingItemBuilder("PollItem")
                .setTitle(FieldOrValue.withValue("testItem"))
                .setAcl(
                    new Acl.Builder()
                        .setReaders(Arrays.asList(Acl.getUserPrincipal("user1")))
                        .build())
                .build())
        .build();
    SettableFuture<Operation> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Operation().setDone(true));
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(any(), any());
    when(mockRepository.getDoc(polledItem)).thenReturn(repositoryDoc.build());

    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    connector.process(polledItem);
    verify(mockIndexingService).indexItem(itemListCaptor.capture(), any());
    assertEquals(DOMAIN_PUBLIC_ACL, itemListCaptor.getAllValues().get(0).getAcl());
  }

  @Test
  public void testGetDocDefaultAclNonPublicOverride() throws Exception {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "override");
    config.put(DefaultAcl.DEFAULT_ACL_READERS_USERS, "google:user1-override@example.com");
    overrideDefaultConfig(config);
    Item polledItem = new Item().setName("PollItem");
    RepositoryDoc.Builder repositoryDoc = new Builder();
    repositoryDoc
        .setItem(
            new IndexingItemBuilder("PollItem")
                .setTitle(FieldOrValue.withValue("testItem"))
                .setAcl(
                    new Acl.Builder()
                        .setReaders(Arrays.asList(Acl.getUserPrincipal("user-other")))
                        .build())
                .build())
        .build();
    SettableFuture<Operation> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Operation().setDone(true));
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(any(), any());
    when(mockRepository.getDoc(polledItem)).thenReturn(repositoryDoc.build());

    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    connector.process(polledItem);
    verify(mockIndexingService, times(2)).indexItem(itemListCaptor.capture(), any());
    ItemAcl expectedInheritedAcl = new Acl.Builder()
        .setInheritFrom(DefaultAcl.DEFAULT_ACL_NAME_DEFAULT)
        .setInheritanceType(InheritanceType.PARENT_OVERRIDE)
        .build()
        .applyTo(new Item())
        .getAcl();
    assertEquals(
        expectedInheritedAcl,
        itemListCaptor.getAllValues().get(1).getAcl());
  }

  @Test
  public void testGetDocDefaultAclFallBack() throws Exception {
    Properties config = new Properties();
    config.put(DefaultAcl.DEFAULT_ACL_MODE, "fallback");
    config.put(DefaultAcl.DEFAULT_ACL_PUBLIC, "true");
    overrideDefaultConfig(config);
    Item polledItem = new Item().setName("PollItem");
    Item repositoryItem =
        new IndexingItemBuilder("PollItem")
            .setTitle(FieldOrValue.withValue("testItem"))
            .setAcl(
                new Acl.Builder().setReaders(Arrays.asList(Acl.getUserPrincipal("user1"))).build())
            .build();
    RepositoryDoc repositoryDoc = new Builder().setItem(repositoryItem).build();
    ItemAcl originalAcl = repositoryItem.getAcl().clone();

    SettableFuture<Operation> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Operation().setDone(true));
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(any(), any());
    when(mockRepository.getDoc(polledItem)).thenReturn(repositoryDoc);
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    connector.process(polledItem);
    verify(mockIndexingService).indexItem(itemListCaptor.capture(), any());
    Item actual = itemListCaptor.getAllValues().get(0);
    // with fallback, original acl shouldn't have changed
    assertEquals(originalAcl, actual.getAcl());
  }

  @Test
  public void testGetDocRepositoryDoc() throws Exception {
    setDefaultConfig();
    Item polledItem = new Item().setName("docId");
    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(polledItem, RequestMode.UNSPECIFIED);
    when(mockRepository.getDoc(polledItem))
        .thenReturn(new RepositoryDoc.Builder().setItem(polledItem).build());
    ListingConnector connector = new ListingConnector(mockRepository);
    connector.init(mockConnectorContext);
    connector.process(polledItem);
    verify(mockIndexingService).indexItem(polledItem, RequestMode.UNSPECIFIED);
  }

  @Test
  public void testClose() throws Exception {
    setupConfig.initConfig(new Properties());
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    connector.destroy();
    verify(mockRepository).close();
    verify(mockRepository).init(repositoryContextCaptor.capture());
    RepositoryContext rc = repositoryContextCaptor.getAllValues().get(0);
    // This will throw an IllegalArgumentException if the connector is no longer registered.
    thrown.expect(IllegalArgumentException.class);
    rc.getEventBus().unregister(connector);
  }

  @Test
  public void testIncrementalChangesNoCheckpoint() throws Exception {
    setDefaultConfig();
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    when(mockCheckpointHandler.readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL))
        .thenReturn(null);
    TestCloseableIterable delegate = new TestCloseableIterable(Collections.emptyList());
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate)
            .setCheckpoint("checkpoint".getBytes())
            .build();
    when(mockRepository.getChanges(null)).thenReturn(testIterable);
    connector.handleIncrementalChanges();
    verify(mockCheckpointHandler)
        .saveCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL, "checkpoint".getBytes());
  }

  @Test
  public void testIncrementalChangesExistingCheckpoint() throws Exception {
    Properties config = new Properties();
    config.put("repository.checkpointId", "CUSTOM_CHECKPOINT_ID");
    overrideDefaultConfig(config);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    when(mockCheckpointHandler.readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL))
        .thenReturn("existing".getBytes());
    ApiOperation change = ApiOperations.deleteItem("deleted");
    TestCloseableIterable delegate = new TestCloseableIterable(Collections.singletonList(change));
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate)
            .setCheckpoint("newCheckpoint".getBytes())
            .build();
    when(mockRepository.getChanges("existing".getBytes())).thenReturn(testIterable);
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deleted", null, RequestMode.UNSPECIFIED);
    connector.handleIncrementalChanges();
    InOrder inOrder = Mockito.inOrder(mockIndexingService, mockCheckpointHandler);
    inOrder
        .verify(mockCheckpointHandler)
        .readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL);
    inOrder.verify(mockIndexingService).deleteItem("deleted", null, RequestMode.UNSPECIFIED);
    inOrder
        .verify(mockCheckpointHandler)
        .saveCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL, "newCheckpoint".getBytes());
  }

  @Test
  public void testSkipCheckpointPushOnError() throws Exception {
    Properties config = new Properties();
    overrideDefaultConfig(config);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    when(mockCheckpointHandler.readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL))
        .thenReturn("existing".getBytes());
    ApiOperation change1 = ApiOperations.deleteItem("deleted1");
    ApiOperation change2 = ApiOperations.deleteItem("deleted2");
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem(anyString(), any(), any());
    TestCloseableIterable delegate =
        new TestCloseableIterable(Arrays.asList(change1, errorOperation, change2));
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate)
            .setCheckpoint("newChec".getBytes())
            .build();

    when(mockRepository.getChanges("existing".getBytes())).thenReturn(testIterable);
    try {
      connector.handleIncrementalChanges();
      fail("Exception missing");
    } catch (IOException expected) {
      // Expected exception.
    }
    verify(mockCheckpointHandler).readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL);
    verifyNoMoreInteractions(mockCheckpointHandler);
  }

  @Test
  public void testIncrementalChangesEmptyNewCheckpoint() throws Exception {
    Properties config = new Properties();
    config.put("repository.checkpointId", "CUSTOM_CHECKPOINT_ID");
    overrideDefaultConfig(config);
    ListingConnector connector = new ListingConnector(mockRepository, mockCheckpointHandler);
    connector.init(mockConnectorContext);
    when(mockCheckpointHandler.readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL))
        .thenReturn("existing".getBytes());
    ApiOperation change = ApiOperations.deleteItem("deleted");
    SettableFuture<Operation> deleteFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              deleteFuture.set(new Operation());
              return deleteFuture;
            })
        .when(mockIndexingService)
        .deleteItem("deleted", null, RequestMode.UNSPECIFIED);
    TestCloseableIterable delegate = new TestCloseableIterable(Collections.singletonList(change));
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(delegate).build();
    when(mockRepository.getChanges("existing".getBytes())).thenReturn(testIterable);
    connector.handleIncrementalChanges();
    InOrder inOrder = Mockito.inOrder(mockIndexingService, mockCheckpointHandler);
    inOrder
        .verify(mockCheckpointHandler)
        .readCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL);
    inOrder.verify(mockIndexingService).deleteItem("deleted", null, RequestMode.UNSPECIFIED);
    inOrder
        .verify(mockCheckpointHandler)
        .saveCheckpoint(ListingConnector.CHECKPOINT_INCREMENTAL, null);
  }

  @Test
  public void testModifyApiOperationProcess() throws Exception {
    Item polledItem = new Item().setName("PollItem");
    RepositoryDoc repositoryDoc = new Builder()
        .setItem(
            new IndexingItemBuilder("PollItem")
            .setTitle(FieldOrValue.withValue("testItem"))
            .build())
        .build();
    when(mockIndexingService.indexItem(any(), any())).thenReturn(completedOperationFuture);
    when(mockRepository.getDoc(polledItem)).thenReturn(repositoryDoc);

    setDefaultConfig();
    ListingConnector connector = new ListingConnector(mockRepository) {
        @Override
        void modifyApiOperation(ApiOperation apiOperation) {
          if (apiOperation instanceof RepositoryDoc) {
            ((RepositoryDoc) apiOperation).getItem().getMetadata().setTitle("Modified Title");
          }
        }
      };
    connector.init(mockConnectorContext);
    connector.process(polledItem);
    verify(mockIndexingService).indexItem(itemListCaptor.capture(), any());
    ItemMetadata metadata = itemListCaptor.getAllValues().get(0).getMetadata();
    assertEquals("Modified Title", metadata.getTitle());
  }

  @Test
  public void testModifyApiOperationAsync() throws Exception {
    RepositoryDoc repositoryDoc = new Builder()
        .setItem(
            new IndexingItemBuilder("PollItem")
            .setTitle(FieldOrValue.withValue("testItem"))
            .build())
        .build();
    AsyncApiOperation asyncOperation = new AsyncApiOperation(repositoryDoc);
    when(mockIndexingService.indexItem(any(), any())).thenReturn(completedOperationFuture);

    setDefaultConfig();
    ListingConnector connector = new ListingConnector(mockRepository) {
        @Override
        void modifyApiOperation(ApiOperation apiOperation) {
          if (apiOperation instanceof RepositoryDoc) {
            ((RepositoryDoc) apiOperation).getItem().getMetadata().setTitle("Modified Title");
          }
        }
      };
    connector.init(mockConnectorContext);
    connector.handleAsyncOperation(asyncOperation);
    asyncOperation.getResult().get();
    verify(mockIndexingService).indexItem(itemListCaptor.capture(), any());
    ItemMetadata metadata = itemListCaptor.getAllValues().get(0).getMetadata();
    assertEquals("Modified Title", metadata.getTitle());
  }

  @Test
  public void testModifyApiOperationIncremental() throws Exception {
    RepositoryDoc repositoryDoc = new Builder()
        .setItem(
            new IndexingItemBuilder("PollItem")
            .setTitle(FieldOrValue.withValue("testItem"))
            .build())
        .build();
    when(mockIndexingService.indexItem(any(), any())).thenReturn(completedOperationFuture);
    CheckpointCloseableIterable<ApiOperation> testIterable =
        new CheckpointCloseableIterableImpl.Builder<>(
            new TestCloseableIterable(Arrays.asList(repositoryDoc))).build();
    when(mockRepository.getChanges(any())).thenReturn(testIterable);

    setDefaultConfig();
    ListingConnector connector = new ListingConnector(mockRepository) {
        @Override
        void modifyApiOperation(ApiOperation apiOperation) {
          if (apiOperation instanceof RepositoryDoc) {
            ((RepositoryDoc) apiOperation).getItem().getMetadata().setTitle("Modified Title");
          }
        }
      };
    connector.init(mockConnectorContext);
    connector.handleIncrementalChanges();
    verify(mockIndexingService).indexItem(itemListCaptor.capture(), any());
    ItemMetadata metadata = itemListCaptor.getAllValues().get(0).getMetadata();
    assertEquals("Modified Title", metadata.getTitle());
  }

  private void setDefaultConfig() {
    Properties config = new Properties();
    overrideDefaultConfig(config);
  }

  private void overrideDefaultConfig(Properties properties) {
    setupConfig.initConfig(properties);
  }
}
