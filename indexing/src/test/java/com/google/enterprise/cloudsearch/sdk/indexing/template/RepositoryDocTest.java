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

import static java.net.HttpURLConnection.HTTP_BAD_GATEWAY;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.AbstractInputStreamContent;
import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link RepositoryDoc}. */
@RunWith(MockitoJUnitRunner.class)
public class RepositoryDocTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock IndexingService mockIndexingService;

  @Test
  public void testNullItem() {
    thrown.expect(NullPointerException.class);
    new RepositoryDoc.Builder().setItem(null).build();
  }

  @Test
  public void testOnlyItem() throws IOException, InterruptedException {
    Item item = new Item().setName("id1").setAcl(getCustomerAcl());
    RepositoryDoc doc = new RepositoryDoc.Builder().setItem(item).build();
    SettableFuture<Operation> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Operation());
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(item, RequestMode.UNSPECIFIED);
    doc.execute(mockIndexingService);
    InOrder inOrder = inOrder(mockIndexingService);
    inOrder.verify(mockIndexingService).indexItem(item, RequestMode.UNSPECIFIED);
    assertEquals("id1", doc.getItem().getName());
  }

  @Test(expected = IOException.class)
  public void execute_indexItemNotFound_notPushedToQueue_throwsIOException() throws Exception {
    Item item = new Item().setName("id1").setAcl(getCustomerAcl());
    RepositoryDoc doc = new RepositoryDoc.Builder().setItem(item).build();
    doAnswer(
            invocation -> {
              SettableFuture<Operation> updateFuture = SettableFuture.create();
              updateFuture.setException(
                  new GoogleJsonResponseException(
                      new HttpResponseException.Builder(
                          HTTP_NOT_FOUND, "not found", new HttpHeaders()),
                      new GoogleJsonError()));
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(item, RequestMode.UNSPECIFIED);
    try {
      doc.execute(mockIndexingService);
    } finally {
      InOrder inOrder = inOrder(mockIndexingService);
      inOrder.verify(mockIndexingService).indexItem(item, RequestMode.UNSPECIFIED);
      inOrder.verifyNoMoreInteractions();
      assertEquals("id1", doc.getItem().getName());
    }
  }

  @Test(expected = IOException.class)
  public void execute_indexFailed_pushedToQueue_throwsIOException() throws Exception {
    Item item =
        new Item().setName("id1").setQueue("Q1").setPayload("1234").setAcl(getCustomerAcl());
    RepositoryDoc doc = new RepositoryDoc.Builder().setItem(item).build();
    doAnswer(
            invocation -> {
              SettableFuture<Operation> updateFuture = SettableFuture.create();
              updateFuture.setException(
                  new GoogleJsonResponseException(
                      new HttpResponseException.Builder(
                          HTTP_BAD_GATEWAY, "bad gateway", new HttpHeaders()),
                      new GoogleJsonError()));
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(item, RequestMode.UNSPECIFIED);

    when(mockIndexingService.push(anyString(), any()))
        .thenReturn(Futures.immediateFuture(new Item()));

    try {
      doc.execute(mockIndexingService);
    } finally {
      InOrder inOrder = inOrder(mockIndexingService);
      inOrder.verify(mockIndexingService).indexItem(item, RequestMode.UNSPECIFIED);
      ArgumentCaptor<PushItem> pushItemArgumentCaptor = ArgumentCaptor.forClass(PushItem.class);
      inOrder
          .verify(mockIndexingService)
          .push(eq(item.getName()), pushItemArgumentCaptor.capture());
      PushItem pushItem = pushItemArgumentCaptor.getValue();
      assertEquals("Q1", pushItem.getQueue());
      assertEquals("SERVER_ERROR", pushItem.getRepositoryError().getType());
      assertEquals("1234", pushItem.getPayload());
      assertEquals("id1", doc.getItem().getName());
    }
  }

  @Test
  public void testItemAndContent() throws IOException, InterruptedException {
    Item item = new Item().setName("id1").setAcl(getCustomerAcl());
    AbstractInputStreamContent content = ByteArrayContent.fromString("", "golden");
    RepositoryDoc doc =
        new RepositoryDoc.Builder().setItem(item).setContent(content, ContentFormat.TEXT).build();
    SettableFuture<Item> updateFuture = SettableFuture.create();

    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItemAndContent(
            any(), any(), any(), eq(ContentFormat.TEXT), eq(RequestMode.UNSPECIFIED));
    doc.execute(mockIndexingService);

    InOrder inOrder = inOrder(mockIndexingService);
    inOrder
        .verify(mockIndexingService)
        .indexItemAndContent(item, content, null, ContentFormat.TEXT, RequestMode.UNSPECIFIED);
    assertEquals("id1", doc.getItem().getName());
    assertEquals(content, doc.getContent());
  }

  @Test
  public void testItemAndContentSynchronous() throws IOException, InterruptedException {
    Item item = new Item().setName("id1").setAcl(getCustomerAcl());
    AbstractInputStreamContent content = ByteArrayContent.fromString("", "golden");
    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(item)
            .setContent(content, ContentFormat.TEXT)
            .setRequestMode(RequestMode.SYNCHRONOUS)
            .build();
    SettableFuture<Item> updateFuture = SettableFuture.create();

    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItemAndContent(
            any(), any(), any(), eq(ContentFormat.TEXT), eq(RequestMode.SYNCHRONOUS));
    doc.execute(mockIndexingService);

    InOrder inOrder = inOrder(mockIndexingService);
    inOrder
        .verify(mockIndexingService)
        .indexItemAndContent(item, content, null, ContentFormat.TEXT, RequestMode.SYNCHRONOUS);
    assertEquals("id1", doc.getItem().getName());
    assertEquals(content, doc.getContent());
  }

  @Test
  public void testItemAndContentNotIncrement() throws IOException, InterruptedException {
    Item item = new Item().setName("id1").setAcl(getCustomerAcl());
    AbstractInputStreamContent content = ByteArrayContent.fromString("", "golden");
    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(item)
            .setContent(content, ContentFormat.TEXT)
            .setRequestMode(RequestMode.ASYNCHRONOUS)
            .build();
    SettableFuture<Item> updateFuture = SettableFuture.create();

    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItemAndContent(
            any(), any(), any(), eq(ContentFormat.TEXT), eq(RequestMode.ASYNCHRONOUS));

    doc.execute(mockIndexingService);

    InOrder inOrder = inOrder(mockIndexingService);
    inOrder
        .verify(mockIndexingService)
        .indexItemAndContent(item, content, null, ContentFormat.TEXT, RequestMode.ASYNCHRONOUS);
  }

  @Test
  public void testItemContentAndChildLinks() throws IOException, InterruptedException {
    Item item = new Item().setName("id1").setAcl(getCustomerAcl());
    AbstractInputStreamContent content = ByteArrayContent.fromString("", "golden");
    PushItem pushItem1 = new PushItem().setQueue("queue1");
    PushItem pushItem2 = new PushItem().setQueue("queue1");
    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(item)
            .setContent(content, ContentFormat.TEXT)
            .addChildId("id1", pushItem1)
            .addChildId("id2+b;more", pushItem2)
            .build();

    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItemAndContent(
            any(), any(), any(), eq(ContentFormat.TEXT), eq(RequestMode.UNSPECIFIED));

    SettableFuture<Item> pushFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              pushFuture.set(new Item());
              return pushFuture;
            })
        .when(mockIndexingService)
        .push(any(), any());
    doc.execute(mockIndexingService);
    verify(mockIndexingService)
        .indexItemAndContent(item, content, null, ContentFormat.TEXT, RequestMode.UNSPECIFIED);
    verify(mockIndexingService).push("id1", pushItem1);
    verify(mockIndexingService).push("id2+b;more", pushItem2);
  }

  @Test
  public void testItemAndFragment() throws IOException, InterruptedException {
    Item item = new Item().setName("id1").setAcl(getCustomerAcl());
    Acl fragmentAcl = new Acl.Builder().setReaders(getCustomerAcl().getReaders()).build();
    Map<String, Acl> fragments =
        new ImmutableMap.Builder<String, Acl>().put("admin", fragmentAcl).build();
    RepositoryDoc doc =
        new RepositoryDoc.Builder().setItem(item).setAclFragments(fragments).build();
    Item expectedFragment =
        new Item()
            .setName(Acl.fragmentId("id1", "admin"))
            .setItemType(ItemType.VIRTUAL_CONTAINER_ITEM.name())
            .setAcl(
                getCustomerAcl()
                    .setDeniedReaders(Collections.emptyList())
                    .setOwners(Collections.emptyList()));
    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(any(), eq(RequestMode.UNSPECIFIED));
    doc.execute(mockIndexingService);
    InOrder inOrder = inOrder(mockIndexingService);
    inOrder.verify(mockIndexingService).indexItem(item, RequestMode.UNSPECIFIED);
    inOrder.verify(mockIndexingService).indexItem(expectedFragment, RequestMode.UNSPECIFIED);
  }

  @Test
  public void testItemAndFragmentNotIncremental() throws IOException, InterruptedException {
    Item item = new Item().setName("id1").setAcl(getCustomerAcl());
    Acl fragmentAcl = new Acl.Builder().setReaders(getCustomerAcl().getReaders()).build();
    Map<String, Acl> fragments =
        new ImmutableMap.Builder<String, Acl>().put("admin", fragmentAcl).build();
    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(item)
            .setAclFragments(fragments)
            .setRequestMode(RequestMode.ASYNCHRONOUS)
            .build();
    Item expectedFragment =
        new Item()
            .setName(Acl.fragmentId("id1", "admin"))
            .setItemType(ItemType.VIRTUAL_CONTAINER_ITEM.name())
            .setAcl(
                getCustomerAcl()
                    .setDeniedReaders(Collections.emptyList())
                    .setOwners(Collections.emptyList()));
    SettableFuture<Item> updateFuture = SettableFuture.create();
    doAnswer(
            invocation -> {
              updateFuture.set(new Item());
              return updateFuture;
            })
        .when(mockIndexingService)
        .indexItem(any(), eq(RequestMode.ASYNCHRONOUS));
    doc.execute(mockIndexingService);
    InOrder inOrder = inOrder(mockIndexingService);
    inOrder.verify(mockIndexingService).indexItem(item, RequestMode.ASYNCHRONOUS);
    inOrder.verify(mockIndexingService).indexItem(expectedFragment, RequestMode.ASYNCHRONOUS);
  }

  @Test
  public void testGetChildIds() {
    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("doc"))
            .addChildId("id1", new PushItem().setQueue("somewhere"))
            .addChildId(
                "id2",
                new PushItem().setRepositoryError(new RepositoryError().setErrorMessage("drat")))
            .build();
    Map<String, PushItem> childIds = doc.getChildIds();
    assertEquals(childIds, doc.getChildIds());
    childIds.get("id1").setQueue("foo");
    assertNotEquals(childIds, doc.getChildIds());
    thrown.expect(UnsupportedOperationException.class);
    childIds.put("id3", new PushItem());
  }

  @Test
  public void testGetChildIds_clonePushItem() {
    assertTrue(isCloneable(PushItem.class));

    // Confirm that the isCloneable method will detect an uncloneable class.
    assertFalse(isCloneable(ItemAcl.class));
  }

  /**
   * Check whether a GenericJson subclass is really cloneable. Most notably, GenericJson.clone fails
   * on immutable collections. This test is more conservative, allowing only Boolean, Number, and
   * String fields, and nested GenericJson objects.
   */
  // TODO(jlacey): This could be a Matcher, or otherwise return the offending class.
  private boolean isCloneable(Class<?> jsonClass) {
    if (!GenericJson.class.isAssignableFrom(jsonClass)) {
      return false;
    }
    Field[] fields = jsonClass.getDeclaredFields();
    for (Field field : fields) {
      Key key = field.getAnnotation(Key.class);
      if (key != null) {
        Class<?> clazz = field.getType();
        if (!Boolean.class.isAssignableFrom(clazz)
            && !Number.class.isAssignableFrom(clazz)
            && !String.class.isAssignableFrom(clazz)
            && !isCloneable(clazz)) {
          return false;
        }
      }
    }
    return true;
  }

  @Test
  public void testEqualsNegative() {
    RepositoryDoc doc1 = new RepositoryDoc.Builder().setItem(new Item()).build();
    assertEquals(doc1, doc1);
    assertThat(doc1, not(equalTo(null)));
    assertThat(doc1, not(equalTo(new RepositoryDoc.Builder())));
  }

  @Test
  public void testEqualsItemMismatch() {
    RepositoryDoc doc1 = new RepositoryDoc.Builder().setItem(new Item().setName("id1")).build();
    RepositoryDoc doc2 = new RepositoryDoc.Builder().setItem(new Item()).build();
    assertThat(doc1, not(equalTo(doc2)));
    assertThat(doc1.hashCode(), not(equalTo(doc2.hashCode())));
  }

  @Test
  public void testEqualsWithContent() {
    AbstractInputStreamContent content = ByteArrayContent.fromString(null, "golden");
    RepositoryDoc doc1 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("id1"))
            .setContent(content, ContentFormat.TEXT)
            .build();
    RepositoryDoc doc2 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("id1"))
            .setContent(content, ContentFormat.TEXT)
            .build();
    assertEquals(doc1, doc2);
    assertEquals(doc1.hashCode(), doc2.hashCode());
  }

  @Test
  public void testNotEqualsWithContent() {
    AbstractInputStreamContent content1 = ByteArrayContent.fromString(null, "golden");
    RepositoryDoc doc1 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("id1"))
            .setContent(content1, ContentFormat.TEXT)
            .build();
    AbstractInputStreamContent content2 = ByteArrayContent.fromString(null, "golden2");
    RepositoryDoc doc2 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("id1"))
            .setContent(content2, ContentFormat.TEXT)
            .build();
    assertNotEquals(doc1, doc2);
    assertNotEquals(doc1.hashCode(), doc2.hashCode());
  }

  @Test
  public void testEqualsWithContentHash() {
    AbstractInputStreamContent content = ByteArrayContent.fromString(null, "golden");
    RepositoryDoc doc1 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("id1"))
            .setContent(content, Integer.toString(Objects.hash(content)), ContentFormat.TEXT)
            .build();
    RepositoryDoc doc2 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("id1"))
            .setContent(content, Integer.toString(Objects.hash(content)), ContentFormat.TEXT)
            .build();
    assertEquals(doc1, doc2);
    assertEquals(doc1.hashCode(), doc2.hashCode());
  }

  @Test
  public void testNotEqualsWithContentHash() {
    AbstractInputStreamContent content = ByteArrayContent.fromString(null, "golden");
    RepositoryDoc doc1 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("id1"))
            .setContent(content, Integer.toString(Objects.hash(content)), ContentFormat.TEXT)
            .build();
    RepositoryDoc doc2 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("id1"))
            .setContent(content, ContentFormat.TEXT)
            .build();
    assertNotEquals(doc1, doc2);
    assertNotEquals(doc1.hashCode(), doc2.hashCode());
  }

  @Test
  public void testEqualsWithChildIds() {
    RepositoryDoc doc1 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("doc"))
            .addChildId("id1", new PushItem())
            .addChildId("id2", new PushItem())
            .build();
    RepositoryDoc doc2 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("doc"))
            .addChildId("id1", new PushItem())
            .addChildId("id2", new PushItem())
            .build();
    assertEquals(doc1, doc2);
    assertEquals(doc1.hashCode(), doc2.hashCode());
  }

  @Test
  public void testEqualsWithChildIdsDifferentOrder() {
    RepositoryDoc doc1 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("doc"))
            .addChildId("id1", new PushItem())
            .addChildId("id2", new PushItem())
            .build();
    RepositoryDoc doc2 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("doc"))
            .addChildId("id2", new PushItem())
            .addChildId("id1", new PushItem())
            .build();
    assertEquals(doc1, doc2);
    assertEquals(doc1.hashCode(), doc2.hashCode());
  }

  @Test
  public void testEqualsWithExtraChildIds() {
    RepositoryDoc doc1 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("doc"))
            .addChildId("id1", new PushItem())
            .addChildId("id2", new PushItem())
            .build();
    RepositoryDoc doc2 =
        new RepositoryDoc.Builder()
            .setItem(new Item().setName("doc"))
            .addChildId("id1", new PushItem())
            .addChildId("id2", new PushItem())
            .addChildId("extraId", new PushItem())
            .build();
    assertThat(doc1, not(equalTo(doc2)));
    assertThat(doc1.hashCode(), not(equalTo(doc2.hashCode())));
  }

  @Test
  public void toString_minimal() {
    Item item = new Item().setName("Don Quixote");
    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(item)
            .build();
    assertThat(doc.toString(), startsWith("RepositoryDoc ["));
    assertThat(doc.toString(), containsString("Don Quixote"));
  }

  @Test
  public void hashCode_reflexive() {
    Item item = new Item().setName("Don Quixote");
    RepositoryDoc doc =
        new RepositoryDoc.Builder()
            .setItem(item)
            .build();
    assertEquals(doc.hashCode(), doc.hashCode());
  }

  @Test
  public void hashCode_mismatch() {
    RepositoryDoc doc1 = new RepositoryDoc.Builder().setItem(new Item().setName("tick")).build();
    RepositoryDoc doc2 = new RepositoryDoc.Builder().setItem(new Item().setName("tock")).build();
    assertThat(doc1.hashCode(), not(equalTo(doc2.hashCode())));
  }


  private ItemAcl getCustomerAcl() {
    return new ItemAcl().setReaders(Collections.singletonList(Acl.getCustomerPrincipal()));
  }
}
