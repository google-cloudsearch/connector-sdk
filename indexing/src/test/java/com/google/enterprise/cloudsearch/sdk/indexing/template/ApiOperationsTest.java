/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.enterprise.cloudsearch.sdk.indexing.template;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Unit tests for {@link ApiOperations} and the classes its methods return. */
@RunWith(JUnitParamsRunner.class)
public class ApiOperationsTest {
  private ImmutableList<ApiOperation> parametersForSingleArgumentTests() {
    ImmutableList.Builder<ApiOperation> builder = new ImmutableList.Builder<>();
    builder
      .add(ApiOperations.deleteItem("foo"))
      .add(ApiOperations.deleteQueueItems("foo"))
      .add(new PushItems.Builder().build());
    builder.add(ApiOperations.batch(builder.build().iterator()));
    return builder.build();
  }

  @Test
  @Parameters(method = "parametersForSingleArgumentTests")
  public void equals_reflexive_returnsTrue(ApiOperation subject) {
    assertThat(subject, equalTo(subject));
  }

  @Test
  @Parameters(method = "parametersForSingleArgumentTests")
  public void equals_null_returnsFalse(ApiOperation subject) {
    assertThat(subject, not(equalTo(null)));
  }

  @Test
  @Parameters(method = "parametersForSingleArgumentTests")
  public void equals_object_returnsFalse(ApiOperation subject) {
    Object other = new Object();
    assertThat(subject, not(equalTo(other)));
  }

  private ImmutableList<ImmutableList<ApiOperation>> parametersForTwoArgumentTests() {
    ImmutableList.Builder<ImmutableList<ApiOperation>> builder = new ImmutableList.Builder<>();
    builder
        .add(
            ImmutableList.of(
                ApiOperations.deleteItem("foo"),
                ApiOperations.deleteItem("bar")))
        .add(
            ImmutableList.of(
                ApiOperations.deleteItem("foo"),
                ApiOperations.deleteItem("foo", "1".getBytes(UTF_8), RequestMode.UNSPECIFIED)))
        .add(
            ImmutableList.of(
                ApiOperations.deleteItem("foo"),
                ApiOperations.deleteItem("foo", null, RequestMode.SYNCHRONOUS)))
        .add(
            ImmutableList.of(
                ApiOperations.deleteQueueItems("foo"),
                ApiOperations.deleteQueueItems("bar")))
        .add(
            ImmutableList.of(
                new PushItems.Builder().build(),
                new PushItems.Builder()
                    .addPushItem("foo", new PushItem())
                    .build()));
    ImmutableList<ImmutableList<ApiOperation>> inProgress = builder.build();
    builder.add(
        ImmutableList.of(
            ApiOperations.batch(inProgress.get(0).iterator()),
            ApiOperations.batch(inProgress.get(1).iterator())));
    return builder.build();
  }

  @Test
  @Parameters(method = "parametersForTwoArgumentTests")
  public void equals_differentIds_returnsFalse(ApiOperation subject, ApiOperation other) {
    assertThat(subject, not(equalTo(other)));
    if (!(subject instanceof BatchApiOperation)) { // See BatchApiOperation.hashCode()
      assertThat(subject.hashCode(), not(equalTo(other.hashCode())));
    }
  }

  @Test
  public void deleteItem_helpers() {
    DeleteItem deleteItem = (DeleteItem) ApiOperations.deleteItem("itemId");
    assertThat(deleteItem.getId(), equalTo("itemId"));
    assertThat(deleteItem.toString(), equalTo("DeleteItem [itemId=itemId]"));
  }

  @Test
  public void deleteQueueItems_helpers() {
    DeleteQueueItems deleteQueueItems =
        (DeleteQueueItems) ApiOperations.deleteQueueItems("queueName");
    assertThat(deleteQueueItems.getQueueName(), equalTo("queueName"));
    assertThat(deleteQueueItems.toString(), equalTo("DeleteQueueItems [queueName=queueName]"));
  }

  @Test
  public void pushItems_helpers() {
    PushItems pushItems = new PushItems.Builder()
        .addPushItem("item1", new PushItem())
        .addPushItem("item2", new PushItem().setQueue("test queue"))
        .addPushItem("item3", new PushItem())
        .build();
    assertThat(pushItems.getPushItemResources().size(), equalTo(3));
    assertThat(pushItems.getPushItemResources().stream()
            .map(item -> item.getId()).collect(ImmutableList.toImmutableList()),
            equalTo(ImmutableList.of("item1", "item2", "item3")));
    assertThat(pushItems.toString(), equalTo("PushItems [items=[[itemId=item1, pushItem=GenericData{classInfo=[contentHash, metadataHash, payload, queue, repositoryError, structuredDataHash, type], {}}], [itemId=item2, pushItem=GenericData{classInfo=[contentHash, metadataHash, payload, queue, repositoryError, structuredDataHash, type], {queue=test queue}}], [itemId=item3, pushItem=GenericData{classInfo=[contentHash, metadataHash, payload, queue, repositoryError, structuredDataHash, type], {}}]]]"));
    assertThat(pushItems.getPushItemResources().get(0),
        equalTo(new PushItemResource("item1", new PushItem())));
  }

  @Test
  public void repositoryDocError_helpers() {
    RepositoryDocError error = new RepositoryDocError("itemId",
        new RepositoryException.Builder()
            .setErrorMessage("error message")
            .setErrorType(RepositoryException.ErrorType.UNKNOWN)
            .setCause(new Exception("test cause"))
            .build());
    assertThat(error.getId(), equalTo("itemId"));
    assertThat(error.toString(), equalTo("RepositoryDocError [itemId=itemId, exception=GenericData{classInfo=[errorMessage, httpStatusCode, type], {errorMessage=error message, type=UNKNOWN}}, cause=java.lang.Exception: test cause]"));
  }
}
