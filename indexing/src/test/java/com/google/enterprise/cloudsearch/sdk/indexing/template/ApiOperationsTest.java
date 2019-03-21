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
}
