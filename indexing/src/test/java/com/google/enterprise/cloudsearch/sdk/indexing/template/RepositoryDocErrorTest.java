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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Unit tests for {@link RepositoryDocError}. */
@RunWith(MockitoJUnitRunner.class)
public class RepositoryDocErrorTest {
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock IndexingService mockIndexingService;

  @Test
  public void constructor_nullId_throwsException() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("itemId");
    thrown.expectMessage(not(containsString("exception")));
    new RepositoryDocError(null, new RepositoryException.Builder().build());
  }

  @Test
  public void constructor_emptyId_throwsException() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("itemId");
    thrown.expectMessage(not(containsString("exception")));
    new RepositoryDocError("", new RepositoryException.Builder().build());
  }

  @Test
  public void constructor_nullException_throwsException() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage(not(containsString("itemId")));
    thrown.expectMessage("exception");
    new RepositoryDocError("foo", null);
  }

  @Test
  public void execute() throws IOException, InterruptedException {
    when(mockIndexingService.push(any(), any()))
        .thenReturn(Futures.immediateFuture(new Item()));

    RepositoryDocError subject =
        new RepositoryDocError("foo", new RepositoryException.Builder().build());
    subject.execute(mockIndexingService);

    ArgumentCaptor<PushItem> pushItemCaptor = ArgumentCaptor.forClass(PushItem.class);
    verify(mockIndexingService)
        .push(eq("foo"), pushItemCaptor.capture());
    PushItem pushItem = pushItemCaptor.getValue();
    assertThat(pushItem.getType(), equalTo("REPOSITORY_ERROR"));
    assertThat(pushItem.getRepositoryError(), equalTo(new RepositoryError()));
  }


  @Test
  public void execute_pushThrowsException_throwsException()
      throws IOException, InterruptedException {
    SettableFuture<Item> pushFuture = SettableFuture.create();
    pushFuture.setException(new RuntimeException("expected exception"));
    when(mockIndexingService.push(any(), any()))
        .thenReturn(pushFuture);

    RepositoryDocError subject =
        new RepositoryDocError("foo", new RepositoryException.Builder().build());
    thrown.expect(IOException.class);
    thrown.expectCause(isA(RuntimeException.class));
    subject.execute(mockIndexingService);
  }
}
