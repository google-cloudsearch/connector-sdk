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
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.enterprise.cloudsearch.sdk.BatchPolicy;
import com.google.enterprise.cloudsearch.sdk.BatchRequestService.ExecutorFactory;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link BatchingIndexingServiceImpl}. */

@RunWith(MockitoJUnitRunner.class)
public class BatchingIndexingServiceImplTest {
  @Mock private CloudSearch service;
  @Mock private ExecutorFactory executorFactory;
  @Mock private ScheduledExecutorService scheduleExecutorService;
  @Mock private Items.Delete deleteItemRequest;
  @Mock private GoogleCredential credential;
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() {
    when(executorFactory.getExecutor()).thenReturn(MoreExecutors.newDirectExecutorService());
    when(executorFactory.getScheduledExecutor()).thenReturn(scheduleExecutorService);
  }

  private BatchingIndexingService getInstance() {
    BatchingIndexingService batchingService =
        new BatchingIndexingServiceImpl.Builder()
            .setService(service)
            .setExecutorFactory(executorFactory)
            .setBatchPolicy(new BatchPolicy.Builder().setFlushOnShutdown(false).build())
            .setCredential(credential)
            .build();
    return batchingService;
  }

  @Test
  public void testDefaultBuilder() {
    BatchingIndexingServiceImpl batchingService =
        new BatchingIndexingServiceImpl.Builder()
            .setService(service)
            .setCredential(credential)
            .build();
    checkNotNull(batchingService);
  }

  @Test
  public void testNullIndexingService() {
    thrown.expect(NullPointerException.class);
    new BatchingIndexingServiceImpl.Builder().setService(null).build();
  }

  @Test
  public void testRequireServiceStarted() throws InterruptedException {
    BatchingIndexingService batchingService = getInstance();
    thrown.expect(IllegalStateException.class);
    batchingService.deleteItem(deleteItemRequest);
  }

  @Test
  public void testCanBatchAfterStarted() throws InterruptedException {
    BatchingIndexingService batchingService = getInstance();
    batchingService.startAsync().awaitRunning();
    assertTrue(batchingService.isRunning());
    batchingService.deleteItem(deleteItemRequest);
    batchingService.stopAsync().awaitTerminated();
    assertFalse(batchingService.isRunning());
  }

  @Test
  public void testCanNotBatchBatchAfterTerminated() throws InterruptedException {
    BatchingIndexingService batchingService = getInstance();
    batchingService.startAsync().awaitRunning();
    assertTrue(batchingService.isRunning());
    batchingService.stopAsync().awaitTerminated();
    thrown.expect(IllegalStateException.class);
    batchingService.deleteItem(deleteItemRequest);
  }
}
