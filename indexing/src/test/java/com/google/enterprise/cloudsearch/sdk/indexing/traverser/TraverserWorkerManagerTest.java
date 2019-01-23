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
package com.google.enterprise.cloudsearch.sdk.indexing.traverser;

import static org.junit.Assert.assertEquals;

import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.BatchItemRetriever;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link TraverserWorkerManager}. */

public class TraverserWorkerManagerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock IndexingService indexingService;
  @Mock ItemRetriever itemRetriever;
  @Mock BatchItemRetriever batchItemRetriever;
  @Mock ExecutorService executor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    Properties properties = new Properties();
    properties.setProperty("traverser.test.pollRequest.queue", "");
    properties.setProperty("traverser.test.pollRequest.statuses", "");
    properties.setProperty("traverser.test.hostload", "2");
    properties.setProperty("traverser.test.timeout", "60");
    properties.setProperty("traverser.test.timeunit", "SECONDS");
    setupConfig.initConfig(properties);
  }

  @Test
  public void testNewWorkerWithoutConf() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("configuration should be defined");
    TraverserWorkerManager.newWorker(null, indexingService);
  }

  @Test
  public void testNewWorkerWithoutIndexingService() {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("indexingService should be defined");
    TraverserConfiguration conf = (new TraverserConfiguration.Builder())
        .name("name of worker")
        .itemRetriever(batchItemRetriever)
        .build();
    TraverserWorkerManager.newWorker(conf, null);
  }

  @Test
  public void testNewWorkerWithoutItemRetriever() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("one and only one itemRetriever or batchItemRetriever should be defined");
    TraverserConfiguration conf = (new TraverserConfiguration.Builder())
        .name("name of worker")
        .build();
    TraverserWorkerManager.newWorker(conf, indexingService);
  }

  @Test
  public void testNewWorkerWithBatchItemRetriever() {
    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(batchItemRetriever)
        .build();
    TraverserWorker worker = TraverserWorkerManager.newWorker(conf, indexingService);
    assertEquals("name of worker", worker.getName());
    assertEquals(BatchProcessingTraverserWorker.class, worker.getClass());
  }

  @Test
  public void testNewWorkerWithItemRetriever() {
    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
    TraverserWorker worker = TraverserWorkerManager.newWorker(conf, indexingService);
    assertEquals("name of worker", worker.getName());
    assertEquals(ParallelProcessingTraverserWorker.class, worker.getClass());
  }

  @Test
  public void testNewWorkerwithExecutorService(){
    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("test name")
        .itemRetriever(itemRetriever)
        .build();
    TraverserWorker worker = TraverserWorkerManager.newWorker(conf, indexingService, executor);
    assertEquals("test name", worker.getName());
    assertEquals(ParallelProcessingTraverserWorker.class, worker.getClass());
  }

}
