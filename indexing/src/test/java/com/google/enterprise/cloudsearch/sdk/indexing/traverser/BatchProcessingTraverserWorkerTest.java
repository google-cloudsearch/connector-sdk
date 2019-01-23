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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.BatchItemRetriever;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link BatchProcessingTraverserWorker}. */

public class BatchProcessingTraverserWorkerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock IndexingService indexingService;
  PollItemsRequest pollQueueRequest = new PollItemsRequest();
  @Mock BatchItemRetriever batchItemRetriever;
  TraverserConfiguration conf;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    Properties config = new Properties();
    config.put("traverser.test.pollRequest.queue", "custom");
    config.put("traverser.test.pollRequest.statuses", "");
    config.put("traverser.test.hostload", "1");
    config.put("traverser.test.timeout", "60");
    config.put("traverser.test.timeunit", "SECONDS");
    setupConfig.initConfig(new Properties());

    conf = (new TraverserConfiguration.Builder("test"))
          .name("name")
          .itemRetriever(batchItemRetriever)
          .build();
  }

  /**
   * Test method for constructor.
   */
  @Test
  public void testBatchProcessingTraverserWorker(){
    new BatchProcessingTraverserWorker(conf, indexingService);
  }

  /**
   * Test method for {@link BatchProcessingTraverserWorker#poll()}.
   * @throws IOException
   * @throws InterruptedException
   */
  @Test
  public void testPool() throws IOException, InterruptedException{
    BatchProcessingTraverserWorker worker = new BatchProcessingTraverserWorker(
        conf, indexingService);
    List<Item> entries =
        Arrays.asList(
            (new Item()).setName("id-1").setQueue("custom"),
            (new Item()).setName("id-2").setQueue("custom"),
            (new Item()).setName("id-3").setQueue("custom"));

    when(indexingService.poll(any(PollItemsRequest.class))).thenReturn(entries);
    worker.poll();

    verify(batchItemRetriever).processBatch(entries);
  }

  /**
   * Test method for {@link BatchProcessingTraverserWorker#shutdown()}.
   */
  @Test
  public void testShutdown(){
    BatchProcessingTraverserWorker worker = new BatchProcessingTraverserWorker(
        conf, indexingService);
    worker.shutdown();
  }

}
