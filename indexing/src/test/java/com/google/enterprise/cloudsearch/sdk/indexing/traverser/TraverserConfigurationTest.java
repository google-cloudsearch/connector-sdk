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
import static org.junit.Assert.assertNull;

import com.google.api.services.cloudsearch.v1.model.PollItemsRequest;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.BatchItemRetriever;
import com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link TraverserConfiguration}. */

public class TraverserConfigurationTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  PollItemsRequest pollQueueRequest = new PollItemsRequest().setQueue("default");
  @Mock ItemRetriever itemRetriever;
  @Mock BatchItemRetriever batchItemRetriever;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testBuildWithoutInitializedConfig() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Configuration should be initialized before using");
    new TraverserConfiguration.Builder()
        .build();
  }

  @Test
  public void testBuildMethod() {
    Properties config = new Properties();
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("one and only one itemRetriever or batchItemRetriever should be defined");
    new TraverserConfiguration.Builder()
        .build();
  }

  @Test
  public void testBuildWithoutItemRetriever() {
    Properties config = new Properties();
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("one and only one itemRetriever or batchItemRetriever should be defined");
    new TraverserConfiguration.Builder("suffixKey")
        .name("name of worker")
        .build();
  }

  @Test
  public void testNewWorkerWithBothItemRetrievers() {
    Properties config = new Properties();
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("one and only one itemRetriever or batchItemRetriever should be defined");
    new TraverserConfiguration.Builder("suffixKey")
        .name("name of worker")
        .itemRetriever(batchItemRetriever)
        .itemRetriever(itemRetriever)
        .build();
  }

  @Test
  public void testNewWorkerWithDefaultConfig() {
    Properties config = new Properties();
    setupConfig.initConfig(config);
    new TraverserConfiguration.Builder()
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
  }

  @Test
  public void testNewWorkerWithIncorrectHostloadParam() {
    Properties config = new Properties();
    config.put("traverser.test.hostload", "0");
    config.put("traverser.test.timeout", "60");
    config.put("traverser.test.timeunit", "SECONDS");
    setupConfig.initConfig(config);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("hostload should be greater than 0");
    new TraverserConfiguration.Builder("test")
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
  }

  @Test
  public void testNewWorkerWithIncorrectRequestLimitParam() {
    Properties config = new Properties();
    config.put("traverser.test.pollRequest.limit", "-1");
    setupConfig.initConfig(config);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("poll request limit should be greater than or equal to 0");
    new TraverserConfiguration.Builder("test")
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
  }

  @Test
  public void testNewWorkerWithIncorrectTimeoutParam() {
    Properties config = new Properties();
    config.put("traverser.test.hostload", "1");
    config.put("traverser.test.timeout", "60");
    // should be DAYS to correct initialization
    config.put("traverser.test.timeunit", "Day");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No enum constant java.util.concurrent.TimeUnit.Day");
    new TraverserConfiguration.Builder("test")
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
  }

  @Test
  public void testNewWorkerWithBatchItemRetriever() {
    Properties config = new Properties();
    config.put("traverser.test.hostload", "10");
    config.put("traverser.test.timeout", "60");
    config.put("traverser.test.timeunit", "SECONDS");
    setupConfig.initConfig(config);

    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(batchItemRetriever)
        .build();
    assertEquals("name of worker", conf.getName());
    assertEquals(pollQueueRequest, conf.getPollRequest());
    assertNull(conf.getItemRetriever());
    assertEquals(batchItemRetriever, conf.getBatchItemRetriever());
    assertEquals(10, conf.getHostload());
    assertEquals(60, conf.getTimeout());
    assertEquals(TimeUnit.SECONDS, conf.getTimeunit());
  }

  @Test
  public void testNewWorkerWithItemRetriever() {
    Properties config = new Properties();
    config.put("traverser.test.hostload", "2");
    config.put("traverser.test.timeout", "20");
    config.put("traverser.test.timeunit", "SECONDS");
    config.put("traverser.pollRequest.limit", "100");
    setupConfig.initConfig(config);

    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
    assertEquals("name of worker", conf.getName());
    assertEquals(new PollItemsRequest().setLimit(100).setQueue("default"), conf.getPollRequest());
    assertEquals(itemRetriever, conf.getItemRetriever());
    assertNull(conf.getBatchItemRetriever());
    assertEquals(2, conf.getHostload());
    assertEquals(20, conf.getTimeout());
    assertEquals(TimeUnit.SECONDS, conf.getTimeunit());
  }

  @Test
  public void testNewWorkerWithItemRetrieverWithDefaultParams() {
    Properties config = new Properties();
    config.put("traverser.hostload", "1");
    config.put("traverser.timeout", "2");
    config.put("traverser.timeunit", "MINUTES");
    setupConfig.initConfig(config);

    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
    assertEquals("name of worker", conf.getName());
    assertEquals(pollQueueRequest, conf.getPollRequest());
    assertEquals(itemRetriever, conf.getItemRetriever());
    assertNull(conf.getBatchItemRetriever());
    assertEquals(1, conf.getHostload());
    assertEquals(2, conf.getTimeout());
    assertEquals(TimeUnit.MINUTES, conf.getTimeunit());
  }

  @Test
  public void testNewWorkerWithItemRetrieverWithMixedParams() {
    Properties config = new Properties();
    config.put("traverser.test.hostload", "100");
    config.put("traverser.hostload", "1");
    config.put("traverser.timeout", "120");
    config.put("traverser.timeunit", "SECONDS");
    setupConfig.initConfig(config);

    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
    assertEquals("name of worker", conf.getName());
    assertEquals(pollQueueRequest, conf.getPollRequest());
    assertEquals(itemRetriever, conf.getItemRetriever());
    assertNull(conf.getBatchItemRetriever());
    assertEquals(100, conf.getHostload());
    assertEquals(120, conf.getTimeout());
    assertEquals(TimeUnit.SECONDS, conf.getTimeunit());
  }

  @Test
  public void testNewWorkerWithOverriddenConfig() {
    Properties config = new Properties();
    config.put("traverser.test.hostload", "5");
    config.put("traverser.test.timeout", "20");
    config.put("traverser.test.timeunit", "SECONDS");
    config.put("traverser.test.pollRequest.queue", "queue");
    config.put("traverser.test.pollRequest.statuses", "ACCEPTED,MODIFIED");
    setupConfig.initConfig(config);

    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
    assertEquals("name of worker", conf.getName());
    assertEquals(
        new PollItemsRequest()
            .setQueue("queue")
            .setStatusCodes(ImmutableList.of("ACCEPTED", "MODIFIED")),
        conf.getPollRequest());
    assertEquals(itemRetriever, conf.getItemRetriever());
    assertNull(conf.getBatchItemRetriever());
    assertEquals(5, conf.getHostload());
    assertEquals(20, conf.getTimeout());
    assertEquals(TimeUnit.SECONDS, conf.getTimeunit());
  }

  @Test
  public void testNewWorkerWithOverriddenQueueName() {
    Properties config = new Properties();
    config.put("traverser.hostload", "3");
    config.put("traverser.timeout", "20");
    config.put("traverser.timeunit", "MINUTES");
    config.put("traverser.pollRequest.queue", "testQueue");
    config.put("traverser.pollRequest.statuses", "ACCEPTED");
    config.put("traverser.test.pollRequest.queue", "testQueueOverridden");
    config.put("traverser.test.pollRequest.statuses", "NEW_ITEM");
    setupConfig.initConfig(config);

    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
    assertEquals("name of worker", conf.getName());
    assertEquals("testQueueOverridden", conf.getPollRequest().getQueue());
    assertEquals(1, conf.getPollRequest().getStatusCodes().size());
    assertEquals("NEW_ITEM", conf.getPollRequest().getStatusCodes().get(0));
    assertEquals(itemRetriever, conf.getItemRetriever());
    assertNull(conf.getBatchItemRetriever());
    assertEquals(3, conf.getHostload());
    assertEquals(20, conf.getTimeout());
    assertEquals(TimeUnit.MINUTES, conf.getTimeunit());
  }

  @Test
  public void testNewWorkerWithNonOverriddenQueueNameAndStatuses() {
    Properties config = new Properties();
    config.put("traverser.hostload", "10");
    config.put("traverser.timeout", "20");
    config.put("traverser.timeunit", "SECONDS");
    config.put("traverser.pollRequest.queue", "testQueue");
    config.put("traverser.pollRequest.statuses", "ACCEPTED");
    config.put("traverser.test.pollRequest.queue", "");
    config.put("traverser.test.pollRequest.statuses", "");
    setupConfig.initConfig(config);

    TraverserConfiguration conf = (new TraverserConfiguration.Builder("test"))
        .name("name of worker")
        .itemRetriever(itemRetriever)
        .build();
    assertEquals("name of worker", conf.getName());
    assertEquals("testQueue", conf.getPollRequest().getQueue());
    assertEquals(1, conf.getPollRequest().getStatusCodes().size());
    assertEquals("ACCEPTED", conf.getPollRequest().getStatusCodes().get(0));
    assertEquals(itemRetriever, conf.getItemRetriever());
    assertNull(conf.getBatchItemRetriever());
    assertEquals(10, conf.getHostload());
    assertEquals(20, conf.getTimeout());
    assertEquals(TimeUnit.SECONDS, conf.getTimeunit());
  }

}
