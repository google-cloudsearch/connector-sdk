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
package com.google.enterprise.cloudsearch.sdk.indexing.samples;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.client.json.GenericJson;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingConnector;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingConnectorContext;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever;
import com.google.enterprise.cloudsearch.sdk.indexing.traverser.TraverserConfiguration;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Sample of Connector. Demonstrates what code is necessary for putting public content onto a Cloud
 * Search.
 */
class ConnectorTemplate implements IndexingConnector, ItemRetriever {
  private static Logger logger = Logger.getLogger(ConnectorTemplate.class.getName());

  private IndexingConnectorContext context;
  private String queueName;

  private static final RequestMode DEFAULT_UPDATE_ITEM_MODE = RequestMode.SYNCHRONOUS;
  private static final String QUEUE_NAME = "traverser.pollRequest.queue";

  /**
   * Call default main for connectors.
   *
   * @param args command line arguments
   * @throws InterruptedException if connector start code encounters a shut down
   */
  public static void main(String[] args) throws InterruptedException {
    IndexingApplication application =
        new IndexingApplication.Builder(new ConnectorTemplate(), args).build();
    application.start();
  }

  /**
   * Initialize connector.
   *
   * <p>Check configuration parameters and verify all required parameters have been set.
   *
   * @param context instance includes a reference to the indexing service
   * @throws Exception if there are start up problems
   */
  @Override
  public void init(IndexingConnectorContext context) throws Exception {
    logger.info("Initialize the connector");
    this.context = context;
    queueName = Configuration.getString(QUEUE_NAME, "default").get();
    logger.info("Queue name: " + queueName);

    context.getIndexingService().unreserve(queueName);

    TraverserConfiguration traverserConfiguration =
        new TraverserConfiguration.Builder().name("SampleTraverser").itemRetriever(this).build();
    context.registerTraverser(traverserConfiguration);
  }

  /**
   * This implements a simple list traversal algorithm with fixed (static) data. The data is
   * converted to {@link PushItem} and pushed to the indexing queue. Later, the {@link
   * ItemRetriever}'s {@link ItemRetriever#process} method will be invoked on each queue item to
   * perform the data source upload.
   */
  @Override
  public void traverse() throws IOException, InterruptedException {
    List<String> ids = Arrays.asList("id-1", "id-2", "id-6", "mutable", "not-found");

    logger.log(Level.INFO, "Pushing {0} items to index", ids.size());
    List<ListenableFuture<? extends GenericJson>> futures = new ArrayList<>();
    for (String id : ids) {
      PushItem item = new PushItem().setQueue(queueName);
      futures.add(context.getIndexingService().push(id, item));
    }
    try {
      Futures.allAsList(futures).get();
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void saveCheckpoint(boolean isShutdown) throws IOException, InterruptedException {
    // save checkpoint here if needed
  }

  @Override
  public void destroy() {
    logger.info("Destroy everything");
    // stop all your executors, release resources
  }

  /** Each {@link Item} will be processed here for data upload to the indexing service. */
  @Override
  public void process(Item polledItem) throws IOException, InterruptedException {
    logger.info("Process item with id " + polledItem.getName());
    String id = polledItem.getName();
    try {
      String content;
      if ("id-1".equals(id)) {
        content = "try apple from my box";
      } else if ("id-2".equals(id)) {
        content = "try pear from my box";
      } else if ("id-6".equals(id)) {
        content = "try avocado from my box";
      } else if ("mutable".equals(id)) {
        List<String> mutableFruits = Arrays.asList("apple", "pear", "nothing", "lemon", "cat");
        content =
            "you will be surprised, but now this box contains... "
                + mutableFruits.get(new Random().nextInt(mutableFruits.size()));
      } else {
        logger.info("- Delete item with id " + id);
        context.getIndexingService().deleteItem(id, null, RequestMode.SYNCHRONOUS);
        return;
      }

      Acl acl =
          new Acl.Builder()
              .setReaders(Collections.singletonList(Acl.getCustomerPrincipal()))
              .build();
      Item item =
          new IndexingItemBuilder(id)
              .setAcl(acl)
              .setSourceRepositoryUrl(FieldOrValue.withValue("http://view.url/" + id))
              .setTitle(FieldOrValue.withValue("Item Title " + id))
              .build();
      context
          .getIndexingService()
          .indexItemAndContent(
              item,
              new ByteArrayContent("text/plain", content.getBytes(Charset.forName("UTF-8"))),
              null,
              ContentFormat.TEXT,
              DEFAULT_UPDATE_ITEM_MODE);
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Exception uploading element (with id " + id + ")", e);
    }
  }
}
