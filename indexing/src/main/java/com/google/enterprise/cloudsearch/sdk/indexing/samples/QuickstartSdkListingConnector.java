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
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingServiceImpl.PollItemStatus;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperations;
import com.google.enterprise.cloudsearch.sdk.indexing.template.DeleteItem;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ListingConnector;
import com.google.enterprise.cloudsearch.sdk.indexing.template.PushItems;
import com.google.enterprise.cloudsearch.sdk.indexing.template.Repository;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import java.io.IOException;
import java.util.Collections;

/**
 * Sample template list connector using the Cloud Search SDK.
 *
 * <p>This is a simplified "Hello World!" sample connector that takes advantage of the Cloud Search
 * SDK including its optional template classes.
 *
 * <p>The configuration file must be created with the correct parameters for the connector to access
 * the data source and (optionally) schedule traversals. This configuration file (for example:
 * QuickstartSdkListing.config) is supplied to the connector via a Linux command line argument:
 *
 * <pre> java
 * -cp "google-cloudsearch-quickstart-sdk-listing-connector.jar"
 * com.google.enterprise.cloudsearch.sdk.sample.QuickstartSdkListingConnector
 * -Dconfig=QuickstartSdkListing.config
 * </pre>
 *
 * <p>Sample configuration file:
 *
 * <pre>
 * #
 * # Required properties for accessing data source
 * # (These values are created by the admin before running the connector)
 * #
 * api.sourceId=1234567890abcdef
 * api.serviceAccountPrivateKeyFile=./PrivateKey.json
 *
 * #
 * # Optional scheduling properties
 * # (If missing, SDK defaults are used)
 * # These are used to schedule the traversals at fixed intervals
 * # For this sample, full traversals every 2 minutes
 * #
 * schedule.traversalIntervalSecs=120
 * schedule.performTraversalOnStart=true
 * </pre>
 */
public class QuickstartSdkListingConnector {

  /**
   * Starting point for the Quickstart SDK sample listing connector execution.
   *
   * <p>This sample connector uses the Cloud Search SDK template class for a "list traversal"
   * connector. This leverages the SDK to use a prebuilt framework for scheduling traversals so
   * that the only required code is to perform the actual collection of data from the data
   * repository.
   *
   * <p>A <em>list traversal</em> strategy has multiple flavors. This sample code simulates a
   * data repository that is non-hierarchical (no children documents or containers) and does not
   * have document modification or deletion detection.
   * <ul>
   * <li>Non-hierarchical</li>
   * <li>No change detection</li>
   * </ul>
   *
   * @param args program command line arguments
   * @throws IOException thrown by SDK on communication errors
   * @throws InterruptedException thrown if an abort is issued during initialization
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    IndexingApplication application = new IndexingApplication.Builder(
        new ListingConnector(new SimpleRepository()), args)
        .build();
    application.start();
  }

  /**
   * Simplistic sample repository.
   *
   * <p>By using the SDK provided connector templates, the only code required from the connector
   * developer are the methods from the {@link Repository} class. These are used to perform the
   * actual access of the data for indexing to Cloud Search using the API.
   */
  public static class SimpleRepository implements Repository {

    /**
     * This object is an interface to our data repository.
     *
     * <p>This connector is not concerned with its implementation details. It only knows to create
     * an instance, use its {@code initializeForNextTraversal()} method to set up each traversal,
     * and use all of its appropriate accessor methods to retrieve documents.
     */
    private QuickstartSdkDocumentManager documentManager;

    /**
     * Performs any data repository initializations here.
     *
     * @param context the {@link RepositoryContext}, not used here
     * @throws RepositoryException when repository initialization fails
     */
    @Override
    public void init(RepositoryContext context) throws RepositoryException {
      System.out.println("Simple Repository init().");
      documentManager = new QuickstartSdkDocumentManager();
    }

    /**
     * Performs any data repository shut down code here.
     */
    @Override
    public void close() {
      System.out.println("Simple Repository close().");
    }

    /**
     * Gets all of the existing document IDs from the data repository.
     *
     * <p>This method is called by {@link ListingConnector#traverse()} during <em>full
     * traversals</em>. Every document ID and content hash value in the <em>repository</em> is
     * pushed to the Cloud Search queue. Each pushed document is later polled and processed in the
     * {@link #getDoc(Item)} method.
     *
     * <p>The content hash values are pushed because this sample connector's repository does not
     * have built in document change detection. The queue sets the document status depending on the
     * hash comparison. If the pushed ID doesn't yet exist in Cloud Search, the document's status is
     * set to <em>new</em>. If the ID exists but has a mismatched hash value, its status is set to
     * <em>modified</em>. If the ID exists and matches the hash value, its status is unchanged.
     *
     * <p>In every case, the pushed content hash value is only used for comparison. The hash value
     * is only set in the queue during an update (see {@link #getDoc(Item)}).
     *
     * @param checkpoint value defined and maintained by this connector
     * @return this is typically a {@link PushItems} instance
     * @throws RepositoryException on data access errors
     */
    @Override
    public CheckpointCloseableIterable<ApiOperation> getIds(byte[] checkpoint)
        throws RepositoryException {
      // prepare the data repository for the next simulated traversal
      documentManager.initializeForNextTraversal();

      PushItems.Builder allIds = new PushItems.Builder();
      for (String id : documentManager.getAllDocumentIds()) {
        allIds.addPushItem(id, new PushItem().setContentHash(documentManager.getContentHash(id)));
        System.out.println("Simple Repository getIds() pushed document " + id);
      }
      ApiOperation pushItemsOperation = allIds.build();

      return new CheckpointCloseableIterableImpl.Builder<>(
              Collections.singletonList(pushItemsOperation))
          .build();
    }

    /**
     * Gets a single data repository document.
     *
     * <p>This method is called by the {@link ListingConnector} during a poll of the Cloud Search
     * queue. Each queued document is processed individually depending on its state in the data
     * repository:
     *
     * <ul>
     * <li>Missing: The document is no longer in the data repository, so it is deleted from
     * Cloud Search.</li>
     * <li>Unmodified: The document is already indexed and it has not changed, so re-push with an
     * unmodified status.</li>
     * <li>New or modified: The document is brand new, or has been modified since it was indexed, so
     * re-index it.</li>
     * </ul>
     *
     * <p>The content hash is sent during all <em>new</em> or <em>modified</em> status document
     * updates. This hash value is stored with the document in the Cloud Search API queue for future
     * comparisons of pushed document IDs (see {@link #getIds(byte[])}).
     *
     * @param item the data repository document to retrieve
     * @return the document's state determines which type of {@link ApiOperation} is returned:
     * {@link RepositoryDoc}, {@link DeleteItem}, or {@link PushItem}
     * @throws RepositoryException on data access errors
     */
    @Override
    public ApiOperation getDoc(Item item) throws RepositoryException {
      String id = item.getName();
      String status = item.getStatus().getCode();
      System.out.print("Simple Repository getDoc(): [" + id + "] status: " + status);
      if (!documentManager.documentExists(id)) {
        System.out.println(" ...delete document.");
        return ApiOperations.deleteItem(id);
      } else if (status.equals(PollItemStatus.ACCEPTED.toString())) {
        System.out.println(" ...push document.");
        PushItem pushItem = new PushItem().setType("NOT_MODIFIED");
        return new PushItems.Builder().addPushItem(id, pushItem).build();
      } else {
        System.out.println(" ...update document.");
        return createDoc(id);
      }
    }

    /**
     * Gets all of the detected changes to the data repository.
     *
     * This method is not used in this list traversal sample connector, but could be implemented if
     * the data repository supports a way to detect changes. The configuration file optionally
     * defines incremental traversal scheduling parameters.
     *
     * @param checkpoint value defined and maintained by this connector
     * @return object representing all the data repository changes
     * @throws RepositoryException on data access errors
     */
    @Override
    public CheckpointCloseableIterable<ApiOperation> getChanges(byte[] checkpoint)
        throws RepositoryException {
      return null;
    }

    /**
     * Creates a document for indexing.
     *
     * <p>For this connector sample, the created document is domain public searchable. The content
     * is a simple text string. See {@link #getDoc(Item)} for the explanation for setting the
     * document's content hash value.
     *
     * <p>If this were a hierarchical data repository, this method would also use the {@link
     * RepositoryDoc.Builder#addChildId(String, PushItem)} method to push each document's children
     * to the Cloud Search queue. Each child ID would be processed in turn during a future {@link
     * #getDoc(Item)} call.
     *
     * @param id unique id for the document
     * @return the fully formed document ready for indexing
     */
    RepositoryDoc createDoc(String id) {
      // the document ACL is required: for this sample, the document is publicly readable within the
      // data source domain
      Acl acl = new Acl.Builder()
          .setReaders(Collections.singletonList(Acl.getCustomerPrincipal())).build();

      // the document view URL is required: for this sample, just using a generic URL search link
      String viewUrl = "https://www.google.com";

      // using the SDK item builder class to create the document with appropriate attributes
      // (this can be expanded to include metadata fields on so on)
      Item item = new IndexingItemBuilder(id)
          .setItemType(ItemType.CONTENT_ITEM)
          .setAcl(acl)
          .setSourceRepositoryUrl(IndexingItemBuilder.FieldOrValue.withValue(viewUrl))
          // The document version is also required. For this sample, we use the SDK default which
          // just uses a current timestamp. If your data repository has a meaningful sense of
          // version, set it here.
          // .setVersion(documentManager.getVersion(id))
          .build();

      // for this sample, content is just plain text
      String content = documentManager.getContent(id);
      ByteArrayContent byteContent = ByteArrayContent.fromString("text/plain", content);

      // create the fully formed document
      RepositoryDoc document = new RepositoryDoc.Builder()
          .setItem(item)
          .setContent(byteContent, documentManager.getContentHash(id), ContentFormat.TEXT)
          .build();

      return document;
    }

    /**
     * Gets every document from the data repository for indexing into Cloud Search.
     *
     * <p>This method is not used in the list traversal connector, but might be used in the template
     * and/or custom full traversal connector implementations. A connector using this method
     * typically would not push document IDs to the Cloud Search indexing queue.
     *
     * @param checkpoint value defined and maintained by this connector
     * @return list of every document in the data repository typically as {@link RepositoryDoc}
     * objects
     * @throws RepositoryException on data access errors
     */
    @Override
    public CheckpointCloseableIterable<ApiOperation> getAllDocs(byte[] checkpoint)
        throws RepositoryException {
      return null;
    }

    /**
     * Checks the existence of a single data repository document.
     *
     * <p>This is not used by either full or listing template traversal connectors, but is available
     * for custom connectors to assist with delete detection.
     *
     * @param item the document to verify existence
     * @return {@code true} if the document matching the passes ID exists, {@code false} indicates
     * that the {@link Item} should be deleted from the data source
     * @throws RepositoryException on data access errors
     */
    @Override
    public boolean exists(Item item) throws RepositoryException {
      return false;
    }
  }
}
