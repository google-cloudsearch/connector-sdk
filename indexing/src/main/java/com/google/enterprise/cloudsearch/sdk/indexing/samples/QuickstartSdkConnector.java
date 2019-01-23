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
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.FullTraversalConnector;
import com.google.enterprise.cloudsearch.sdk.indexing.template.PushItems;
import com.google.enterprise.cloudsearch.sdk.indexing.template.Repository;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Sample connector using the Cloud Search SDK.
 *
 * <p>This is a simplified "Hello World!" sample connector that takes advantage of the Cloud Search
 * SDK including its optional template classes.
 *
 * <p>The configuration file must be created with the correct parameters for the connector to access
 * the data source and (optionally) schedule traversals. This configuration file (for example:
 * QuickstartSdk.config) is supplied to the connector via a command line argument:
 *
 * <pre> java -cp google-cloudsearch-quickstart-sdk-connector.jar
 * com.google.enterprise.cloudsearch.sdk.sample.QuickstartSdkConnector
 * -Dconfig=QuickstartSdk.config
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
 * # This simple sample only needs to run one time and exit
 * #
 * connector.runOnce=true
 *
 * #
 * # Optional scheduling properties
 * # (These would be used to schedule the traversals at fixed intervals)
 * #
 * #schedule.traversalIntervalSecs=36000
 * #schedule.performTraversalOnStart=true
 * #schedule.incrementalTraversalIntervalSecs=3600
 *
 * #
 * # Optional traverser properties
 * # (If missing, SDK defaults are used)
 * #
 * traverser.timeout=120
 * traverser.timeunit=SECONDS
 * traverse.containerTag=QuickstartSdk
 * traverse.exceptionHandler=0
 * </pre>
 */
public class QuickstartSdkConnector {

  /**
   * Starting point for the Quickstart SDK sample connector execution.
   *
   * <p>This sample connector uses the Cloud Search SDK template class for a "full traversal"
   * connector. This leverages the SDK to use a prebuilt framework for scheduling traversals so
   * that the only required code herein is to perform the actual collection of data from the data
   * repository.
   *
   * @param args program command line arguments
   * @throws IOException thrown by SDK on communication errors
   * @throws InterruptedException thrown if an abort is issued during initialization
   */
  public static void main(String[] args) throws IOException, InterruptedException {
    IndexingApplication application = new IndexingApplication.Builder(
        new FullTraversalConnector(new SimpleRepository()), args)
        .build();
    application.start();
  }

  /**
   * Simplistic sample repository.
   *
   * <p>By using the SDK provided connector templates, the only code required from the connector
   * developer are the methods from the {@link Repository} class. These are used to perform the
   * actual access of the data for uploading via the API.
   */
  public static class SimpleRepository implements Repository {

    private static final int NUMBER_OF_DOCUMENTS = 3;

    /**
     * Performs any data repository initializations here.
     *
     * @param context the {@link RepositoryContext}, not used here
     * @throws RepositoryException when repository initialization fails
     */
    @Override
    public void init(RepositoryContext context) throws RepositoryException {
      System.out.println("Simple Repository init().");
    }

    /**
     * Performs any data repository shut down code here.
     */
    @Override
    public void close() {
      System.out.println("Simple Repository close().");
    }

    /**
     * Gets all the data repository documents.
     *
     * <p>This is the core of the {@link Repository} implemented code for a full traversal template
     * connector. A complete traversal of the entire data repository is performed here. This would
     * be unused in the listing traversal template connector implementation.
     *
     * <p>For this simple sample, there are only a small set of statically created documents
     * defined. This code would be expanded upon to interface to an actual external data repository.
     * For a large data repository that might take too much memory to hold all of the documents,
     * return instead an iterator that accesses documents one at a time as requested by the template
     * connector.
     *
     * @param checkpoint save state from last iteration
     * @return all the data repository documents, typically in an iterator of {@link RepositoryDoc}
     *     instances
     * @throws RepositoryException on data access errors
     */
    @Override
    public CheckpointCloseableIterable<ApiOperation> getAllDocs(byte[] checkpoint)
        throws RepositoryException {
      System.out.println("Simple Repository getAllDocs().");

      // this sample builds a list of documents for indexing to Cloud Search
      List<ApiOperation> allDocs = new ArrayList<>();
      for (int i = 1; i <= NUMBER_OF_DOCUMENTS; i++) {
        String id = "SDK_ID" + i;
        String content = "Hello World (sdk) " + i + "!";
        allDocs.add(createDoc(id, content));
        System.out.println("Simple Repository added document " + id + " (" + content + ").");
      }
      return new CheckpointCloseableIterableImpl.Builder<>(allDocs).build();
    }

    /**
     * Creates a document for indexing.
     *
     * <p>For this connector sample, the created document is domain public searchable. The content
     * is a simple text string.
     *
     * @param id unique id for the document
     * @param content document's searchable content
     * @return the fully formed document ready for indexing
     */
    private RepositoryDoc createDoc(String id, String content) {
      // the document ACL is required: for this sample, the document is publicly readable within the
      // data source domain
      Acl acl = new Acl.Builder()
          .setReaders(Collections.singletonList(Acl.getCustomerPrincipal())).build();

      // the document view URL is required: for this sample, just using a generic URL search link
      String viewUrl = "www.google.com";

      // the document version is required: for this sample, just using a current timestamp
      byte[] version = Long.toString(System.currentTimeMillis()).getBytes();

      // using the SDK item builder class to create the document with appropriate attributes
      // (this can be expanded to include metadata fields etc.)
      Item item = new IndexingItemBuilder(id)
          .setItemType(ItemType.CONTENT_ITEM)
          .setAcl(acl)
          .setSourceRepositoryUrl(IndexingItemBuilder.FieldOrValue.withValue(viewUrl))
          .setVersion(version)
          .build();

      // for this sample, content is just plain text
      ByteArrayContent byteContent = ByteArrayContent.fromString("text/plain", content);

      // create the fully formed document
      RepositoryDoc document = new RepositoryDoc.Builder()
          .setItem(item)
          .setContent(byteContent, ContentFormat.TEXT)
          .build();

      return document;
    }

    //
    // The following method is not used in this simple full traversal sample connector, but could
    // be implemented if the data repository supports a way to detect changes.
    //

    /**
     * Gets all documents from the data repository that have been modified since the last
     * checkpoint.
     *
     * <p>This can be called by either the full or listing traversal connectors if supported by the
     * data repository. This method could even perform document deletes if delete detection was
     * supported by the data repository. This simple sample does not support incremental changes.
     *
     * @param checkpoint save state from last iteration
     * @return changed documents or {@code null} if incremental changes are not supported
     * @throws RepositoryException on data access errors
     */
    @Override
    public CheckpointCloseableIterable<ApiOperation> getChanges(byte[] checkpoint)
        throws RepositoryException {
      return null;
    }

    //
    // The following methods are not used in the full traversal connector, but might be used in
    // the template and/or custom listing traversal connector implementations.
    //

    /**
     * Gets only the document IDs from the data repository.
     *
     * <p>This is unused in this full traversal connector sample. If using the listing traversal,
     * this method would fetch only the data IDs to be pushed to the Cloud Search API indexing
     * queue. Then a separate SDK thread would poll these IDs and call {@link #getDoc(Item)} for
     * each ID to fetch the document for indexing.
     *
     * @param checkpoint save state from last iteration
     * @return typically this is a {@link PushItems} instance
     * @throws RepositoryException on data access errors
     */
    @Override
    public CheckpointCloseableIterable<ApiOperation> getIds(byte[] checkpoint)
        throws RepositoryException {
      return null;
    }

    /**
     * Gets a single data repository document.
     *
     * <p>This is unused in this full traversal connector sample. If using the listing traversal,
     * this method would fetch only the data repository document specified. This occurs during
     * {@link com.google.enterprise.cloudsearch.sdk.indexing.ItemRetriever#process(Item)} calls.
     *
     * @param item the data repository document to retrieve
     * @return typically a {@link RepositoryDoc} instance
     * @throws RepositoryException on data access errors
     */
    @Override
    public ApiOperation getDoc(Item item) throws RepositoryException {
      return null;
    }

    /**
     * Checks the existence of a single data repository document.
     *
     * <p>This is not used by either full or listing traversal connectors, but is available for
     * custom connectors to assist with delete detection.
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
