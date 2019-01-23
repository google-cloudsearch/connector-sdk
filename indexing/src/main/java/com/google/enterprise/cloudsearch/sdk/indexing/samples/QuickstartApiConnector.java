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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.CloudSearch.Indexing.Datasources.Items;
import com.google.api.services.cloudsearch.v1.model.GSuitePrincipal;
import com.google.api.services.cloudsearch.v1.model.IndexItemRequest;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.ItemContent;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.Principal;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Set;

/**
 * Sample connector using the Cloud Search API.
 *
 * <p>This is a simplified "Hello World!" sample connector using the Cloud Search API directly
 * (bypassing the Cloud Search SDK). This sample connector performs a simple upload of static data.
 * To take advantage of automated scheduling, multi-threading, traversing strategies and other
 * helpful features, see the SDK sample connector and SDK documentation.
 *
 * <p>The service account private key file, the data source id and Cloud Search root URL must be
 * provided to this sample connector via the command line. For this example, the service key file is
 * "PrivateKey.json", the data source id is "1234567890abcdef" and the root URL is
 * "https://cloudsearch.googleapis.com":
 *
 * <pre> java -cp google-cloud-search-quickstart-api-connector-v11-0.0.1.jar
 * com.google.enterprise.cloudsearch.sdk.sample.QuickstartApiConnector
 * PrivateKey.json
 * 1234567890abcdef
 * https://cloudsearch.googleapis.com
 * </pre>
 */
public class QuickstartApiConnector {

  private static final String APPLICATION_NAME = "Cloud Search API Java Quickstart";
  private static final Set<String> API_SCOPES =
      Collections.singleton("https://www.googleapis.com/auth/cloud_search");
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static HttpTransport httpTransport;
  private static final int NUMBER_OF_DOCUMENTS = 3;

  /**
   * Builds and return an authorized CloudSearch client service.
   *
   * @param keyFile the service account key file name
   * @return an authorized CloudSearch client service
   */
  private static CloudSearch getCloudSearchService(String keyFile, String rootUrl)
      throws IOException, GeneralSecurityException {
    InputStream in = Files.newInputStream(Paths.get(keyFile));
    httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    Credential credential =
        GoogleCredential.fromStream(in, httpTransport, JSON_FACTORY).createScoped(API_SCOPES);
    return new CloudSearch.Builder(httpTransport, JSON_FACTORY, credential)
        .setApplicationName(APPLICATION_NAME)
        .setRootUrl(rootUrl)
        .build();
  }

  /**
   * Runs the Quickstart API sample connector.
   *
   * <p>The command line arguments are required to specify the json service account key file, the
   * data source and the Cloud Search root URL. The key file and root URL are used to create the
   * Cloud Search service which is the access point of the API. The data source is used for the API
   * calls.
   *
   * <p>The created Cloud Search service is used to upload a small set of static document items to
   * the data source for indexing and then the connector exits.
   *
   * @param args program command line arguments
   * @throws IOException thrown by API on communication errors
   */
  public static void main(String[] args) throws IOException, GeneralSecurityException {
    System.out.println("Simple API Connector starts.");

    if (args.length != 3) {
      System.err
          .printf("Usage: %s keyFile sourceId rootUrl\n", QuickstartApiConnector.class.getName());
      System.exit(1);
    }
    String keyFile = args[0];
    String sourceId = args[1];
    String rootUrl = args[2];

    // Build a new authorized API client service.
    CloudSearch service = getCloudSearchService(keyFile, rootUrl);
    for (int i = 1; i <= NUMBER_OF_DOCUMENTS; i++) {
      // generate a unique item id and static content
      String id = "datasources/" + sourceId + "/items/API_ID" + i;
      String content = "Hello World (api)" + i + "!";

      // create the "document" and upload to Cloud Search for indexing
      uploadDocument(service, id, content);

      System.out.println("Simple API Connector added document " + id + " (" + content + ").");
    }

    System.out.println("Simple API Connector exits.");
  }

  /**
   * Uses the API to upload a single "document".
   *
   * <p>Many document attributes are statically defined such as view URL and ACL. This can be
   * expanded to include metadata fields, etc.
   *
   * @param service the Cloud Search service access to the API
   * @param id the "name" of the document, must be unique in the data source
   * @param content the document content, just simple text here
   * @throws IOException on API communication errors
   */
  private static void uploadDocument(CloudSearch service, String id, String content)
      throws IOException {

    // the document ACL is required: for this sample, the document is publicly readable within the
    // data source domain
    ItemAcl itemAcl =
        new ItemAcl()
            .setReaders(
                Collections.singletonList(
                    new Principal()
                        .setGsuitePrincipal(new GSuitePrincipal().setGsuiteDomain(true))));

    // the document view URL is required: for this sample, just using a generic URL search link
    String viewUrl = "www.google.com";

    // the document version is required: for this sample, just using a current timestamp
    byte[] version = Long.toString(System.currentTimeMillis()).getBytes();

    // create the "document"
    Item document =
        new Item()
            .setName(id)
            .setItemType("CONTENT_ITEM")
            .setContent(
                new ItemContent().encodeInlineContent(content.getBytes()).setContentFormat("TEXT"))
            .setMetadata(new ItemMetadata().setSourceRepositoryUrl(viewUrl))
            .setAcl(itemAcl)
            .encodeVersion(version);

    // create the API Index request
    Items.Index indexRequest =
        service
            .indexing()
            .datasources()
            .items()
            .index(
                document.getName(),
                new IndexItemRequest().setItem(document).setMode("SYNCHRONOUS"));
    // execute the API request
    Operation item = indexRequest.execute();
    System.out.println("Update request results: " + item.toString());
  }
}
