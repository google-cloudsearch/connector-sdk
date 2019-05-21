package com.google.enterprise.cloudsearch.sdk.serving;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.cloudsearch.v1.CloudSearch;
import com.google.api.services.cloudsearch.v1.model.RequestOptions;
import com.google.api.services.cloudsearch.v1.model.SearchRequest;
import com.google.api.services.cloudsearch.v1.model.SearchResponse;
import com.google.api.services.cloudsearch.v1.model.SearchResult;
import com.google.enterprise.cloudsearch.sdk.BaseApiService.RetryRequestInitializer;
import com.google.enterprise.cloudsearch.sdk.RetryPolicy;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

/**
 * Helper class to serving for items indexed by connectors.
 *
 * This class is used to verify that indexed items are served correctly, something that is
 * particularly useful to verify that ACLs are set correctly by connectors and the SDK.
 *
 * Sample usage:
 *
 * <pre>
 *   SearchAuthInfo userAuthInfo =
 *       new SearchAuthInfo(clientSecrets, credentialsDirectory, userEmail);
 *   SearchHelper searchHelper = SearchHelper.createSearchHelper(
 *       userAuthInfo,
 *       searchApplicationId,
 *       Optional.of(rootUrl));
 *   SearchResponse response = searchHelper.serving(query);
 *   for (SearchResult result : response.getResults()) {
 *     // do something with each result
 *   }
 * </pre>
 */
public class SearchHelper {
  private static final String SEARCH_APPLICATION_NAME = "Cloud Search Mock Indexing Connector";
  private static final Set<String> API_SCOPES =
      Collections.singleton("https://www.googleapis.com/auth/cloud_search");
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final RetryRequestInitializer RETRY_REQUEST_INITIALIZER =
      new RetryRequestInitializer(new RetryPolicy.Builder().build());

  private final CloudSearch cloudSearch;
  private final String searchApplicationId;

  /**
   * Factory method for {@code SearchHelper} objects.
   *
   * @param searchAuthInfo object containing the info to authenticate the impersonated user
   * @param searchApplicationId ID of the serving application linked to the data sourced containing
   *  the items to serving (this is can be obtained from the Admin console)
   * @param rootUrl URL of the Indexing API
   */
  public static SearchHelper createSearchHelper(
      SearchAuthInfo searchAuthInfo, String searchApplicationId, Optional<String> rootUrl)
      throws GeneralSecurityException, IOException {
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    Credential credential = createCredentials(transport, searchAuthInfo);

    CloudSearch.Builder builder =
        new CloudSearch.Builder(
                transport,
                JSON_FACTORY,
                createChainedHttpRequestInitializer(credential, RETRY_REQUEST_INITIALIZER))
            .setApplicationName(SEARCH_APPLICATION_NAME);
    rootUrl.ifPresent(r -> builder.setRootUrl(r));
    return new SearchHelper(builder.build(), searchApplicationId);
  }

  public SearchResponse search(String query) throws IOException {
    SearchRequest searchRequest = new SearchRequest()
        .setQuery(query)
        .setRequestOptions(
            new RequestOptions()
                .setSearchApplicationId(searchApplicationId));
    return cloudSearch.query().search(searchRequest).execute();
  }

  private SearchHelper(CloudSearch cloudSearch, String searchApplicationId) {
    this.cloudSearch = cloudSearch;
    this.searchApplicationId = searchApplicationId;
  }

  private static Credential createCredentials(
      HttpTransport httpTransport, SearchAuthInfo searchAuthInfo)
      throws IOException {
    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
        JSON_FACTORY, new InputStreamReader(searchAuthInfo.getClientSecretsStream(), UTF_8));
    GoogleAuthorizationCodeFlow flow =
        new GoogleAuthorizationCodeFlow.Builder(
            httpTransport, JSON_FACTORY, clientSecrets, API_SCOPES)
            .setDataStoreFactory(new FileDataStoreFactory(searchAuthInfo.getCredentialsDirectory()))
            .build();
    return flow.loadCredential(searchAuthInfo.getUserEmail());
  }

  private static HttpRequestInitializer createChainedHttpRequestInitializer(
      HttpRequestInitializer... initializers) {
    return request -> {
      for (HttpRequestInitializer initializer : initializers) {
        if (initializer != null) {
          initializer.initialize(request);
        }
      }
    };
  }

  public static void main(String args[]) throws GeneralSecurityException, IOException {
    String nl = System.lineSeparator();
    if (args.length < 6) {
      System.err.println(
          "Usage: SearchHelper <secrets_path> <store_dir> <user_email> <root_url> <app_id>" + nl
          + "where" + nl
          + "  <secrets_path>: path to the clients secret JSON of the user doing the serving." + nl
          + "  <store_dir>: path to the directory with the stored credentials for the user." + nl
          + "  <user_email>: e-mail of the user performing the serving" + nl
          + "  <root_url>: URL of the Indexing API endpoint." + nl
          + "  <app_id>: ID of the serving application." + nl
          + "  <query>: Query for the items to serving." + nl
      );
      System.exit(1);
    }
    File clientSecrets = new File(args[0]);
    File credentialsDirectory = new File(args[1]);
    String userEmail = args[2];
    String rootUrl = args[3];
    String searchApplicationId = args[4];
    String query = args[5];
    SearchAuthInfo searchAuthInfo = new SearchAuthInfo(
        clientSecrets, credentialsDirectory, userEmail);
    SearchHelper searchHelper = SearchHelper.createSearchHelper(
        searchAuthInfo, searchApplicationId, Optional.of(rootUrl));
    SearchResponse response = searchHelper.search(query);
    long resultsCount = response.getResultCountExact();
    if (resultsCount == 0) {
      System.out.println("No results found.");
      return;
    }
    System.out.println("Found " + response.getResultCountExact() + " results:");
    for (SearchResult result : response.getResults()) {
      System.out.println(result.toPrettyString());
    }
  }
}
