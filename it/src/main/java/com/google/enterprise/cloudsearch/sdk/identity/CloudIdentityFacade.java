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
package com.google.enterprise.cloudsearch.sdk.identity;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudidentity.v1beta1.CloudIdentity;
import com.google.api.services.cloudidentity.v1beta1.CloudIdentity.Groups;
import com.google.api.services.cloudidentity.v1beta1.model.EntityKey;
import com.google.api.services.cloudidentity.v1beta1.model.Group;
import com.google.api.services.cloudidentity.v1beta1.model.SearchGroupsResponse;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.enterprise.cloudsearch.sdk.BaseApiService.RetryRequestInitializer;
import com.google.enterprise.cloudsearch.sdk.RetryPolicy;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Simple interface to the Cloud Identity service.
 */
public class CloudIdentityFacade {

  private static final Logger logger = Logger.getLogger(CloudIdentityFacade.class.getName());
  private static final int GROUPS_MAX_BASIC_PAGE_SIZE = 1000;
  private static final String GROUPS_BASIC_VIEW = "BASIC";

  public static final String CLOUD_IDENTITY_SCOPE =
      "https://www.googleapis.com/auth/cloud-identity";
  public static final String DEFAULT_GROUP_LABEL = "system/groups/external";

  private final CloudIdentity service;
  private final String identitySource;

  /**
   * Lists the IDs for all the groups in this identity source.
   *
   * @return the set of IDs (e.g., "domain/group").
   */
  ImmutableSet<String> listAllGroupIds() throws IOException {
    return listAllGroups()
        .stream()
        .map(g -> g.getGroupKey().getId())
        .collect(ImmutableSet.toImmutableSet());
  }

  /**
   * Creates the specified groups.
   *
   * @param groupIds the list of group IDs in the format "domain/group".
   */
  private void createGroups(ImmutableSet<String> groupIds) throws IOException {
    checkState(Sets.intersection(groupIds, listAllGroupIds()).isEmpty());
    for (String groupId : groupIds) {
      logger.fine(format("Creating group \"%s\"...", groupId));
      EntityKey groupKey = new EntityKey()
          .setId(groupId)
          .setNamespace("identitysources/" + identitySource);
      Group group = new Group()
          .setGroupKey(groupKey)
          .setLabels(ImmutableMap.of(DEFAULT_GROUP_LABEL, ""))
          .setParent(groupKey.getNamespace())
          .setDisplayName(groupId);
      service.groups()
          .create(group)
          .execute();
    }
  }

  /**
   * Deletes the existing groups from those specified.
   *
   * E.g., if {@code groupIds} is {@code {"dom/g1", "dom/g2"}} but only {@code dom/g2} exists, only
   * {@code dom/g2} is deleted.
   *
   * Implementation note: groups are deleted serially, so deleting more than a few tens of groups
   * will take a lot of time.
   */
  void deleteExistingGroups(ImmutableSet<String> groupIds) throws IOException {
    ImmutableSet<Group> groupsForDeletion = listAllGroups()
        .stream()
        .filter(group -> groupIds.contains(group.getDisplayName()))
        .collect(ImmutableSet.toImmutableSet());
    for (Group group : groupsForDeletion) {
      logger.fine(format("Deleting group \"%s\"...", group.getGroupKey().getId()));
      service.groups()
          .delete(group.getName())
          .execute();
    }
  }

  private CloudIdentityFacade(String identitySource, CloudIdentity service) {
    this.identitySource = identitySource;
    this.service = service;
  }

  private static Credential authorize(
      InputStream serviceKeyStream) throws IOException {

    return GoogleCredential
        .fromStream(serviceKeyStream)
        .createScoped(ImmutableSet.of(CLOUD_IDENTITY_SCOPE));
  }

  static CloudIdentityFacade create(
      String identitySource, InputStream serviceKeyStream, String appName)
      throws GeneralSecurityException, IOException {
    Credential credential = authorize(serviceKeyStream);
    // Google services are rate-limited. The RetryPolicy allows to rety when a
    // 429 HTTP status response (Too Many Requests) is received.
    RetryPolicy retryPolicy = new RetryPolicy.Builder().build();
    RetryRequestInitializer requestInitializer = new RetryRequestInitializer(retryPolicy);
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
    CloudIdentity.Builder serviceBuilder = new CloudIdentity.Builder(transport, jsonFactory,
        request -> {
          credential.initialize(request);
          requestInitializer.initialize(request);
        });
    serviceBuilder.setApplicationName(appName).build();
    CloudIdentity service = serviceBuilder.build();
    return new CloudIdentityFacade(identitySource, service);
  }

  private Groups.Search makeGroupsSearch(String pageToken) throws IOException {
    String query = format(
        "namespace = identitysources/%s AND labels:system/groups/external",
        identitySource);
    return service
        .groups()
        .search()
        .setQuery(query)
        .setPageToken(pageToken)
        .setView(GROUPS_BASIC_VIEW)
        .setPageSize(GROUPS_MAX_BASIC_PAGE_SIZE);
  }

  private ImmutableSet<Group> listAllGroups() throws IOException {
    Groups.Search search = makeGroupsSearch(null);
    Set<Group> allGroups = new HashSet<>();
    do {
      logger.fine("Fetching the next page of groups...");
      SearchGroupsResponse response = search.execute();
      List<Group> groups = response.getGroups();
      if (groups != null) {
        logger.fine(format("Fetched %d groups.", groups.size()));
        allGroups.addAll(groups);
      }
      String nextPageToken = response.getNextPageToken();
      logger.fine(format("Next groups page token is \"%s\".", nextPageToken));
      if (nextPageToken == null) {
        // That was the last page.
        break;
      }
      search = makeGroupsSearch(nextPageToken);
    } while (search != null);
    logger.fine(format("Fetched a total of %d groups.", allGroups.size()));
    return ImmutableSet.copyOf(allGroups);
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    if (args.length < 3) {
      System.err.println(
          "Wrong number of arguments." + System.lineSeparator()
              + "Usage: " + System.lineSeparator()
              + "  List: CloudIdentityFacade <path/to/key> <id-source> l" + System.lineSeparator()
              + "  Delete: CloudIdentityFacade <path/to/key> <id-source> d "
              + "<comma-separated-group-ids-list>" + System.lineSeparator()
              + "  Create: CloudIdentityFacade <path/to/key> <id-source> c "
              + "<comma-separated-group-ids-list>" + System.lineSeparator());
      System.exit(1);
    }

    String keyPath = args[0];
    String idSource = args[1];
    String command = args[2].toLowerCase();
    ImmutableSet<String> groupIds =
        args.length > 3 ? ImmutableSet.copyOf(args[3].split(",")) : null;

    CloudIdentityFacade cloudIdentityFacade = CloudIdentityFacade.create(
        idSource, new FileInputStream(new File(keyPath)), "CloudIdentityFacadeCLI");

    if (command.startsWith("l")) {
      System.out.println("Listing groups...");
      System.out.println(
          String.join(System.lineSeparator(), cloudIdentityFacade.listAllGroupIds()));
    } else if (command.startsWith("d")) {
      System.out.println("Deleting groups...");
      cloudIdentityFacade.deleteExistingGroups(groupIds);
    } else if (command.startsWith("c")) {
      System.out.println("Creating groups...");
      cloudIdentityFacade.createGroups(groupIds);
    } else {
      System.err.println(format("Invalid command \"%s\".", command));
      System.exit(1);
    }
  }
}
