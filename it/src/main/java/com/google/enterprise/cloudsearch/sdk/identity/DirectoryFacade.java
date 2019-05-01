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
import static com.google.common.base.Verify.verify;
import static java.lang.String.format;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.admin.directory.DirectoryScopes;
import com.google.api.services.admin.directory.model.User;
import com.google.api.services.admin.directory.model.UserName;
import com.google.api.services.admin.directory.model.Users;
import com.google.common.collect.ImmutableList;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * A facade for Google's Directory API
 * (https://developers.google.com/admin-sdk/directory/v1/reference/).
 */
public class DirectoryFacade {

  private static final Logger logger = Logger.getLogger(DirectoryFacade.class.getName());
  private static final String USER_PROJECTION_FULL = "full";
  // The max documented in
  // https://developers.google.com/admin-sdk/directory/v1/reference/users/list.
  private static final Integer NUM_USERS_PER_PAGE = 500;
  private static final List<String> ADMIN_SCOPES = ImmutableList.of(
      DirectoryScopes.ADMIN_DIRECTORY_USER,
      DirectoryScopes.ADMIN_DIRECTORY_GROUP);

  private final Directory directory;
  private final String domain;

  private DirectoryFacade(Directory directory, String domain) {
    this.directory = directory;
    this.domain = domain;
  }

  /**
   * Creates the specified users in the Directory service.
   *
   * This method attempts to creates the users transactionally (all or none), but this can't be
   * guaranteed (e.g., if there're connection issues).
   *
   * Implementation note: users are created serially, so adding more than a few tens of users will
   * take a lot of time.
   *
   * @throws {@link com.google.common.base.VerifyException} if a user can't be created.
   * @throws IllegalStateException if any of the users already exist (which may indicate that
   * another test is using the same username).
   */
  void createUsers(ImmutableSet<String> userEmails) throws IOException {
    ImmutableSet<String> allDomainEmails = listAllEmailsInDomain();
    checkState(Sets.intersection(userEmails, allDomainEmails).isEmpty());
    try {
      for (String userEmail : userEmails) {
        createUser(userEmail);
      }
    } catch (Exception e) {
      logger.log(Level.SEVERE, "Not all of the users were created: {0}", e.getMessage());
      logger.info("Attempting to delete all the users created...");
      // deleteExistingUsers() may fail, in which case it'll throw an exception and override the
      // original one. Unfortunately, nothing helpful can be done about that at this point.
      deleteExistingUsers(userEmails);
      throw e;
    }
  }

  /**
   * Deletes the existing users from those specified.
   *
   * E.g., if {@code userEmails} is {@code {"u1@dom.com", "u2@dom.com"}} but only {@code u2@dom.com}
   * exists, only {@code u2@dom.com} is deleted.
   *
   * Implementation note: users are deleted serially, so deleting more than a few tens of users will
   * take a lot of time.
   */
  void deleteExistingUsers(ImmutableSet<String> userEmails) throws IOException {
    ImmutableSet<String> existingUserEmails = Sets.intersection(listAllEmailsInDomain(), userEmails)
        .immutableCopy();
    for (String userEmail : existingUserEmails) {
      deleteUser(userEmail);
    }
  }

  /**
   * Fetches a user's details from the Cloud Identity Service.
   */
  User fetchUser(String userEmail) throws IOException {
    logger.info("Fetching user " + userEmail);
    User user = directory
        .users()
        .get(userEmail)
        .setProjection(USER_PROJECTION_FULL)
        .execute();
    logger.fine("Fetched user " + user.toPrettyString());
    return user;
  }

  /**
   * Creates a user in the Cloud Identity service.
   *
   * The mandatory fields password, first, and last name are set to the user's email.
   */
  private void createUser(String userEmail) throws IOException {
    User user = new User()
        .setPrimaryEmail(userEmail)
        .setPassword(userEmail)
        .setName(new UserName()
            .setGivenName("Fake Given Name")
            .setFamilyName("Fake Family Name"));
    logger.info("Creating user " + userEmail);
    user = directory
        .users()
        .insert(user)
        .execute();
    verify(user != null);
    logger.fine("Created user " + user.toPrettyString());
  }

  /**
   * Deletes a user (an exception is thrown if the user doesn't exist).
   */
  private void deleteUser(String userEmail) throws IOException {
    logger.info("Deleting user " + userEmail);
    directory
        .users()
        .delete(userEmail)
        .execute();
    logger.fine("Deleted user " + userEmail);
  }

  /**
   * Gets a list of all the e-mails in the domain.
   */
  private ImmutableSet<String> listAllEmailsInDomain() throws IOException {
    Set<String> allUserIds = new HashSet<>();
    String nextPageToken = null;
    do {
      logger.fine("Fetching the next page of users...");
      Users users = directory
          .users()
          .list()
          .setMaxResults(NUM_USERS_PER_PAGE)
          .setDomain(domain)
          .setPageToken(nextPageToken)
          .execute();
      List<User> userList = users.getUsers();
      if (userList != null) {
        logger.log(Level.FINE, "Fetched {0} users.", userList.size());
        allUserIds.addAll(
            userList
                .stream()
                .map(User::getPrimaryEmail)
                .collect(Collectors.toSet()));
      }
      nextPageToken = users.getNextPageToken();
      logger.log(Level.FINE, "Next users page token is \"{0}\".", nextPageToken);
    } while (nextPageToken != null);
    logger.log(Level.FINE, "Fetched a total of {0} users.", allUserIds.size());
    return ImmutableSet.copyOf(allUserIds);
  }

  /**
   * Builder for DirectoryFacade objects.
   *
   * @param serviceKeyStream {@link InputStream} for the JSON file containing the service account
   *   key to authenticate with the Cloud Identity service.
   * @param adminEmail the email of the domain's admin account
   * @param domain the organization's domain
   */

  static DirectoryFacade create(
      InputStream serviceKeyStream, String adminEmail, String domain)
      throws IOException, GeneralSecurityException {
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    GoogleCredential credential = GoogleCredential
        .fromStream(serviceKeyStream)
        .createScoped(ADMIN_SCOPES);
    Credential adminCredential = new GoogleCredential.Builder()
        .setTransport(httpTransport)
        .setJsonFactory(jsonFactory)
        .setServiceAccountId(credential.getServiceAccountId())
        .setServiceAccountPrivateKey(credential.getServiceAccountPrivateKey())
        .setServiceAccountScopes(ADMIN_SCOPES)
        .setServiceAccountUser(adminEmail)
        .build();
    // Google services are rate-limited. The RetryPolicy allows to rety when a
    // 429 HTTP status response (Too Many Requests) is received.
    RetryPolicy retryPolicy = new RetryPolicy.Builder().build();
    RetryRequestInitializer requestInitializer = new RetryRequestInitializer(retryPolicy);
    Directory.Builder directoryBuilder = new Directory.Builder(
        httpTransport, jsonFactory, request -> {
      adminCredential.initialize(request);
      requestInitializer.initialize(request);
    });
    Directory directory = directoryBuilder.build();
    return new DirectoryFacade(directory, domain);
  }

  public static void main(String[] args) throws IOException, GeneralSecurityException {
    String nl = System.lineSeparator();
    String commonParams = "<admin_email> <path/to/key.json> <domain>";

    if (args.length < 3) {
      System.err.println(
          "Wrong number of arguments." + nl
              + "Usage: " + nl
              + "  List: DirectoryFacade " + commonParams + " l" + nl
              + "  Delete: DirectoryFacade " + commonParams + " d "
              + "<comma-separated-emails-list>" + nl
              + "  Create: DirectoryFacade " + commonParams + " c "
              + "<comma-separated-emails-list>" + nl);
    }

    String adminEmail = args[0];
    String keyPath = args[1];
    String domain = args[2];
    String command = args[3].toLowerCase();
    ImmutableSet<String> emails = args.length > 4 ? ImmutableSet.copyOf(args[4].split(",")) : null;

    DirectoryFacade directoryFacade = DirectoryFacade.create(
        new FileInputStream(new File(keyPath)), adminEmail, domain);

    if (command.startsWith("l")) {
      System.out.println("Listing all users in the domain...");
      System.out.println(
          String.join(System.lineSeparator(), directoryFacade.listAllEmailsInDomain()));
    } else if (command.startsWith("d")) {
      System.out.println("Deleting users...");
      directoryFacade.deleteExistingUsers(emails);
    } else if (command.startsWith("c")) {
      System.out.println("Creating users...");
      directoryFacade.createUsers(emails);
    } else {
      System.err.println(format("Invalid command \"%s\".", command));
      System.exit(1);
    }
  }
}
