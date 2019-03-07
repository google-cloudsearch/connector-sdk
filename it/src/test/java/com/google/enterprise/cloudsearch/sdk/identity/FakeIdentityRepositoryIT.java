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

import static com.google.common.base.Verify.verify;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.SERVICE_KEY_PROPERTY_NAME;
import static com.google.enterprise.cloudsearch.sdk.TestProperties.qualifyTestProperty;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import com.google.api.services.admin.directory.model.User;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

// Truth's assertWithMessage() allows to specify better descriptions when assertions fail,
// but since it requires Guava >= 25.1 and the SDK uses 23, sticking to the simpler
// assertThat() functionality for now.

/**
 * Tests to check the integration between the identity connector SDK and the Cloud Identity API.
 */
@RunWith(Parameterized.class)
public class FakeIdentityRepositoryIT {

  private static final Logger logger = Logger.getLogger(FakeIdentityRepositoryIT.class.getName());
  // The ID of the Cloud Identity source to manage groups. E.g., "01ba7beab007dbb4b97383f3dc456c03".
  private static final String IDENTITY_SOURCE_ID_PROPERTY_NAME =
      qualifyTestProperty("identitySourceId");
  // Where to find it: In Google Admin console, Security > Setup single sing-on > value of idpid
  // in SSO URL.
  // ID of the customer associated with the
  private static final String CUSTOMER_ID_PROPERTY_NAME = qualifyTestProperty("customerId");
  /// Domain associated with the customer ID above.
  private static final String DOMAIN_PROPERTY_NAME = qualifyTestProperty("domain");
  private static final String ADMIN_USER_ID_PROPERTY_NAME =
      qualifyTestProperty("adminUserId");
  // A random user prefix to avoid clashes if multiple instances of the test are
  // run concurrently.
  private static final String USERNAME_PREFIX_FORMAT =
      format("auto-test-user-%%d-%s", UUID.randomUUID().toString());
  private static final String GROUP_PREFIX_FORMAT =
      format("auto-test-group-%%d-%s", UUID.randomUUID().toString());
  private static final long CONNECTOR_EXECUTION_TIMEOUT_MINS = 5;
  private static final long REPOSITORY_CLOSED_POLL_INTERVAL_SECS = 5;

  private static File serviceKeyFile;
  private static String adminUserId;
  private static String customerId;
  private static String identitySourceId;

  @Parameter
  public FakeIdentityRepository fakeIdRepo;

  @Rule
  public TemporaryFolder configFolder = new TemporaryFolder();
  // If the configuration is not explicitly reset, it'll persist across tests
  // (see Application.initConfig()), so wipe it out after each test is run.
  @Rule
  public ResetConfigRule resetConfig = new ResetConfigRule();

  private DirectoryFacade dirFacade;
  private File configPath;
  private CloudIdentityFacade cloudIdentityFacade;

  @Parameters
  public static Object[] data() {
    String domain = System.getProperty(DOMAIN_PROPERTY_NAME);
    verify(!Strings.isNullOrEmpty(domain));
    return new FakeIdentityRepository[] {
        new FakeIdentityRepository.Builder(domain)
            .addSnapshot(
                new String[][] {generateUserNames(1, 5)},
                new String[][] {generateGroupNames(1, 5)})
            .addSnapshot(
                new String[][] {generateUserNames(2, 7)},
                new String[][] {generateGroupNames(2, 7)})
            .build()
    };
  }

  @BeforeClass
  public static void readTestProperties() {
    logger.info("Reading test properties...");
    adminUserId = System.getProperty(ADMIN_USER_ID_PROPERTY_NAME);
    verify(!Strings.isNullOrEmpty(adminUserId));
    String serviceKeyPath = System.getProperty(SERVICE_KEY_PROPERTY_NAME);
    verify(!Strings.isNullOrEmpty(serviceKeyPath));
    serviceKeyFile = new File(serviceKeyPath);
    verify(serviceKeyFile.exists());
    customerId = System.getProperty(CUSTOMER_ID_PROPERTY_NAME);
    verify(!Strings.isNullOrEmpty(customerId));
    identitySourceId = System.getProperty(IDENTITY_SOURCE_ID_PROPERTY_NAME);
    verify(!Strings.isNullOrEmpty(identitySourceId));

  }

  @Before
  public void setUp() throws GeneralSecurityException, IOException {
    enableFineLoggingOnAllLoggers();
    setUpUsers();
    setUpGroups();
    setUpPropertiesFile();
  }

  @After
  public void cleanUp() throws InterruptedException, IOException {
    cleanUpUsers();
    cleanUpGroups();
  }

  @Test
  public void testSyncingUsersAndGroups() throws InterruptedException, IOException {
    String[] args = {"-Dconfig=" + configPath};
    while (fakeIdRepo.hasMoreSnapshots()) {
      logger.info("Grabbing the next snapshot...");

      FakeIdentityRepository.Snapshot snapshot = fakeIdRepo.nextSnapshot();
      IdentityApplication application =
          new IdentityApplication.Builder(
              new FullSyncIdentityConnector(fakeIdRepo), args)
              .build();
      logger.info("Starting identity application...");
      application.start();

      logger.info("Waiting for the identity application to shut down...");
      await()
          .atMost(CONNECTOR_EXECUTION_TIMEOUT_MINS, MINUTES)
          .pollInterval(REPOSITORY_CLOSED_POLL_INTERVAL_SECS, SECONDS)
          .until(() -> fakeIdRepo.isClosed());

      ImmutableSet<String> allSnapshotUserKeys = snapshot.getAllUserNames();
      ImmutableSet<String> allSnapshotGroupIds = snapshot.getAllGroupIds();

      logger.info("Checking existing users...");
      for (String userKey : allSnapshotUserKeys) {
        String userEmail = FakeIdentityRepository.buildUserEmail(fakeIdRepo.getDomain(), userKey);
        String externalId = FakeIdentityRepository
            .buildExternalIdentity(fakeIdRepo.getDomain(), userKey);
        Object entry = getSchemaValueForUser(userEmail);
        assertEquals(externalId, entry);
        logger.log(Level.FINE, "User {0} is good.", userEmail);
      }
      logger.info("Checking removed users...");
      for (String userKey : fakeIdRepo.getRemovedUserNames()) {
        String userEmail = FakeIdentityRepository.buildUserEmail(fakeIdRepo.getDomain(), userKey);
        Object entry = getSchemaValueForUser(userEmail);
        assertEquals("", entry);
        logger.log(Level.FINE, "Removed user {0} was synced correctly.", userEmail);
      }
      logger.info("Checking existing groups...");
      ImmutableSet<String> existingGroupIds = cloudIdentityFacade.listAllGroupIds();
      Set<String> difference = new HashSet<String>(allSnapshotGroupIds);
      difference.removeAll(existingGroupIds);
      assertThat(difference, not(hasItem(anything())));
      logger.info("Checking removed groups...");
      Set<String> intersection = new HashSet<String>(existingGroupIds);
      intersection.retainAll(fakeIdRepo.getRemovedGroupIds());
      assertThat(intersection, not(hasItem(anything())));
    }
  }

  /**
   * Generates an array of formatted strings suffixed from {@code min} to {@code max}, inclusive.
   *
   * @param formatter - formatter function called to generate each string.
   */
  private static String[] generateStrings(int min, int max, IntFunction<String> formatter) {
    return IntStream
        .rangeClosed(min, max)
        .mapToObj(formatter)
        .toArray(String[]::new);
  }

  /**
   * Generates a {@code String[]} with mock user names suffixed from {@code min} to {@code max},
   * inclusive.
   */
  private static String[] generateUserNames(int min, int max) {
    return generateStrings(min, max, i -> format(USERNAME_PREFIX_FORMAT, i));
  }

  /**
   * Generates a {@code String[]} with mock group names suffixed from {@code min} to {@code max},
   * inclusive.
   */
  private static String[] generateGroupNames(int min, int max) {
    return generateStrings(min, max, i -> format(GROUP_PREFIX_FORMAT, i));
  }

  /**
   * Enables fine logging on all loggers to make it easier to diagnose failures from logs.
   */
  private void enableFineLoggingOnAllLoggers() {
    logger.info("Enabling fine logging...");
    Logger rootLogger = LogManager.getLogManager().getLogger("");
    rootLogger.setLevel(Level.FINE);
    for (Handler h : rootLogger.getHandlers()) {
      h.setLevel(Level.FINE);
    }
  }

  private void setUpUsers() throws IOException, GeneralSecurityException {
    logger.info("Setting up users...");
    dirFacade = DirectoryFacade.create(
        new FileInputStream(serviceKeyFile),
        adminUserId,
        fakeIdRepo.getDomain());
    dirFacade.createUsers(fakeIdRepo.getAllUserEmails());
  }

  private void setUpGroups()
      throws IOException, GeneralSecurityException {
    logger.info("Setting up groups...");
    cloudIdentityFacade = CloudIdentityFacade.create(
        identitySourceId, new FileInputStream(serviceKeyFile),
        FakeIdentityRepositoryIT.class.getName());
    Set<String> intersection = new HashSet<String>(cloudIdentityFacade.listAllGroupIds());
    intersection.retainAll(fakeIdRepo.getAllGroupIds());
    assertThat(intersection, not(hasItem(anything())));
  }

  private void setUpPropertiesFile() throws IOException {
    logger.info("Setting up properties file...");
    // Set up the properties.
    configPath = configFolder.newFile();
    try (PrintWriter configWriter = new PrintWriter(configPath)) {
      configWriter.println("api.serviceAccountPrivateKeyFile=" + serviceKeyFile);
      configWriter.println("api.customerId=" + customerId);
      configWriter.println("api.identitySourceId=" + identitySourceId);
      configWriter.println("connector.runOnce=true");
      configWriter.println("schedule.performTraversalOnStart=true");
    }
  }

  private void cleanUpUsers() throws IOException, InterruptedException {
    logger.info("Cleaning up users...");
    if (dirFacade != null) {
      dirFacade.deleteExistingUsers(fakeIdRepo.getAllUserEmails());
    }
  }

  private void cleanUpGroups() throws IOException, InterruptedException {
    logger.info("Cleaning up groups...");
    if (cloudIdentityFacade != null) {
      cloudIdentityFacade.deleteExistingGroups(fakeIdRepo.getAllGroupNames());
    }
  }

  private Object getSchemaValueForUser(String userEmail) throws IOException {
    final String idKey = identitySourceId + "_identifier";
    User user = dirFacade.fetchUser(userEmail);
    Map<String, Map<String, Object>> schemas = user.getCustomSchemas();

    assertNotNull(schemas);
    assertThat(schemas.keySet(), hasItem(identitySourceId));
    Map<String, Object> sourceIdMap = schemas.get(identitySourceId);
    assertThat(sourceIdMap.keySet(), hasItem(idKey));
    return sourceIdMap.get(idKey);
  }
}
