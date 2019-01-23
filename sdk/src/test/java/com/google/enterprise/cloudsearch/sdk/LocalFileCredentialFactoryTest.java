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
package com.google.enterprise.cloudsearch.sdk;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.LocalFileCredentialFactory.CredentialHelper;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link LocalFileCredentialFactory}. */

@RunWith(MockitoJUnitRunner.class)
public class LocalFileCredentialFactoryTest {

  private static final String SERVICE_ACCOUNT_FILE_P12 = "service_account.p12";
  private static final String SERVICE_ACCOUNT_FILE_JSON = "service_account.json";

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock CredentialHelper mockCredentialHelper;

  @Before
  public void setUp() {
    LocalFileCredentialFactory.setCredentialHelper(mockCredentialHelper);
  }

  @Test
  public void testNullKeyPath() throws Exception {
    thrown.expect(NullPointerException.class);
    new LocalFileCredentialFactory.Builder()
        .setServiceAccountKeyFilePath(null)
        .setServiceAccountId("123456")
        .build();
  }

  @Test
  public void testEmptyKeyPath() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    new LocalFileCredentialFactory.Builder()
        .setServiceAccountKeyFilePath("")
        .setServiceAccountId("123456")
        .build();
  }

  @Test
  public void testNullServiceAccountIdP12() throws Exception {
    File tmpfile =
        temporaryFolder.newFile("testNullServiceAccountIdP12" + SERVICE_ACCOUNT_FILE_P12);
    thrown.expect(NullPointerException.class);
    LocalFileCredentialFactory subject =
        new LocalFileCredentialFactory.Builder()
            .setServiceAccountKeyFilePath(tmpfile.getAbsolutePath())
            .setServiceAccountId(null)
            .build();
    subject.getCredential(Arrays.asList("email"));
  }

  @Test
  public void testNullServiceAccountIdJson() throws Exception {
    File tmpfile =
        temporaryFolder.newFile("testNullServiceAccountIdJson" + SERVICE_ACCOUNT_FILE_JSON);
    GoogleCredential mockCredential = new MockGoogleCredential.Builder().build();
    List<String> scopes = Arrays.asList("profile");
    when(mockCredentialHelper.getJsonCredential(
        eq(tmpfile.toPath()),
        any(HttpTransport.class),
        any(JsonFactory.class),
        any(HttpRequestInitializer.class),
        eq(scopes)))
        .thenReturn(mockCredential);
    LocalFileCredentialFactory subject =
        new LocalFileCredentialFactory.Builder()
            .setServiceAccountKeyFilePath(tmpfile.getAbsolutePath())
            .build();
    subject.getCredential(Arrays.asList("email"));
    assertEquals(mockCredential, subject.getCredential(scopes));
  }

  @Test
  public void testEmptyServiceAccountIdP12() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("service account Id is empty."));
    File tmpfile =
        temporaryFolder.newFile("testEmptyServiceAccountIdP12" + SERVICE_ACCOUNT_FILE_P12);
    new LocalFileCredentialFactory.Builder()
        .setServiceAccountKeyFilePath(tmpfile.getAbsolutePath())
        .setServiceAccountId("")
        .build();
  }

  @Test
  public void testInvalidPathServiceAccountIdP12() throws Exception {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString(SERVICE_ACCOUNT_FILE_P12));
    new LocalFileCredentialFactory.Builder()
        .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_P12)
        .setServiceAccountId("")
        .build();
  }

  @Test
  public void testGetCredentialJsonKey() throws Exception {
    File tmpfile = temporaryFolder.newFile(SERVICE_ACCOUNT_FILE_JSON);
    GoogleCredential mockCredential = new MockGoogleCredential.Builder().build();
    List<String> scopes = Arrays.asList("profile");
    when(mockCredentialHelper.getJsonCredential(
        eq(tmpfile.toPath()),
        any(HttpTransport.class),
        any(JsonFactory.class),
        any(HttpRequestInitializer.class),
        eq(scopes)))
        .thenReturn(mockCredential);
    LocalFileCredentialFactory credentialFactory =
        new LocalFileCredentialFactory.Builder()
            .setServiceAccountKeyFilePath(tmpfile.getAbsolutePath())
            .setServiceAccountId("ServiceAccountID")
            .build();
    assertEquals(mockCredential, credentialFactory.getCredential(scopes));
  }

  @Test
  public void testGetCredentialP12() throws Exception {
    File tmpfile = temporaryFolder.newFile(SERVICE_ACCOUNT_FILE_P12);
    GoogleCredential mockCredential = new MockGoogleCredential.Builder().build();
    List<String> scopes = Arrays.asList("profile, calendar");
    when(mockCredentialHelper.getP12Credential(
        eq("ServiceAccount"),
        eq(tmpfile.toPath()),
        any(HttpTransport.class),
        any(JsonFactory.class),
        any(HttpRequestInitializer.class),
        eq(scopes)))
        .thenReturn(mockCredential);
    LocalFileCredentialFactory credentialFactory =
        new LocalFileCredentialFactory.Builder()
            .setServiceAccountKeyFilePath(tmpfile.getAbsolutePath())
            .setServiceAccountId("ServiceAccount")
            .build();
    assertEquals(mockCredential, credentialFactory.getCredential(scopes));
  }

  @Test
  public void testfromConfigurationJsonKey() throws Exception {
    File tmpfile = temporaryFolder.newFile("testfromConfiguration" + SERVICE_ACCOUNT_FILE_JSON);
    createConfig(tmpfile.getAbsolutePath(), "");
    GoogleCredential mockCredential = new MockGoogleCredential.Builder().build();
    List<String> scopes = Arrays.asList("profile");
    when(mockCredentialHelper.getJsonCredential(
        eq(tmpfile.toPath()),
        any(HttpTransport.class),
        any(JsonFactory.class),
        any(HttpRequestInitializer.class),
        eq(scopes)))
        .thenReturn(mockCredential);
    LocalFileCredentialFactory credentialFactory = LocalFileCredentialFactory.fromConfiguration();
    assertEquals(mockCredential, credentialFactory.getCredential(scopes));
  }

  @Test
  public void testfromConfigurationCredentialP12() throws Exception {
    File tmpfile = temporaryFolder
        .newFile("testfromConfigurationCredentialP12" + SERVICE_ACCOUNT_FILE_P12);
    createConfig(tmpfile.getAbsolutePath(), "ServiceAccount");
    GoogleCredential mockCredential = new MockGoogleCredential.Builder().build();
    List<String> scopes = Arrays.asList("profile, calendar");
    when(mockCredentialHelper.getP12Credential(
        eq("ServiceAccount"),
        eq(tmpfile.toPath()),
        any(HttpTransport.class),
        any(JsonFactory.class),
        any(HttpRequestInitializer.class),
        eq(scopes)))
        .thenReturn(mockCredential);
    LocalFileCredentialFactory credentialFactory = LocalFileCredentialFactory.fromConfiguration();
    assertEquals(mockCredential, credentialFactory.getCredential(scopes));
  }

  @Test
  public void testfromConfigurationNotInitialized() throws Exception {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(containsString("configuration not initialized"));
    LocalFileCredentialFactory.fromConfiguration();
  }

  private void createConfig(String keyFile, String accountId) {
    Properties properties = new Properties();
    if (!Strings.isNullOrEmpty(keyFile)) {
      properties.put(LocalFileCredentialFactory.SERVICE_ACCOUNT_KEY_FILE_CONFIG, keyFile);
    }

    if (!Strings.isNullOrEmpty(accountId)) {
      properties.put(LocalFileCredentialFactory.SERVICE_ACCOUNT_ID_CONFIG, accountId);
    }
    setupConfig.initConfig(properties);
  }
}
