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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.file.Files.newInputStream;
import static java.util.Locale.ENGLISH;

import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.api.client.util.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.enterprise.cloudsearch.sdk.config.ConfigValue;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.Collection;

/**
 * Factory to create a {@link GoogleCredential} object for accessing the Cloud Search API.
 *
 * <p>Required configuration parameters:
 *
 * <ul>
 * <li>{@code api.serviceAccountPrivateKeyFile} - Specifies the service account private key file
 * path.
 * </ul>
 *
 * <p>Optional configuration parameters:
 *
 * <ul>
 * <li>{@code api.serviceAccountId} - Specifies the service account ID. If {@code
 * api.serviceAccountPrivateKeyFile} is not a json key, then {@code api.serviceAccountId} is
 * required.
 * </ul>
 */
public class LocalFileCredentialFactory implements CredentialFactory {

  /**
   * Configuration key for service account ID.
   */
  public static final String SERVICE_ACCOUNT_ID_CONFIG = "api.serviceAccountId";
  /**
   * Configuration key for service account key file path.
   */
  public static final String SERVICE_ACCOUNT_KEY_FILE_CONFIG = "api.serviceAccountPrivateKeyFile";

  private static CredentialHelper credentialHelper = new CredentialHelper();
  private final String serviceAccountId;
  private final Path serviceAccountKeyFilePath;
  private final boolean isJsonKey;
  private final GoogleProxy proxy;

  /**
   * Method to build an instance of {@link LocalFileCredentialFactory} from configuration.
   *
   * @return an instance of {@link LocalFileCredentialFactory}
   */
  public static LocalFileCredentialFactory fromConfiguration() {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    ConfigValue<String> serviceAccountKeyFilePath =
        Configuration.getString(LocalFileCredentialFactory.SERVICE_ACCOUNT_KEY_FILE_CONFIG, null);
    ConfigValue<String> serviceAccountId =
        Configuration.getString(LocalFileCredentialFactory.SERVICE_ACCOUNT_ID_CONFIG, "");

    return new Builder()
        .setServiceAccountKeyFilePath(serviceAccountKeyFilePath.get())
        .setServiceAccountId(serviceAccountId.get())
        .setProxy(GoogleProxy.fromConfiguration())
        .build();
  }

  /**
   * Gets {@link GoogleCredential} instance constructed for service account.
   */
  @Override
  public GoogleCredential getCredential(Collection<String> scopes)
      throws GeneralSecurityException, IOException {

    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    HttpTransport transport = proxy.getHttpTransport();
    if (!isJsonKey) {
      return credentialHelper.getP12Credential(
          serviceAccountId,
          serviceAccountKeyFilePath,
          transport,
          jsonFactory,
          proxy.getHttpRequestInitializer(),
          scopes);
    }
    return credentialHelper.getJsonCredential(
        serviceAccountKeyFilePath,
        transport,
        jsonFactory,
        proxy.getHttpRequestInitializer(),
        scopes);
  }

  private LocalFileCredentialFactory(Builder builder) {
    this.serviceAccountId = builder.serviceAccountId;
    this.serviceAccountKeyFilePath = builder.serviceAccountKeyFilePath;
    this.isJsonKey = builder.isJsonKey;
    this.proxy = builder.proxy;
  }

  @VisibleForTesting
  static class CredentialHelper {

    GoogleCredential getJsonCredential(
        Path keyPath,
        HttpTransport transport,
        JsonFactory jsonFactory,
        HttpRequestInitializer httpRequestInitializer,
        Collection<String> scopes)
        throws IOException {
      try (InputStream is = newInputStream(keyPath)) {
        GoogleCredential credential =
            GoogleCredential.fromStream(is, transport, jsonFactory).createScoped(scopes);
        GoogleCredential.Builder builder =
            new GoogleCredential.Builder()
                .setServiceAccountId(credential.getServiceAccountId())
                .setServiceAccountScopes(scopes)
                .setServiceAccountPrivateKey(credential.getServiceAccountPrivateKey())
                .setTransport(transport)
                .setJsonFactory(jsonFactory)
                .setRequestInitializer(httpRequestInitializer);
        return new ProxyGoogleCredential(builder);
      }
    }

    GoogleCredential getP12Credential(
        String serviceAccountId,
        Path keyPath,
        HttpTransport transport,
        JsonFactory jsonFactory,
        HttpRequestInitializer requestInitializer,
        Collection<String> scopes)
        throws GeneralSecurityException, IOException {
      GoogleCredential.Builder builder =
          new GoogleCredential.Builder()
              .setServiceAccountId(serviceAccountId)
              .setServiceAccountScopes(scopes)
              .setServiceAccountPrivateKeyFromP12File(keyPath.toFile())
              .setTransport(transport)
              .setJsonFactory(jsonFactory)
              .setRequestInitializer(requestInitializer);
      return new ProxyGoogleCredential(builder);
    }
  }

  @VisibleForTesting
  static void setCredentialHelper(CredentialHelper helper) {
    credentialHelper = helper;
  }

  /**
   * Builder for creating instance of {@link LocalFileCredentialFactory}.
   */
  public static class Builder {

    private String serviceAccountId;
    private String serviceAccountKeyFile;
    private Path serviceAccountKeyFilePath;
    private boolean isJsonKey = true;
    private GoogleProxy proxy = new GoogleProxy.Builder().build();

    /**
     * Sets service account ID for creating {@link GoogleCredential}.
     *
     * @param serviceAccountId to be used for creating {@link GoogleCredential}.
     */
    public Builder setServiceAccountId(String serviceAccountId) {
      this.serviceAccountId = serviceAccountId;
      return this;
    }

    /**
     * Sets service account key file path for creating {@link GoogleCredential}.
     *
     * @param serviceAccountKeyFile path to be used for creating {@link GoogleCredential}.
     */
    public Builder setServiceAccountKeyFilePath(String serviceAccountKeyFile) {
      this.serviceAccountKeyFile = serviceAccountKeyFile;
      this.serviceAccountKeyFilePath = Paths.get(this.serviceAccountKeyFile);
      return this;
    }

    /**
     * Sets {@link GoogleProxy} for creating {@link GoogleCredential}.
     *
     * @param proxy to be used for creating {@link GoogleCredential}.
     */
    public Builder setProxy(GoogleProxy proxy) {
      this.proxy = proxy;
      return this;
    }

    /**
     * Builds an instance of {@link LocalFileCredentialFactory}.
     */
    public LocalFileCredentialFactory build() {
      checkNotNull(serviceAccountKeyFile, SERVICE_ACCOUNT_KEY_FILE_CONFIG + " is not specified.");
      checkArgument(!serviceAccountKeyFile.isEmpty());
      checkArgument(
          Files.exists(serviceAccountKeyFilePath), serviceAccountKeyFile + " does not exist.");
      checkArgument(
          !Files.isDirectory(serviceAccountKeyFilePath),
          serviceAccountKeyFile + "is a directory. A file is expected");
      if (!serviceAccountKeyFilePath.toString().toLowerCase(ENGLISH).endsWith(".json")) {
        checkNotNull(serviceAccountId, SERVICE_ACCOUNT_ID_CONFIG + " is not specified");
        checkArgument(!serviceAccountId.isEmpty(), "service account Id is empty.");
        isJsonKey = false;
      }

      return new LocalFileCredentialFactory(this);
    }
  }

  private static class ProxyGoogleCredential extends GoogleCredential {
    public ProxyGoogleCredential(GoogleCredential.Builder builder) {
      super(builder);
    }

    /**
     * Note : This is local modification to GoogleCredential object implementation in order to
     * support setting proxy authorization header for token requests.
     */
    @Override
    @Beta
    protected TokenResponse executeRefreshToken() throws IOException {
      if (getServiceAccountPrivateKey() == null) {
        return super.executeRefreshToken();
      }
      // service accounts: no refresh token; instead use private key to request new access token
      JsonWebSignature.Header header = new JsonWebSignature.Header();
      header.setAlgorithm("RS256");
      header.setType("JWT");
      header.setKeyId(getServiceAccountPrivateKeyId());
      JsonWebToken.Payload payload = new JsonWebToken.Payload();
      long currentTime = getClock().currentTimeMillis();
      payload.setIssuer(getServiceAccountId());
      payload.setAudience(getTokenServerEncodedUrl());
      payload.setIssuedAtTimeSeconds(currentTime / 1000);
      payload.setExpirationTimeSeconds(currentTime / 1000 + 3600);
      payload.setSubject(getServiceAccountUser());
      payload.put("scope", Joiner.on(' ').join(getServiceAccountScopes()));
      try {
        String assertion =
            JsonWebSignature.signUsingRsaSha256(
                getServiceAccountPrivateKey(), getJsonFactory(), header, payload);
        TokenRequest request =
            new TokenRequest(
                getTransport(),
                getJsonFactory(),
                new GenericUrl(getTokenServerEncodedUrl()),
                "urn:ietf:params:oauth:grant-type:jwt-bearer");
        request.put("assertion", assertion);
        // Current implementation of GoogleCredential is not setting request initializer for token
        // requests.
        request.setRequestInitializer(getRequestInitializer());
        return request.execute();
      } catch (GeneralSecurityException exception) {
        IOException e = new IOException();
        e.initCause(exception);
        throw e;
      }
    }
  }
}
