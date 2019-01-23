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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;

/**
 * Factory to create a {@link GoogleCredential} object for accessing the Cloud Search API.
 */
public interface CredentialFactory {

  /**
   * Creates a {@link GoogleCredential} object for accessing the Cloud Search API.
   *
   * @param scopes the OAuth 2.0 scope requirements to access the Cloud Search API
   * @return a fully built {@link GoogleCredential}
   */
  public GoogleCredential getCredential(Collection<String> scopes)
      throws GeneralSecurityException, IOException;
}
