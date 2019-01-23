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

/**
 * Common configuration properties for test case classes.
 */
public class TestProperties {
  // Credentials to authenticate as a service account. This is required by the connectors SDK and
  // also to programmatically manage test users and groups.
  public static final String SERVICE_KEY_PROPERTY_NAME =
      qualifyTestProperty("serviceAccountPrivateKeyFile");

  public static String qualifyTestProperty(String propertyName) {
    return "api.test." + propertyName;
  }
}