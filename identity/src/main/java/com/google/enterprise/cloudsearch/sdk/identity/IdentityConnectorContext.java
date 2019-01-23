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

import com.google.enterprise.cloudsearch.sdk.ConnectorContext;

/**
 * Interface for a context object created by the SDK to pass to the {@link IdentityConnector} code.
 *
 * <p>The {@link IdentityApplication} object creates a context instance containing an
 * {@link IdentityService} instance and
 * {@link com.google.enterprise.cloudsearch.sdk.ExceptionHandler} instances for the connector
 * to access. It calls the
 * {@link IdentityConnector#init(IdentityConnectorContext)} method to pass the context to
 * the connector code.
 */
public interface IdentityConnectorContext extends ConnectorContext {
  /**
   * Returns the {@link IdentityService} instance used to communicate with the Cloud Search API.
   *
   * @return an {@link IdentityService} instance
   */
  IdentityService getIdentityService();
}
