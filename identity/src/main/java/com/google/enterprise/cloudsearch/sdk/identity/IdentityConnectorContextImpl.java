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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.enterprise.cloudsearch.sdk.ConnectorContextImpl;

/** Context for an {@link IdentityConnector} . */
class IdentityConnectorContextImpl extends ConnectorContextImpl
      implements IdentityConnectorContext {

  private final IdentityService identityService;

  private IdentityConnectorContextImpl(Builder builder) {
    super(builder);
    identityService = checkNotNull(builder.identityService);
  }

  public static class Builder extends AbstractBuilder<Builder, IdentityConnectorContext> {
    private IdentityService identityService;

    @Override
    protected Builder getThis() {
      return this;
    }

    public Builder setIdentityService(IdentityService identityService) {
      this.identityService = identityService;
      return this;
    }

    @Override
    public IdentityConnectorContext build() {
      return new IdentityConnectorContextImpl(this);
    }
  }

  /**
   * Returns Identity service instance.
   *
   * @return {@link IdentityService} instance
   */
  @Override
  public IdentityService getIdentityService() {
    return identityService;
  }
}
