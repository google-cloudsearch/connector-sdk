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
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.api.services.cloudidentity.v1.model.Membership;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.cloudsearch.sdk.GroupIdEncoder;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/** Context used by {@link Repository} to initialize itself */
public class RepositoryContext {

  private final IdentitySourceConfiguration identitySourceConfiguration;
  private final ImmutableMap<String, IdentitySourceConfiguration>
      referenceIdentitySourceConfiguration;
  private final ImmutableMap<String, RepositoryContext> referenceRepositoryContext;

  private RepositoryContext(Builder builder) {
    this.identitySourceConfiguration =
        checkNotNull(
            builder.identitySourceConfiguration, "identity source configuration can not be null");
    this.referenceIdentitySourceConfiguration =
        checkNotNull(
            builder.referenceIdentitySourceConfiguration,
            "reference identity source configuration can not be null");
    this.referenceRepositoryContext =
        referenceIdentitySourceConfiguration
            .entrySet()
            .stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    e -> e.getKey(),
                    e ->
                        new RepositoryContext.Builder()
                            .setIdentitySourceConfiguration(e.getValue())
                            .build()));
  }

  public ImmutableMap<String, IdentitySourceConfiguration>
      getReferenceIdentitySourceConfiguration() {
    return referenceIdentitySourceConfiguration;
  }

  public IdentitySourceConfiguration getIdentitySourceConfiguration() {
    return identitySourceConfiguration;
  }

  public Optional<RepositoryContext> getRepositoryContextForReferenceIdentitySource(
      String referenceSourceName) {
    return Optional.ofNullable(referenceRepositoryContext.get(referenceSourceName));
  }

  public IdentityUser buildIdentityUser(String googleId, String externalId) {
    return new IdentityUser.Builder()
        .setAttribute(getIdentitySourceConfiguration().getIdentitySourceSchemaAttribute())
        .setSchema(getIdentitySourceConfiguration().getIdentitySourceSchema())
        .setGoogleIdentity(googleId)
        .setUserIdentity(externalId)
        .build();
  }

  public IdentityGroup buildIdentityGroup(
      String externalGroupId, Supplier<Set<Membership>> members) {
    return new IdentityGroup.Builder()
        .setMembers(members)
        .setGroupIdentity(externalGroupId)
        .setGroupKey(buildEntityKeyForGroup(externalGroupId))
        .build();
  }

  /**
   * Creates an {@link EntityKey} for given external group identifier. This method uses {@link
   * GroupIdEncoder#encodeGroupId} to escape unsupported characters from identifier.
   *
   * @param externalGroupId to build {@link EntityKey}
   * @return EntityKey for provided externalGroupId
   */
  public EntityKey buildEntityKeyForGroup(String externalGroupId) {
    return new EntityKey()
        .setId(GroupIdEncoder.encodeGroupId(externalGroupId))
        .setNamespace(getIdentitySourceConfiguration().getGroupNamespace());
  }

  public static RepositoryContext fromConfiguration() {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    return new Builder()
        .setIdentitySourceConfiguration(IdentitySourceConfiguration.fromConfiguration())
        .setReferenceIdentitySourceConfiguration(
            IdentitySourceConfiguration.getReferenceIdentitySourcesFromConfiguration())
        .build();
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        identitySourceConfiguration,
        referenceIdentitySourceConfiguration,
        referenceRepositoryContext);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof RepositoryContext)) {
      return false;
    }
    RepositoryContext other = (RepositoryContext) obj;
    return Objects.equals(identitySourceConfiguration, other.identitySourceConfiguration)
        && Objects.equals(
            referenceIdentitySourceConfiguration, other.referenceIdentitySourceConfiguration)
        && Objects.equals(referenceRepositoryContext, other.referenceRepositoryContext);
  }

  static class Builder {
    private IdentitySourceConfiguration identitySourceConfiguration;
    private ImmutableMap<String, IdentitySourceConfiguration> referenceIdentitySourceConfiguration =
        ImmutableMap.of();

    protected Builder setIdentitySourceConfiguration(
        IdentitySourceConfiguration identitySourceConfiguration) {
      this.identitySourceConfiguration = identitySourceConfiguration;
      return this;
    }

    protected Builder setReferenceIdentitySourceConfiguration(
        ImmutableMap<String, IdentitySourceConfiguration> referenceIdentitySourceConfiguration) {
      this.referenceIdentitySourceConfiguration = referenceIdentitySourceConfiguration;
      return this;
    }

    RepositoryContext build() {
      return new RepositoryContext(this);
    }
  }
}
