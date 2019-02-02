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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.api.services.cloudidentity.v1.model.Group;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Structure to hold configuration information about identity sources, schemas and group namespace.
 */
public class IdentitySourceConfiguration {
  private static final String IDENTITY_SOURCE_CONFIG = "api.identitySourceId";
  private static final String IDENTITY_SOURCE_SCHEMA_CONFIG = "api.identitySourceSchema";
  private static final String IDENTITY_SOURCE_SCHEMA_ATTRIBUTE_CONFIG =
      "api.identitySourceSchemaAttribute";
  private static final String REFERENCE_IDENTITY_SOURCES_CONFIG = "api.referenceIdentitySources";
  private static final String REFERENCE_IDENTITY_SOURCE_ID_CONFIG =
      "api.referenceIdentitySource.%s.id";
  private static final String REFERENCE_IDENTITY_SOURCE_SCHEMA_CONFIG =
      "api.referenceIdentitySource.%s.schema";
  private static final String REFERENCE_IDENTITY_SOURCE_SCHEMA_ATTRIBUTE_CONFIG =
      "api.referenceIdentitySource.%s.attribute";

  private static final String GROUP_NS_FORMAT = "identitysources/%s";
  private static final String SCHEMA_ATTRIBUTE_FORMAT = "%s_identifier";

  private final String identitySourceId;
  private final String identitySourceSchema;
  private final String identitySourceSchemaAttribute;
  private final String groupNamespace;

  private IdentitySourceConfiguration(IdentitySourceConfiguration.Builder builder) {
    checkNotNull(builder, "builder can not be null");
    checkArgument(
        !Strings.isNullOrEmpty(builder.identitySourceId),
        "Identity Source ID can not be null or empty");
    checkArgument(
        !Strings.isNullOrEmpty(builder.identitySourceSchema),
        "Identity Source schema can not be null or empty");
    checkArgument(
        !Strings.isNullOrEmpty(builder.identitySourceSchemaAttribute),
        "Identity Source schema attribute can not be null or empty");
    this.identitySourceId = builder.identitySourceId;
    this.identitySourceSchema = builder.identitySourceSchema;
    this.identitySourceSchemaAttribute = builder.identitySourceSchemaAttribute;
    this.groupNamespace = String.format(GROUP_NS_FORMAT, identitySourceId);
  }

  /** Gets schema attribute name to be populated for user identity mapping. */
  public String getIdentitySourceSchemaAttribute() {
    return identitySourceSchemaAttribute;
  }

  /** Gets namespace to be used to create {@link Group} under. */
  public String getGroupNamespace() {
    return groupNamespace;
  }

  /** Gets schema name to be populated for user identity mapping. */
  public String getIdentitySourceSchema() {
    return identitySourceSchema;
  }

  /** Gets identity source ID under which user and group identities will be mapped. */
  public String getIdentitySourceId() {
    return identitySourceId;
  }

  /**
   * Construct primary {@link IdentitySourceConfiguration} from Configuration.
   *
   * @return {@link IdentitySourceConfiguration} as per connector configuration
   */
  public static IdentitySourceConfiguration fromConfiguration() {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    String identitySource = Configuration.getString(IDENTITY_SOURCE_CONFIG, null).get();
    String identitySourceSchema =
        Configuration.getString(IDENTITY_SOURCE_SCHEMA_CONFIG, identitySource).get();
    String identitySourceSchemaAttribute =
        Configuration.getString(
                IDENTITY_SOURCE_SCHEMA_ATTRIBUTE_CONFIG,
                String.format(SCHEMA_ATTRIBUTE_FORMAT, identitySourceSchema))
            .get();
    return new Builder(identitySource)
        .setIdentitySourceSchema(identitySourceSchema)
        .setIdentitySourceSchemaAttribute(identitySourceSchemaAttribute)
        .build();
  }

  /**
   * Construct primary {@link IdentitySourceConfiguration} from Configuration.
   *
   * @return {@link IdentitySourceConfiguration} as per connector configuration
   */
  private static IdentitySourceConfiguration forReferenceIdentitySource(String identitySourceName) {
    String identitySource =
        Configuration.getString(
                String.format(REFERENCE_IDENTITY_SOURCE_ID_CONFIG, identitySourceName), null)
            .get();
    String identitySourceSchema =
        Configuration.getString(
                String.format(REFERENCE_IDENTITY_SOURCE_SCHEMA_CONFIG, identitySource),
                identitySource)
            .get();
    String identitySourceSchemaAttribute =
        Configuration.getString(
                String.format(REFERENCE_IDENTITY_SOURCE_SCHEMA_ATTRIBUTE_CONFIG, identitySource),
                String.format(SCHEMA_ATTRIBUTE_FORMAT, identitySourceSchema))
            .get();
    return new Builder(identitySource)
        .setIdentitySourceSchema(identitySourceSchema)
        .setIdentitySourceSchemaAttribute(identitySourceSchemaAttribute)
        .build();
  }

  /**
   * Construct map of reference {@link IdentitySourceConfiguration} from Configuration. Each
   * referenced identity source is identified by associated identity source name from configuration
   * file.
   *
   * @return {@link IdentitySourceConfiguration} as per connector configuration
   */
  public static ImmutableMap<String, IdentitySourceConfiguration>
      getReferenceIdentitySourcesFromConfiguration() {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    List<String> configurations = Configuration.getMultiValue(
            REFERENCE_IDENTITY_SOURCES_CONFIG, Collections.emptyList(), Configuration.STRING_PARSER)
        .get();
    return configurations
        .stream()
        .filter(s -> !Strings.isNullOrEmpty(s))
        .collect(ImmutableMap.toImmutableMap(k -> k, v -> forReferenceIdentitySource(v)));
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        identitySourceId, identitySourceSchema, identitySourceSchemaAttribute, groupNamespace);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof IdentitySourceConfiguration)) {
      return false;
    }
    IdentitySourceConfiguration other = (IdentitySourceConfiguration) obj;
    return Objects.equals(identitySourceId, other.identitySourceId)
        && Objects.equals(identitySourceSchema, other.identitySourceSchema)
        && Objects.equals(identitySourceSchemaAttribute, other.identitySourceSchemaAttribute)
        && Objects.equals(groupNamespace, other.groupNamespace);
  }

  /** Builder for creating an instance of {@link IdentitySourceConfiguration} */
  public static class Builder {
    private String identitySourceId;
    private String identitySourceSchema;
    private String identitySourceSchemaAttribute;

    /** Constructs builder for creating an instance of {@link IdentitySourceConfiguration} */
    public Builder(String identitySourceId) {
      this.identitySourceId = identitySourceId;
      this.identitySourceSchema = identitySourceId;
      this.identitySourceSchemaAttribute = String.format(SCHEMA_ATTRIBUTE_FORMAT, identitySourceId);
    }

    /** Sets schema name to be populated for user identity mapping. */
    public Builder setIdentitySourceSchema(String identitySourceSchema) {
      this.identitySourceSchema = identitySourceSchema;
      return this;
    }

    /** Sets schema attribute name to be populated for user identity mapping. */
    public Builder setIdentitySourceSchemaAttribute(String identitySourceSchemaAttribute) {
      this.identitySourceSchemaAttribute = identitySourceSchemaAttribute;
      return this;
    }

    /** Builds an instance of {@link IdentitySourceConfiguration} */
    public IdentitySourceConfiguration build() {
      return new IdentitySourceConfiguration(this);
    }
  }
}