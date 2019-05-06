/*
 * Copyright 2019 Google LLC
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

package com.google.enterprise.cloudsearch.sdk.indexing.itemsspec;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Description of an expected indexed item.
 */
@JsonDeserialize(builder = SpecItem.Builder.class)
public class SpecItem {
  private final String name;
  private final SpecMetadata metadata;
  private final Map<String, Object> structuredData;
  private final List<String> readers;
  private final SpecUserAccess userAccess;

  private SpecItem(Builder builder) {
    this.name = builder.name;
    this.metadata = builder.metadata;
    this.structuredData = ImmutableMap.copyOf(builder.structuredData);
    this.readers = ImmutableList.copyOf(builder.readers);
    this.userAccess = builder.userAccess;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SpecItem) {
      SpecItem other = (SpecItem) object;
      return Objects.equals(name, other.name)
          && Objects.equals(metadata, other.metadata)
          && Objects.equals(structuredData, other.structuredData)
          && Objects.equals(readers, other.readers)
          && Objects.equals(userAccess, other.userAccess);
    }
    return false;
  }

  public String getName() {
    return name;
  }

  public SpecMetadata getMetadata() {
    return metadata;
  }

  public Map<String, Object> getStructuredData() {
    return structuredData;
  }

  public List<String> getReaders() {
    return readers;
  }

  public SpecUserAccess getUserAccess() {
    return userAccess;
  }

  /**
   * Builder of SpecItem objects.
   */
  @JsonPOJOBuilder(withPrefix = "set")
  public static class Builder {
    private String name;
    private SpecMetadata metadata;
    private Map<String, Object> structuredData = Collections.emptyMap();
    private List<String> readers = Collections.emptyList();
    private SpecUserAccess userAccess;

    public Builder(@JsonProperty("name") String name) {
      checkNotNull(name, "name may not be null");
      this.name = name;
    }

    public Builder setMetadata(SpecMetadata metadata) {
      checkNotNull(metadata, "metadata may not be null");
      this.metadata = metadata;
      return this;
    }

    public Builder setStructuredData(Map<String, Object> structuredData) {
      checkNotNull(structuredData, "structuredData may not be null");
      this.structuredData = structuredData;
      return this;
    }

    public Builder setReaders(List<String> readers) {
      checkNotNull(readers, "readers may not be null");
      this.readers = readers;
      return this;
    }

    public Builder setUserAccess(
        SpecUserAccess userAccess) {
      checkNotNull(userAccess, "userAccess may not be null");
      this.userAccess = userAccess;
      return this;
    }

    public SpecItem build() {
      return new SpecItem(this);
    }
  }
}
