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

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableList;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Description of the items expected to be indexed.
 *
 * This spec can be used by tests and tools to describe the items that are
 * expected to have been indexed by a connector. The spec also provides
 * information on how to check the validity of the items ingested (e.g.,
 * it specifies credentials to use to verify they are serving correctly).
 */
@JsonDeserialize(builder = Spec.Builder.class)
public final class Spec {
  private static final ObjectMapper MAPPER = createObjectMapper();

  private final List<SpecItem> items;

  private Spec(Builder builder) {
    items = ImmutableList.copyOf(builder.items);
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof Spec && Objects.deepEquals(items, ((Spec) obj).items);
  }

  public static void toJson(Spec spec, String jsonPath) throws IOException {
    MAPPER.writeValue(new File(jsonPath), spec);
  }

  public static Spec fromJson(String jsonPath) throws IOException {
    return MAPPER.readValue(new File(jsonPath), Spec.class);
  }

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    mapper.setSerializationInclusion(Include.NON_NULL);
    mapper.setSerializationInclusion(Include.NON_EMPTY);
    mapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    return mapper;
  }

  /**
   * Builder of Spec objects.
   */
  @JsonPOJOBuilder(withPrefix = "set")
  public static class Builder {
    private List<SpecItem> items = Collections.emptyList();

    public Builder setItems(List<SpecItem> items) {
      checkNotNull(items, "items may not be null");
      this.items = items;
      return this;
    }

    public Spec build() {
      return new Spec(this);
    }
  }
}
