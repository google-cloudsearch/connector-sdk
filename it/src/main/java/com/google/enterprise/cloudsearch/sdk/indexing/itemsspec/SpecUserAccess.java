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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * List of users expected to have (or not) access to an item.
 */
@JsonDeserialize(builder = SpecUserAccess.Builder.class)
public class SpecUserAccess {
  private final List<String> allowed;
  private final List<String> denied;

  private SpecUserAccess(Builder builder) {
    this.allowed = ImmutableList.copyOf(builder.allowed);
    this.denied = ImmutableList.copyOf(builder.denied);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SpecUserAccess) {
      SpecUserAccess other = (SpecUserAccess) object;
      return Objects.equals(allowed, other.allowed)
          && Objects.equals(denied, other.denied);
    }
    return false;
  }

  public List<String> getAllowed() {
    return allowed;
  }

  public List<String> getDenied() {
    return denied;
  }

  /**
   * Builder of SpecUserAccess objects.
   */
  @JsonPOJOBuilder(withPrefix = "set")
  public static class Builder {
    private List<String> allowed = Collections.emptyList();
    private List<String> denied = Collections.emptyList();

    public Builder setAllowed(List<String> allowed) {
      checkNotNull(allowed, "allowed may not be null");
      this.allowed = allowed;
      return this;
    }

    public Builder setDenied(List<String> denied) {
      checkNotNull(denied, "denied may not be null");
      this.denied = denied;
      return this;
    }

    public SpecUserAccess build() {
      return new SpecUserAccess(this);
    }
  }
}
