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
import com.google.common.base.Objects;

/**
 * Description of the expected metadata in an indexed item.
 */
@JsonDeserialize(builder = SpecMetadata.Builder.class)
public class SpecMetadata {
  private final String title;
  private final String sourceRepositoryUrl;
  private final String mimeType;
  private final String contentLanguage;

  private SpecMetadata(Builder builder) {
    title = builder.title;
    sourceRepositoryUrl = builder.sourceRepositoryUrl;
    mimeType = builder.mimeType;
    contentLanguage = builder.contentLanguage;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof SpecMetadata) {
      SpecMetadata other = (SpecMetadata) object;
      return Objects.equal(title, other.title)
          && Objects.equal(sourceRepositoryUrl, other.sourceRepositoryUrl)
          && Objects.equal(mimeType, other.mimeType)
          && Objects.equal(contentLanguage, other.contentLanguage);
    }
    return false;
  }

  public String getTitle() {
    return title;
  }

  public String getSourceRepositoryUrl() {
    return sourceRepositoryUrl;
  }

  public String getMimeType() {
    return mimeType;
  }

  public String getContentLanguage() {
    return contentLanguage;
  }

  /**
   * Builder of SpecMetadata objects.
   */
  @JsonPOJOBuilder(withPrefix = "set")
  public static class Builder {
    private String title;
    private String sourceRepositoryUrl;
    private String mimeType;
    private String contentLanguage;

    public Builder setTitle(String title) {
      checkNotNull(title, "title may not be null");
      this.title = title;
      return this;
    }

    public Builder setSourceRepositoryUrl(String url) {
      checkNotNull(url, "url may not be null");
      this.sourceRepositoryUrl = url;
      return this;
    }

    public Builder setMimeType(String mimeType) {
      checkNotNull(mimeType, "mimeType may not be null");
      this.mimeType = mimeType;
      return this;
    }

    public Builder setContentLanguage(String contentLanguage) {
      checkNotNull(contentLanguage, "contentLanguage may not be null");
      this.contentLanguage = contentLanguage;
      return this;
    }

    public SpecMetadata build() {
      return new SpecMetadata(this);
    }
  }
}
