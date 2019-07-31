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
package com.google.enterprise.cloudsearch.sdk.indexing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Generates an Item using {@link IndexingItemBuilder} with specified values for metadata.
 *
 * <pre>
 * MockItem item = new MockItem.Builder("ItemName").setTitle("Title").setMimeType("HTML")
 *     .setContentLanguage("en-us").setItemType(ItemType.CONTENT_ITEM.toString()).build();
 * </pre>
 */
public class MockItem {
  private static final String MIME_TYPE = "mimeType";
  private static final String ITEM_TYPE = "itemType";
  private static final String OBJECT_TYPE = "objectType";
  private static final String TITLE = "title";
  private static final String LANGUAGE = "language";
  private static final String HASH_VALUE = "hash";
  private static final String CONTAINER = "container";
  private static final String URL = "url";
  private static final String PAYLOAD = "payload";
  private static final String QUEUE = "queue";
  private static final String VERSION = "version";
  private static final String CREATE_TIME = "createTime";
  private static final String UPDATE_TIME = "updateTime";
  private static final String ACL = "acl";

  private final String name;
  private final Multimap<String, Object> values;

  public MockItem(String itemName, Multimap<String, Object> values) {
    this.name = itemName;
    this.values = ImmutableMultimap.copyOf(values);
  }

  public Item getItem() {
    return new IndexingItemBuilder(name)
        .setValues(values)
        .setTitle(FieldOrValue.withField(TITLE))
        .setSourceRepositoryUrl(FieldOrValue.withField(URL))
        .setItemType(getItemType(values))
        .setObjectType(FieldOrValue.withField(OBJECT_TYPE))
        .setMimeType(FieldOrValue.withField(MIME_TYPE))
        .setContainerName(FieldOrValue.withField(CONTAINER))
        .setContentLanguage(FieldOrValue.withField(LANGUAGE))
        .setCreateTime(FieldOrValue.withField(CREATE_TIME))
        .setUpdateTime(FieldOrValue.withField(UPDATE_TIME))
        .setHash(FieldOrValue.withField(HASH_VALUE))
        .setQueue(getSingleStringValue(values, QUEUE))
        .setVersion(getByteArrayValue(values, VERSION))
        .setPayload(getByteArrayValue(values, PAYLOAD))
        .setAcl(MockItem.<Acl>getSingleValue(values, ACL).orElse(null))
        .build();
  }

  private static ItemType getItemType(Multimap<String, Object> values) {
    String itemType = getSingleStringValue(values, ITEM_TYPE);
    return (itemType == null) ? ItemType.CONTENT_ITEM : ItemType.valueOf(itemType);
  }

  private static String getSingleStringValue(Multimap<String, Object> values, String key) {
    return MockItem.<String>getSingleValue(values, key).orElse(null);
  }

  private static byte[] getByteArrayValue(Multimap<String, Object> values, String key) {
    String value = getSingleStringValue(values, key);
    return value == null ? null : value.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * @throws ClassCastException when there is a mismatch between
   *         value in the map and expected type.
   */
  @SuppressWarnings("unchecked")
  private static <T> Optional<T> getSingleValue(Multimap<String, Object> values, String key) {
    checkNotNull(values, "values can not be null");
    checkArgument(!Strings.isNullOrEmpty(key), "lookup key can not be null or empty");
    return values
        .get(key)
        .stream()
        .filter(v -> v != null)
        .map(v -> (T) v)
        .findFirst();
  }

  /**
   * Builder class to set meta data attributes of an Item.
   */
  public static class Builder {
    private Multimap<String, Object> values = ArrayListMultimap.create();
    private String name;

    public Builder(String itemName) {
      this.name = itemName;
    }

    public Builder setTitle(String title) {
      values.put(TITLE, title);
      return this;
    }

    public Builder setItemType(String itemType) {
      values.put(ITEM_TYPE, itemType);
      return this;
    }

    public Builder setMimeType(String mimeType) {
      values.put(MIME_TYPE, mimeType);
      return this;
    }

    public Builder setObjectType(String objectType) {
      values.put(OBJECT_TYPE, objectType);
      return this;
    }

    public Builder setHash(String hash) {
      values.put(HASH_VALUE, hash);
      return this;
    }

    public Builder setContainerName(String container) {
      values.put(CONTAINER, container);
      return this;
    }

    public Builder setContentLanguage(String language) {
      values.put(LANGUAGE, language);
      return this;
    }

    public Builder setSourceRepositoryUrl(String url) {
      values.put(URL, url);
      return this;
    }

    public Builder setPayload(String payload) {
      values.put(PAYLOAD, payload);
      return this;
    }

    public Builder setQueue(String queue) {
      values.put(QUEUE, queue);
      return this;
    }

    public Builder setVersion(String version) {
      values.put(VERSION, version);
      return this;
    }

    public Builder setAcl(Acl acl) {
      values.put(ACL, acl);
      return this;
    }

    public Builder setCreateTime(String createTime) {
      values.put(CREATE_TIME, createTime);
      return this;
    }

    public Builder setUpdateTime(String updateTime) {
      values.put(UPDATE_TIME, updateTime);
      return this;
    }

    public Builder addValue(String key, Object value) {
      values.put(key, value);
      return this;
    }

    public MockItem build() {
      return new MockItem(name, values);
    }
  }
}


