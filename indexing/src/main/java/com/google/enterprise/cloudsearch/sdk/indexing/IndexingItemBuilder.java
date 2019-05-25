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

import com.google.api.client.util.DateTime;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.ItemStructuredData;
import com.google.api.services.cloudsearch.v1.model.SearchQualityMetadata;
import com.google.common.base.Converter;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * Helper object to build an {@link Item}.
 *
 * <p>Use the setters to provide all desired attributes of an {@link Item} including the name, ACL,
 * metadata fields, queue, version, and so on. To set the attribute’s value explicitly at build time
 * (value) or derive it from the repository’s map of key/values (field), define the metadata fields
 * as {@link FieldOrValue} objects.
 *
 * <p>Sample usage:
 * <pre>{@code
 *   // within Repository method fetching a document
 *   Multimap<String, Object> multiMapValues = ... // populate the map with repository data
 *   String documentName = ... // create the specific document name (unique id)
 *   String documentTitle = ... // set title to a "field" in multiMapValues
 *   ...
 *   IndexingItemBuilder indexingItemBuilder =
 *       IndexingItemBuilder.fromConfiguration(documentName).setValues(multiMapValues);
 *   ...
 *   // the title is now set to the value of the title field during the build()
 *   indexingItemBuilder.setTitle(FieldOrValue.withField(documentTitle));
 *   ...
 *   // the URL is now set to the hard-coded URL string
 *   indexingItemBuilder.setSourceRepositoryUrl(FieldOrValue.withValue("https://www.mycompany.com");
 *   ...
 *   // generate the fully built document
 *   Item documentItem = indexingItemBuilder.build();
 *   ...
 *   }
 * </pre>
 */
public class IndexingItemBuilder {
  private static final Logger logger = Logger.getLogger(IndexingItemBuilder.class.getName());

  public enum ItemType {
    CONTENT_ITEM,
    CONTAINER_ITEM,
    VIRTUAL_CONTAINER_ITEM
  }

  private static final String MIME_TYPE = "itemMetadata.mimeType";
  private static final String TITLE = "itemMetadata.title";
  private static final String SOURCE_REPOSITORY_URL = "itemMetadata.sourceRepositoryUrl";
  private static final String UPDATE_TIME = "itemMetadata.updateTime";
  private static final String CREATE_TIME = "itemMetadata.createTime";
  private static final String CONTENT_LANGUAGE = "itemMetadata.contentLanguage";
  private static final String HASH = "itemMetadata.hash";
  private static final String CONTAINER_NAME = "itemMetadata.containerName";
  private static final String SEARCH_QUALITY_METADATA_QUALITY =
      "itemMetadata.searchQualityMetadata.quality";
  public static final String OBJECT_TYPE = "itemMetadata.objectType";

  public static final String MIME_TYPE_FIELD = MIME_TYPE + ".field";
  public static final String TITLE_FIELD = TITLE + ".field";
  public static final String SOURCE_REPOSITORY_URL_FIELD = SOURCE_REPOSITORY_URL + ".field";
  public static final String UPDATE_TIME_FIELD = UPDATE_TIME + ".field";
  public static final String CREATE_TIME_FIELD = CREATE_TIME + ".field";
  public static final String CONTENT_LANGUAGE_FIELD = CONTENT_LANGUAGE + ".field";
  public static final String HASH_FIELD = HASH + ".field";
  public static final String CONTAINER_NAME_FIELD = CONTAINER_NAME + ".field";
  public static final String SEARCH_QUALITY_METADATA_QUALITY_FIELD =
      SEARCH_QUALITY_METADATA_QUALITY + ".field";
  public static final String OBJECT_TYPE_FIELD = OBJECT_TYPE + ".field";

  public static final String MIME_TYPE_VALUE = MIME_TYPE + ".defaultValue";
  public static final String TITLE_VALUE = TITLE + ".defaultValue";
  public static final String SOURCE_REPOSITORY_URL_VALUE = SOURCE_REPOSITORY_URL + ".defaultValue";
  public static final String UPDATE_TIME_VALUE = UPDATE_TIME + ".defaultValue";
  public static final String CREATE_TIME_VALUE = CREATE_TIME + ".defaultValue";
  public static final String CONTENT_LANGUAGE_VALUE = CONTENT_LANGUAGE + ".defaultValue";
  public static final String HASH_VALUE = HASH + ".defaultValue";
  public static final String CONTAINER_NAME_VALUE = CONTAINER_NAME + ".defaultValue";
  public static final String SEARCH_QUALITY_METADATA_QUALITY_VALUE =
      SEARCH_QUALITY_METADATA_QUALITY + ".defaultValue";
  public static final String OBJECT_TYPE_VALUE = OBJECT_TYPE + ".defaultValue";

  // These methods are not used above so that the constants will be detected as such by javadoc.
  private static String dotField(String configKey) {
    return configKey + ".field";
  }

  private static String dotValue(String configKey) {
    return configKey + ".defaultValue";
  }

  private final String name;
  private Acl acl;
  private Multimap<String, Object> values;

  private FieldOrValue<String> mimeType;
  private FieldOrValue<String> title;
  private FieldOrValue<String> url;
  private FieldOrValue<DateTime> updateTime;
  private FieldOrValue<DateTime> createTime;
  private FieldOrValue<String> language;
  private FieldOrValue<String> hash;
  private FieldOrValue<String> containerName;
  private FieldOrValue<Double> searchQualityMetadataQuality;
  private FieldOrValue<String> objectType;

  private final Optional<FieldOrValue<String>> configMimeType;
  private final Optional<FieldOrValue<String>> configTitle;
  private final Optional<FieldOrValue<String>> configUrl;
  private final Optional<FieldOrValue<DateTime>> configUpdateTime;
  private final Optional<FieldOrValue<DateTime>> configCreateTime;
  private final Optional<FieldOrValue<String>> configLanguage;
  private final Optional<FieldOrValue<String>> configHash;
  private final Optional<FieldOrValue<String>> configContainerName;
  private final Optional<FieldOrValue<Double>> configSearchQualityMetadataQuality;
  private final Optional<FieldOrValue<String>> configObjectType;

  private SearchQualityMetadata searchQuality;
  private String queue;
  private byte[] payload;
  private byte[] version;
  private ItemType itemType;

  /**
   * Constructs an {@code IndexingItemBuilder} from the {@link Configuration}.
   *
   * <p>Optional configuration parameters for {@code ItemMetadata}:
   *
   * <ul>
   *   <li>{@code itemMetadata.mimeType.field} - The key for the mimeType field in the values map.
   *   <li>{@code itemMetadata.title.field} - The key for the title field in the values map.
   *   <li>{@code itemMetadata.sourceRepositoryUrl.field} - The key for the URL field in the
   *       values map.
   *   <li>{@code itemMetadata.updateTime.field} - The key for the update time field in the
   *       values map.
   *   <li>{@code itemMetadata.createTime.field} - The key for the create time field in the
   *       values map.
   *   <li>{@code itemMetadata.contentLanguage.field} - The key for the content language field
   *       in the values map.
   *   <li>{@code itemMetadata.hash.field} - The key for the hash field in the values map.
   *   <li>{@code itemMetadata.containerName.field} - The key for the container name field
   *       in the values map.
   *   <li>{@code itemMetadata.searchQualityMetadata.quality.field} - The key for the
   *       SearchQualityMetadata quality field in the values map.
   *   <li>{@code itemMetadata.mimeType.defaultValue} - The value for the media type.
   *   <li>{@code itemMetadata.title.defaultValue} - The value for the title.
   *   <li>{@code itemMetadata.sourceRepositoryUrl.defaultValue} - The value for the URL.
   *   <li>{@code itemMetadata.updateTime.defaultValue} - The value for the update time in
   *       RFC 3339 format.
   *   <li>{@code itemMetadata.createTime.defaultValue} - The value for the create time in
   *       RFC 3339 format.
   *   <li>{@code itemMetadata.contentLanguage.defaultValue} - The value for the content language.
   *   <li>{@code itemMetadata.hash.defaultValue} - The value for the hash.
   *   <li>{@code itemMetadata.containerName.defaultValue} - The value for the container name.
   *   <li>{@code itemMetadata.searchQualityMetadata.quality.defaultValue} - The value for
   *       the SearchQualityMetadata quality.
   * </ul>
   *
   * <p>Optional configuration parameters for {@code ItemMetadata} and {@code StructuredData}:
   *
   * <ul>
   *   <li>{@code itemMetadata.objectType.field} - The key for the object type field in the
   *       values map.
   *   <li>{@code itemMetadata.objectType.defaultValue} - The value for the object type.
   * </ul>
   *
   * Note: For each {@code ItemMetadata} field, check the following in order for a non-empty value:
   * <ol>
   *   <li> A call to the correponding setter method on the returned
   *       instance of {@code IndexingItemBuilder}.
   *   <li> A config property with a suffix of {@code .field}, used as
   *       a key into the the {@link #setValues values map}.
   *   <li> A config property with a suffix of {@code .defaultValue}.
   * </ol>
   */
  public static IndexingItemBuilder fromConfiguration(String name) {
    String objectType = Configuration.getString(OBJECT_TYPE, "").get();
    if (!objectType.isEmpty()) {
      logger.log(Level.WARNING, "{0} is deprecated, use {1}={2}.",
          new Object[] { OBJECT_TYPE, OBJECT_TYPE_VALUE, objectType });
    }

    ConfigDefaults config = new ConfigDefaults()
        .setMimeType(fieldOrValue(MIME_TYPE, Configuration.STRING_PARSER))
        .setTitle(fieldOrValue(TITLE, Configuration.STRING_PARSER))
        .setUrl(fieldOrValue(SOURCE_REPOSITORY_URL, Configuration.STRING_PARSER))
        .setUpdateTime(fieldOrValue(UPDATE_TIME, DATE_PARSER))
        .setCreateTime(fieldOrValue(CREATE_TIME, DATE_PARSER))
        .setLanguage(fieldOrValue(CONTENT_LANGUAGE, Configuration.STRING_PARSER))
        .setHash(fieldOrValue(HASH, Configuration.STRING_PARSER))
        .setContainerName(fieldOrValue(CONTAINER_NAME, Configuration.STRING_PARSER))
        .setSearchQualityMetadataQuality(fieldOrValue(SEARCH_QUALITY_METADATA_QUALITY,
                Configuration.DOUBLE_PARSER))
        .setObjectType(
            fieldOrValue(
                OBJECT_TYPE,
                Configuration.getString(OBJECT_TYPE_FIELD, "").get(),
                Configuration.getOverriden(
                    OBJECT_TYPE_VALUE, Configuration.getString(OBJECT_TYPE, ""))
                    .get(),
                Configuration.STRING_PARSER));
    return new IndexingItemBuilder(name, config);
  }

  /**
   * Constructs an empty {@code IndexingItemBuilder}.
   */
  public IndexingItemBuilder(String name) {
    this(name, new ConfigDefaults());
  }

  private IndexingItemBuilder(String name, ConfigDefaults config) {
    this.name = name;
    this.values = ArrayListMultimap.create();

    this.configMimeType = config.mimeType;
    this.configTitle = config.title;
    this.configUrl = config.url;
    this.configUpdateTime = config.updateTime;
    this.configCreateTime = config.createTime;
    this.configLanguage = config.language;
    this.configHash = config.hash;
    this.configContainerName = config.containerName;
    this.configSearchQualityMetadataQuality = config.searchQualityMetadataQuality;
    this.configObjectType = config.objectType;
  }

  /**
   * Sets the {@link Acl} instance, which is used to construct the {@code ItemAcl}.
   *
   * @param acl the {@code Acl} instance
   * @return this instance
   */
  public IndexingItemBuilder setAcl(Acl acl) {
    this.acl = acl;
    return this;
  }

  /**
   * Sets the name of the object definition from the schema to use when
   * constructing the {@code ItemStructuredData}, either from
   * the given field (or key) in the {@code values} multimap, or a literal value.
   *
   * @param objectType the source of the object definition name
   * @return this instance
   */
  public IndexingItemBuilder setObjectType(FieldOrValue<String> objectType) {
    this.objectType = objectType;
    return this;
  }

  /**
   * Sets the name of the object definition from the schema to use when
   * constructing the {@code ItemStructuredData}
   *
   * @param objectType the object definition name
   * @return this instance
   * @deprecated Use {@link #setObjectType(FieldOrValue)} with {@link FieldOrValue#withValue}
   */
  @Deprecated
  public IndexingItemBuilder setObjectType(String objectType) {
    return setObjectType(FieldOrValue.withValue(objectType));
  }

  /**
   * Sets the {@code mimeType} field value for the {@code ItemMetadata}, either from
   * the given field (or key) in the {@code values} multimap, or a literal value.
   *
   * @param mimeType the source of the media type
   * @return this instance
   */
  public IndexingItemBuilder setMimeType(FieldOrValue<String> mimeType) {
    this.mimeType = mimeType;
    return this;
  }

  /**
   * Sets the {@code mimeType} field value for the {@code ItemMetadata}.
   *
   * @param mimeType a media type, such as "application/pdf"
   * @return this instance
   * @deprecated Use {@link #setMimeType(FieldOrValue)} with {@link FieldOrValue#withValue}
   */
  @Deprecated
  public IndexingItemBuilder setMimeType(String mimeType) {
    return setMimeType(FieldOrValue.withValue(mimeType));
  }

  /**
   * Sets the repository attributes that may be used for the {@code ItemMetadata}
   * or {@code StructuredDataObject} fields, depending on the {@code FieldOrValue}
   * setters called by the connector as well as the configuration. The map may
   * have repeated values for a key.
   *
   * @param values the repository attribute values
   * @return this instance
   */
  public IndexingItemBuilder setValues(Multimap<String, Object> values) {
    this.values = values;
    return this;
  }

  /**
   * Sets the {@code title} field value for the {@code ItemMetadata}, either from
   * the given field (or key) in the {@code values} multimap, or a literal value.
   *
   * @param title the source of the {@code title} field value
   * @return this instance
   */
  public IndexingItemBuilder setTitle(FieldOrValue<String> title) {
    this.title = title;
    return this;
  }

  /**
   * Sets the {@code sourceRepositoryUrl} field value for the {@code ItemMetadata},
   * either from the given field (or key) in the {@code values} multimap, or a
   * literal value.
   *
   * @param url the source of the {@code url} field value
   * @return this instance
   */
  public IndexingItemBuilder setSourceRepositoryUrl(FieldOrValue<String> url) {
    this.url = url;
    return this;
  }

  /**
   * Sets the {@code updateTime} field value for the {@code ItemMetadata},
   * either from the given field (or key) in the {@code values} multimap, or a
   * literal value.
   *
   * @param updateTime the source of the {@code updateTime} field value
   * @return this instance
   */
  public IndexingItemBuilder setUpdateTime(FieldOrValue<DateTime> updateTime) {
    this.updateTime = updateTime;
    return this;
  }

  /**
   * Sets the {@code createTime} field value for the {@code ItemMetadata},
   * either from the given field (or key) in the {@code values} multimap, or a
   * literal value.
   *
   * @param createTime the source of the {@code createTime} field value
   * @return this instance
   */
  public IndexingItemBuilder setCreateTime(FieldOrValue<DateTime> createTime) {
    this.createTime = createTime;
    return this;
  }

  /**
   * Sets the {@code contentLanguage} field value for the {@code ItemMetadata},
   * either from the given field (or key) in the {@code values} multimap, or a
   * literal value.
   *
   * @param language the source of the {@code contentLanguage} field value
   * @return this instance
   */
  public IndexingItemBuilder setContentLanguage(FieldOrValue<String> language) {
    this.language = language;
    return this;
  }

  /**
   * Sets the {@code searchQualityMetadata} field value for the {@code ItemMetadata}.
   * <p>
   * Using this setter will override any value previously set using {@link
   * #setSearchQualityMetadataQuality(FieldOrValue)}.
   *
   * @param searchQuality the {@code SearchQualityMetadata} instance
   * @return this instance
   */
  public IndexingItemBuilder setSearchQualityMetadata(SearchQualityMetadata searchQuality) {
    this.searchQuality = searchQuality;
    this.searchQualityMetadataQuality = null;
    return this;
  }

  /**
   * Sets the {@code searchQualityMetadata.quality} field value for the {@code ItemMetadata}.
   * <p>
   * Using this setter will override any value previously set using {@link
   * #setSearchQualityMetadata(SearchQualityMetadata)}.
   *
   * @param quality the source of the {@code searchQualityMetadata.quality} value
   * @return this instance
   */
  public IndexingItemBuilder setSearchQualityMetadataQuality(FieldOrValue<Double> quality) {
    this.searchQualityMetadataQuality = quality;
    this.searchQuality = null;
    return this;
  }

  /**
   * Sets the {@code hash} field value for the {@code ItemMetadata}, either from
   * the given field (or key) in the {@code values} multimap, or a literal value.
   *
   * @param hash the source of the {@code hash} field value
   * @return this instance
   */
  public IndexingItemBuilder setHash(FieldOrValue<String> hash) {
    this.hash = hash;
    return this;
  }

  /**
   * Sets the {@code hash} field value for the {@code ItemMetadata}.
   *
   * @param hash the {@code hash} field value
   * @return this instance
   * @deprecated Use {@link #setHash(FieldOrValue)} with {@link FieldOrValue#withValue}
   */
  @Deprecated
  public IndexingItemBuilder setHash(String hash) {
    return setHash(FieldOrValue.withValue(hash));
  }

  /**
   * Sets the {@code containerName} field value for the {@code ItemMetadata}, either from
   * the given field (or key) in the {@code values} multimap, or a literal value.
   *
   * @param containerName the source of the {@code containerName} field value
   * @return this instance
   */
  public IndexingItemBuilder setContainerName(FieldOrValue<String> containerName) {
    this.containerName = containerName;
    return this;
  }

  /**
   * Sets the {@code containerName} field value for the {@code ItemMetadata}.
   *
   * @param containerName the {@code containerName} field value
   * @return this instance
   * @deprecated Use {@link #setContainerName(FieldOrValue)} with {@link FieldOrValue#withValue}
   */
  @Deprecated
  public IndexingItemBuilder setContainerName(String containerName) {
    return setContainerName(FieldOrValue.withValue(containerName));
  }

  /**
   * Sets the {@code queue} field value for the {@code Item}.
   *
   * @param queue the {@code queue} field value
   * @return this instance
   */
  public IndexingItemBuilder setQueue(String queue) {
    this.queue = queue;
    return this;
  }

  /**
   * Sets the {@code payload} field value for the {@code Item}.
   *
   * @param payload the {@code payload} field value
   * @return this instance
   */
  public IndexingItemBuilder setPayload(byte[] payload) {
    this.payload = payload;
    return this;
  }

  /**
   * Sets the {@code version} field value for the {@code Item}.
   *
   * @param version the {@code version} field value
   * @return this instance
   */
  public IndexingItemBuilder setVersion(byte[] version) {
    this.version = version;
    return this;
  }

  /**
   * Sets the {@code itemType} field value for the {@code Item}.
   *
   * @param itemType the {@code itemType} field value
   * @return this instance
   */
  public IndexingItemBuilder setItemType(ItemType itemType) {
    this.itemType = itemType;
    return this;
  }

  /**
   * Builds the {@link Item} using all of the previously set attributes.
   *
   * <p>Aside from the {@code name} and {@code values} map, all of the attributes are optional.
   * The metadata attributes
   * ({@code mimeType, title, sourceRepositoryUrl, updateTime, createTime, contentLanguage,
   * hash, containerName})
   * can be set explicitly in the setter, from the {@code values} map, or using the
   * {@link #fromConfiguration configuration properties}.
   *
   * @return fully built {@link Item} object
   */
  public Item build() {
    checkArgument(!Strings.isNullOrEmpty(name), "item name can not be null");
    checkNotNull(values, "values can not be null");
    Item item = new Item();
    item.setName(name);
    if (acl != null) {
      acl.applyTo(item);
    }
    ItemMetadata metadata = new ItemMetadata();
    setFromFieldOrValues(objectType, configObjectType, values, StructuredData.STRING_CONVERTER,
        Strings::isNullOrEmpty, Function.identity(),
        itemObjectType -> {
          metadata.setObjectType(itemObjectType);
          item.setStructuredData(
              new ItemStructuredData().setObject(
                  StructuredData.getStructuredData(itemObjectType, values)));
        });
    setFromFieldOrValues(mimeType, configMimeType, values, StructuredData.STRING_CONVERTER,
        Strings::isNullOrEmpty, Function.identity(), metadata::setMimeType);
    setFromFieldOrValues(title, configTitle, values, StructuredData.STRING_CONVERTER,
        Strings::isNullOrEmpty, Function.identity(), metadata::setTitle);
    setFromFieldOrValues(url, configUrl, values, StructuredData.STRING_CONVERTER,
        Strings::isNullOrEmpty, Function.identity(), metadata::setSourceRepositoryUrl);
    setFromFieldOrValues(language, configLanguage, values, StructuredData.STRING_CONVERTER,
        Strings::isNullOrEmpty, Function.identity(), metadata::setContentLanguage);
    setFromFieldOrValues(updateTime, configUpdateTime, values, StructuredData.DATETIME_CONVERTER,
        Objects::isNull, DateTime::toStringRfc3339, metadata::setUpdateTime);
    setFromFieldOrValues(createTime, configCreateTime, values, StructuredData.DATETIME_CONVERTER,
        Objects::isNull, DateTime::toStringRfc3339, metadata::setCreateTime);
    setFromFieldOrValues(hash, configHash, values, StructuredData.STRING_CONVERTER,
        Strings::isNullOrEmpty, Function.identity(), metadata::setHash);
    setFromFieldOrValues(containerName, configContainerName, values,
        StructuredData.STRING_CONVERTER, Strings::isNullOrEmpty, Function.identity(),
        metadata::setContainerName);
    if (searchQuality != null) {
      metadata.setSearchQualityMetadata(searchQuality);
    } else {
      SearchQualityMetadata searchQualityMetadata = new SearchQualityMetadata();
      setFromFieldOrValues(searchQualityMetadataQuality, configSearchQualityMetadataQuality,
          values, StructuredData.DOUBLE_CONVERTER, Objects::isNull, Function.identity(),
          searchQualityMetadata::setQuality);
      if (searchQualityMetadata.getQuality() != null) {
        metadata.setSearchQualityMetadata(searchQualityMetadata);
      }
    }
    if (version != null) {
      item.encodeVersion(version);
    }
    if (payload != null) {
      item.encodePayload(payload);
    }
    if (!Strings.isNullOrEmpty(queue)) {
      item.setQueue(queue);
    }
    if (itemType != null) {
      item.setItemType(itemType.name());
    }
    item.setMetadata(metadata);
    return item;
  }

  /**
   * Gets the first value from the primary (nullable) or backup (optional) FieldOrValue
   * instances, extracts non-empty values, and passes them to the given ItemMetadata setter.
   *
   * @param <T> the field type in this class
   * @param <M> the field type in the API model, usually String
   * @param primary the FieldOrValue from the setter
   * @param backup the FieldOrValue from the configuration
   * @param values the multimap used to resolve field name references
   * @param converter the converter for multimap values
   * @param isEmpty a predicate to check whether a value is present or missing
   */
  private static <T, M> void setFromFieldOrValues(FieldOrValue<T> primary,
      Optional<FieldOrValue<T>> backup,
      Multimap<String, Object> values,
      Converter<Object, T> converter,
      Predicate<T> isEmpty,
      Function<T, M> extractor,
      Consumer<M> setter) {
    T value = getSingleValue(primary, values, converter);
    if (isEmpty.test(value) && backup.isPresent()) {
      value = getSingleValue(backup.get(), values, converter);
    }
    if (!isEmpty.test(value)) {
      setter.accept(extractor.apply(value));
    }
  }

  private static <T> T getSingleValue(
      FieldOrValue<T> field,
      Multimap<String, Object> values,
      Converter<Object, T> converter) {
    if (field == null) {
      return null;
    }
    if (field.fieldName == null) {
      return field.defaultValue;
    }
    List<Object> fieldValues =
        values.get(field.fieldName).stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (fieldValues.isEmpty()) {
      return field.defaultValue;
    }
    return converter.convert(fieldValues.get(0));
  }

  /**
   * Construct to specify an actual field value or pointer to a key within the key/values map.
   */
  public static class FieldOrValue<T> {
    private final String fieldName;
    private final T defaultValue;

    FieldOrValue(String fieldName, T defaultValue) {
      this.fieldName = fieldName;
      this.defaultValue = defaultValue;
    }

    /** Gets a string suitable for unit tests. */
    @Override
    public String toString() {
      return "{field:" + fieldName + "; value:" + defaultValue + '}';
    }

    @Override
    public int hashCode() {
      return Objects.hash(fieldName, defaultValue);
    }

    @Override
    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }
      if (!(other instanceof FieldOrValue)) {
        return false;
      }
      FieldOrValue<?> that = (FieldOrValue) other;
      return Objects.equals(this.fieldName, that.fieldName)
          && Objects.equals(this.defaultValue, that.defaultValue);
    }

    /**
     * Looks up value for the property from {@link IndexingItemBuilder#setValues}
     * for the given field.
     *
     * @param field the key to lookup from {@link IndexingItemBuilder#setValues}
     * @return {@link FieldOrValue} instance pointing to field
     */
    public static <T> FieldOrValue<T> withField(String field) {
      checkArgument(!Strings.isNullOrEmpty(field), "field name can not be null or empty");
      return new FieldOrValue<T>(field, null);
    }

    /**
     * Uses the provided value for the property.
     *
     * @param value the value for the property
     * @return {@link FieldOrValue} instance pointing to value
     */
    public static <T> FieldOrValue<T> withValue(T value) {
      return new FieldOrValue<T>(null, value);
    }
  }

  /**
   * Constructs a {@link FieldOrValue} instance from the given {@code configKey},
   * trying first a suffix of {@code .field} and if that is not does not exist,
   * or has an empty value, a suffix of {@code .defaultValue}.
   *
   * @param configKey a configuration key prefix, for example, {@code "itemMetadata.title"}
   * @param parser a configuration {@link Parser} of the appropriate type, for example,
   *     {@code String} or {@code DateTime}
   */
  private static <T> Optional<FieldOrValue<T>> fieldOrValue(String configKey,
      Configuration.Parser<T> parser) {
    String field = Configuration.getString(dotField(configKey), "").get();
    String value = Configuration.getString(dotValue(configKey), "").get();
    return fieldOrValue(configKey, field, value, parser);
  }

  /**
   * Constructs a {@link FieldOrValue} instance for the given {@code configKey},
   * trying first the given field and if that is not does not exist,
   * or has an empty value, the given default value.
   *
   * @param configKey a configuration key prefix, for example, {@code "itemMetadata.title"}
   * @param field the key to lookup from {@link IndexingItemBuilder#setValues}
   * @param value the default value
   * @param parser a configuration {@link Parser} of the appropriate type, for example,
   *     {@code String} or {@code DateTime}
   */
  private static <T> Optional<FieldOrValue<T>> fieldOrValue(String configKey, String field,
      String value, Configuration.Parser<T> parser) {
    if (field.isEmpty()) {
      if (value.isEmpty()) {
        return Optional.empty();
      } else {
        logger.log(Level.CONFIG, "{0} = {1}", new Object[] { dotValue(configKey), value });
        return Optional.of(FieldOrValue.withValue(parser.parse(value)));
      }
    } else {
      logger.log(Level.CONFIG, "{0} = {1}", new Object[] { dotField(configKey), field });
      if (value.isEmpty()) {
        return Optional.of(FieldOrValue.withField(field));
      } else {
        logger.log(Level.CONFIG, "{0} = {1}", new Object[] { dotValue(configKey), value });
        return Optional.of(new FieldOrValue<>(field, parser.parse(value)));
      }
    }
  }

  // TODO(jlacey): This could be moved to Configuration to support date properties.
  private static final Configuration.Parser<DateTime> DATE_PARSER =
      value -> {
        checkArgument(!Strings.isNullOrEmpty(value), "value to parse cannot be null or empty.");
        try {
          return new DateTime(value);
        } catch (NumberFormatException e) {
          throw new InvalidConfigurationException(e);
        }
      };

  /** Mutable holder of configurable defaults. */
  static class ConfigDefaults {
    private Optional<FieldOrValue<String>> mimeType = Optional.empty();
    private Optional<FieldOrValue<String>> title = Optional.empty();
    private Optional<FieldOrValue<String>> url = Optional.empty();
    private Optional<FieldOrValue<DateTime>> updateTime = Optional.empty();
    private Optional<FieldOrValue<DateTime>> createTime = Optional.empty();
    private Optional<FieldOrValue<String>> language = Optional.empty();
    private Optional<FieldOrValue<String>> hash = Optional.empty();
    private Optional<FieldOrValue<String>> containerName = Optional.empty();
    private Optional<FieldOrValue<Double>> searchQualityMetadataQuality = Optional.empty();
    private Optional<FieldOrValue<String>> objectType = Optional.empty();

    public ConfigDefaults setMimeType(Optional<FieldOrValue<String>> mimeType) {
      this.mimeType = mimeType;
      return this;
    }

    public ConfigDefaults setTitle(Optional<FieldOrValue<String>> title) {
      this.title = title;
      return this;
    }

    public ConfigDefaults setUrl(Optional<FieldOrValue<String>> url) {
      this.url = url;
      return this;
    }

    public ConfigDefaults setUpdateTime(Optional<FieldOrValue<DateTime>> updateTime) {
      this.updateTime = updateTime;
      return this;
    }

    public ConfigDefaults setCreateTime(Optional<FieldOrValue<DateTime>> createTime) {
      this.createTime = createTime;
      return this;
    }

    public ConfigDefaults setLanguage(Optional<FieldOrValue<String>> language) {
      this.language = language;
      return this;
    }

    public ConfigDefaults setHash(Optional<FieldOrValue<String>> hash) {
      this.hash = hash;
      return this;
    }

    public ConfigDefaults setContainerName(Optional<FieldOrValue<String>> containerName) {
      this.containerName = containerName;
      return this;
    }

    public ConfigDefaults setSearchQualityMetadataQuality(Optional<FieldOrValue<Double>> quality) {
      this.searchQualityMetadataQuality = quality;
      return this;
    }

    public ConfigDefaults setObjectType(Optional<FieldOrValue<String>> objectType) {
      this.objectType = objectType;
      return this;
    }
  }
}
