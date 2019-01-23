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
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.escape.Escaper;
import com.google.common.html.HtmlEscapers;
import com.google.enterprise.cloudsearch.sdk.config.ConfigValue;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility to create an HTML template used for formatting content from repository field data
 * (database, CSV, CRM, etc.) for uploading to Cloud Search.
 *
 * <p>The simplest use case is to define configuration parameters describing the repository's
 * content fields and then calling {@link #fromConfiguration(String)}. This will create an HTML
 * template object that will be used by the repository for calling {@link #apply(Map)} to infuse
 * content field values to generate the HTML content for uploading.
 *
 * <p>Alternatively, the {@link Builder} can be used directly to create a template from defined
 * content fields and then calling {@link #apply(Map)} as before.
 *
 * <p>The HTML template is required to have a title field ({@link Builder#setTitle(String)}), and
 * may optionally have "high" ({@link Builder#setHighContent(List)}), "medium"
 * ({@link Builder#setMediumContent(List)}, and "low" ({@link Builder#setLowContent(List)}) search
 * quality fields. These fields will have HTML tags appropriate their respective quality status. Any
 * fields not given a quality settings will be considered "unmapped" fields, and will be included or
 * excluded from the content template depending on the configuration parameter (see
 * {@link #fromConfiguration(String)}).
 *
 * <p>Sample usage from the repository initialization code:
 * <pre> {@code
 *    // one time creation of the HTML content template
 *    ContentTemplate myTemplate = ContentTemplate.fromConfiguration("myTemplateName");}
 * </pre>
 *
 * <p>And then later during iteration through the repository data:
 * <pre> {@code
 *   // while looping through the repository data
 *   ...
 *   Map<String, Object> dataValues = ... // get the data values for this repository item
 *   String htmlContent = myTemplate.apply(dataValues);
 *   // upload the content with this item
 *   ...}
 * </pre>
 */
public class ContentTemplate {
  //configuration constants
  private static final String CONFIG_TITLE_KEY_FORMAT = "contentTemplate.%s.title";
  private static final String CONFIG_HIGH_KEY_FORMAT = "contentTemplate.%s.quality.high";
  private static final String CONFIG_MEDIUM_KEY_FORMAT = "contentTemplate.%s.quality.medium";
  private static final String CONFIG_LOW_KEY_FORMAT = "contentTemplate.%s.quality.low";
  private static final String CONFIG_INCLUDE_FIELDNAME_FORMAT =
      "contentTemplate.%s.includeFieldName";
  private static final String CONFIG_UNMAPPED_COL_MODE_FORMAT =
      "contentTemplate.%s.unmappedColumnsMode";
  // HTML template tag constants
  private static final String HIGH_TAG = "<h1>";
  private static final String HIGH_TAG_CLOSE = "</h1>";
  private static final String MEDIUM_TAG = "<p>";
  private static final String MEDIUM_TAG_CLOSE = "</p>";
  private static final String LOW_TAG = "<p><small>";
  private static final String LOW_TAG_CLOSE = "</small></p>";

  /**
   * Defines how to treat extra field data provided to the {@link #apply(Map)} call.
   */
  public enum UnmappedColumnsMode {
    /**
     * Ignore any extra fields provided to the {@link #apply(Map)} call.
     */
    IGNORE,
    /**
     * Append all extra fields provided to the {@link #apply(Map)} call.
     */
    APPEND
  }

  // handles characters special to HTML.
  private static final Escaper HTML_ESCAPER = HtmlEscapers.htmlEscaper();

  // builder variables
  private final String title;
  private final LinkedHashSet<String> highContent;
  private final LinkedHashSet<String> mediumContent;
  private final LinkedHashSet<String> lowContent;
  private final List<String> allOrderedContent;
  private final String template;
  private final UnmappedColumnsMode unmappedColumnMode;
  private final boolean includeFieldName;

  /** This object must be generated via its builder. */
  private ContentTemplate(Builder builder) {
    List<String> orderedContent = new ArrayList<>();

    this.title = builder.title;
    this.highContent = new LinkedHashSet<>(builder.highContent);
    this.mediumContent = new LinkedHashSet<>(builder.mediumContent);
    this.lowContent = new LinkedHashSet<>(builder.lowContent);
    orderedContent.addAll(this.highContent);
    orderedContent.addAll(this.mediumContent);
    orderedContent.addAll(this.lowContent);
    this.allOrderedContent = Collections.unmodifiableList(orderedContent);
    this.template = builder.template;
    this.unmappedColumnMode = builder.unmappedColumnMode;
    this.includeFieldName = builder.includeFieldName;
  }

  /**
   * Returns the title of this template.
   */
  public String getTitle() {
    return title;
  }

  /**
   * Returns the previously defined high content fields.
   */
  public Set<String> getHighContent() {
    return Collections.unmodifiableSet(highContent);
  }

  /**
   * Returns the previously defined medium content fields.
   */
  public Set<String> getMediumContent() {
    return Collections.unmodifiableSet(mediumContent);
  }

  /**
   * Returns the previously defined low content fields.
   */
  public Set<String> getLowContent() {
    return Collections.unmodifiableSet(lowContent);
  }

  @VisibleForTesting
  String getTemplate() {
    return this.template;
  }

  /**
   * Creates an HTML content template using parameters specified in the connector configuration
   * file.
   *
   * <p>The configuration file should have parameters in the format of:
   * <ul>
   *   <li>contentTemplate.[templateName].title - Specifies the HTML title for the template. Note:
   *       This is a required field.
   *   <li>contentTemplate.[templateName].quality.high - Optionally specifies the "high" quality
   *       fields.
   *   <li>contentTemplate.[templateName].quality.medium - Optionally specifies the "medium" quality
   *       fields.
   *   <li>contentTemplate.[templateName].quality.low - Optionally specifies the "low" quality
   *       fields.
   *   <li>contentTemplate.[templateName].includeFieldName - Specifies whether to include the field
   *       names along with the field data in the HTML template. The default of {@code true} will
   *       cause the field names to be searchable as part of the content data.
   *   <li>contentTemplate.[templateName].unmappedColumnsMode - Specifies whether fields not
   *       designated as high/medium/low should have their data values included in the HTML content.
   *       "IGNORE" to exclude extra fields contained in the map passed to {@link #apply(Map)}, or
   *       "APPEND" (the default) to include these fields in the HTML content.
   * </ul>
   *
   * <p>Sample configuration file parameters:
   * <pre>
   *   contentTemplate.myTemplateName.title = tfield1
   *   contentTemplate.myTemplateName.quality.high = hfield1, hfield2
   *   contentTemplate.myTemplateName.quality.medium = mfield1, mfield2
   *   contentTemplate.myTemplateName.quality.low = lfield1, lfield2
   *   contentTemplate.myTemplateName.includeFieldName = true
   *   contentTemplate.myTemplateName.unmappedColumnsMode = APPEND
   * </pre>
   *
   * @param templateName the specific template name within the configuration object
   */
  public static ContentTemplate fromConfiguration(String templateName) {
    checkArgument(!Strings.isNullOrEmpty(templateName), "templateName can not be null");
    checkState(Configuration.isInitialized(), "configuration not initialized");
    ConfigValue<String> titleField =
        Configuration.getString(String.format(CONFIG_TITLE_KEY_FORMAT, templateName), null);
    ConfigValue<List<String>> highQualityFields =
        Configuration.getMultiValue(
            String.format(CONFIG_HIGH_KEY_FORMAT, templateName),
            Collections.emptyList(),
            Configuration.STRING_PARSER);
    ConfigValue<List<String>> mediumQualityFields =
        Configuration.getMultiValue(
            String.format(CONFIG_MEDIUM_KEY_FORMAT, templateName),
            Collections.emptyList(),
            Configuration.STRING_PARSER);
    ConfigValue<List<String>> lowQualityFields =
        Configuration.getMultiValue(
            String.format(CONFIG_LOW_KEY_FORMAT, templateName),
            Collections.emptyList(),
            Configuration.STRING_PARSER);
    ConfigValue<Boolean> includeFieldName =
        Configuration.getBoolean(
            String.format(CONFIG_INCLUDE_FIELDNAME_FORMAT, templateName), true);
    ConfigValue<UnmappedColumnsMode> unmappedColumnMode =
        Configuration.getValue(
            String.format(CONFIG_UNMAPPED_COL_MODE_FORMAT, templateName),
            UnmappedColumnsMode.APPEND,
            UnmappedColumnsMode::valueOf);
    return new Builder()
        .setTitle(titleField.get())
        .setHighContent(highQualityFields.get())
        .setMediumContent(mediumQualityFields.get())
        .setLowContent(lowQualityFields.get())
        .setIncludeFieldName(includeFieldName.get())
        .setUnmappedColumnMode(unmappedColumnMode.get())
        .build();
  }

  /**
   * Infuses the passed key value pairs into the previously generated template.
   *
   * @param keyValues field name/field value pairs
   * @return populated HTML string
   */
  public String apply(Map<String, Object> keyValues) {
    checkNotNull(keyValues, "Key Values map cannot be null.");
    Multimap<String, Object> multiMap = ArrayListMultimap.create();
    for (Map.Entry<String, Object> entry : keyValues.entrySet()) {
      multiMap.put(entry.getKey(), entry.getValue());
    }
    return apply(multiMap);
  }

  /**
   * Infuses the passed key value pairs into the previously generated template.
   *
   * @param keyValues field name/field values pairs as {@link Multimap}
   * @return populated HTML string
   */
  public String apply(Multimap<String, Object> keyValues) {
    checkNotNull(keyValues, "Key Values map cannot be null.");
    List<String> values = new ArrayList<>();
    values.add(HTML_ESCAPER.escape(getOrDefault(keyValues, this.title, "Title")));

    // add each field or empty if not present to preserve order, etc.
    values.add(HTML_ESCAPER.escape(getOrDefault(keyValues, this.title, ""))); // duplicate in body
    for (String field : this.allOrderedContent) {
      values.add(HTML_ESCAPER.escape(getOrDefault(keyValues, field, "")));
    }

    if (unmappedColumnMode == UnmappedColumnsMode.APPEND) {
      Set<String> processed = new HashSet<>(allOrderedContent);
      processed.add(title);
      List<String> additionalValues = new ArrayList<>();
      StringBuilder unmappedContent = new StringBuilder();
      keyValues
          .keySet()
          .stream()
          .filter(k -> !processed.contains(k))
          .forEach(
              field -> {
                unmappedContent.append(getDiv(field, LOW_TAG, LOW_TAG_CLOSE, includeFieldName));
                additionalValues.add(HTML_ESCAPER.escape(getOrDefault(keyValues, field, "")));
              });
      values.add(String.format(unmappedContent.toString(), additionalValues.toArray()));
    }
    // infuse the values
    return String.format(this.template, values.toArray());
  }

  private static String getOrDefault(
      Multimap<String, Object> keyValues, String key, String defaultVal) {
    if (!keyValues.containsKey(key)) {
      return defaultVal;
    }
    Collection<Object> values =
        keyValues.get(key).stream().filter(Objects::nonNull).collect(Collectors.toList());
    if (values.isEmpty()) {
      return defaultVal;
    }
    return Joiner.on(", ").join(values);
  }

  public static class Builder {
    // Parameter based variables
    private String title = "";
    private LinkedHashSet<String> highContent = new LinkedHashSet<>();
    private LinkedHashSet<String> mediumContent = new LinkedHashSet<>();
    private LinkedHashSet<String> lowContent = new LinkedHashSet<>();
    private String template = "";
    // include the field name in the content option
    private boolean includeFieldName = true;
    private UnmappedColumnsMode unmappedColumnMode = UnmappedColumnsMode.APPEND;

    /**
     * Sets the "title" of the HTML. This will be the highest priority for indexing.
     *
     * @param title main HTML title
     * @return builder
     */
    public Builder setTitle(String title) {
      checkArgument(title != null, "Template title cannot be null.");
      checkArgument(!title.trim().isEmpty(), "Template title cannot be empty.");
      this.title = title.trim();
      return this;
    }

    /**
     * Determines whether to include the field name within the content for that field.
     *
     * <p>If set to true (default), the format in the content will be "field:value" instead of just
     * "value". This may be desirable for search results, but may not be desired if the field name
     * is different from the field's repository display name or is not descriptive to the user.
     *
     * @param include {@code true} to include field name in content.
     * @return builder
     */
    public Builder setIncludeFieldName(boolean include) {
      this.includeFieldName = include;
      return this;
    }

    /**
     * Adds high priority content fields.
     *
     * @param highFields the field names
     * @return builder
     */
    public Builder setHighContent(List<String> highFields) {
      this.highContent = getFields(highFields);
      return this;
    }

    /**
     * Adds medium priority content fields.
     *
     * @param mediumFields the field names
     * @return builder
     */
    public Builder setMediumContent(List<String> mediumFields) {
      this.mediumContent = getFields(mediumFields);
      return this;
    }

    /**
     * Adds low priority content fields.
     *
     * @param lowFields the field names
     * @return builder
     */
    public Builder setLowContent(List<String> lowFields) {
      this.lowContent = getFields(lowFields);
      return this;
    }

    /**
     * Sets the mode (APPEND or IGNORE) for unmapped columns.
     *
     * @param mode UnmappedColumnsMode
     * @return builder
     */
    public Builder setUnmappedColumnMode(UnmappedColumnsMode mode) {
      this.unmappedColumnMode = mode;
      return this;
    }

    /**
     * Trims, checks, and copies the fields for later template building.
     *
     * @param fields the content fields of a specific quality level
     * @return a sanitized copy of the content fields
     */
    private LinkedHashSet<String> getFields(List<String> fields) {
      checkArgument((fields != null) && !fields.contains(null),
          "Template content fields cannot be null.");
      List<String> trimFields = new ArrayList<>(fields);
      trimFields.replaceAll(String::trim);
      LinkedHashSet<String> content = new LinkedHashSet<>(trimFields);
      checkArgument(!content.contains(""), "Template content fields cannot be empty: " + content);
      return content;
    }

    /**
     * Gets the generated HTML template based on the passed field data.
     *
     * <p>After building the template, merge the content fields into a single array to simplify
     * template substitution in {@link #apply(Map)}.</p>
     *
     * @return HTML template
     */
    public ContentTemplate build() {
      this.template = makeTemplate();
      return new ContentTemplate(this);
    }

    /**
     * Generates the HTML template based on the passed field data.
     *
     * <p>When built, the HTML template will have one "%s" for the future "title", one "%s" for the
     * "title" as part of the content, and one "%s" for each field (high, medium, and low) defined.
     * If the values are missing, they will be replaced with empty strings. The order of the values
     * will be preserved with highs preceding mediums preceding lows. Additionally, the order is
     * preserved within each priority based on the initial lists.</p>
     *
     * @return the HTML template
     */
    private String makeTemplate() {
      checkArgument(!title.isEmpty(), "Template title cannot be empty.");
      checkArgument((unmappedColumnMode == UnmappedColumnsMode.APPEND)
              || !this.highContent.isEmpty()
              || !this.mediumContent.isEmpty()
              || !this.lowContent.isEmpty(),
          "Cannot create content HTML without any content.");
      removeDuplicates();
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder
          .append("<!DOCTYPE html>\n")
          .append("<html lang='en'>\n")
          .append("<head>\n")
          .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
          .append("<title>%s</title>\n")
          .append("</head>\n")
          .append("<body>\n");
      stringBuilder.append(getDiv(title, HIGH_TAG, HIGH_TAG_CLOSE, includeFieldName));
      for (String field : highContent) {
        stringBuilder.append(getDiv(field, HIGH_TAG, HIGH_TAG_CLOSE, includeFieldName));
      }
      for (String field : mediumContent) {
        stringBuilder.append(getDiv(field, MEDIUM_TAG, MEDIUM_TAG_CLOSE, includeFieldName));
      }
      for (String field : lowContent) {
        stringBuilder.append(getDiv(field, LOW_TAG, LOW_TAG_CLOSE, includeFieldName));
      }
      if (unmappedColumnMode == UnmappedColumnsMode.APPEND) {
        stringBuilder.append("%s");
      }
      stringBuilder.append("</body>\n").append("</html>\n");
      return stringBuilder.toString();
    }

    /**
     * Removes entries from "lower" set if they occur in "higher" set.
     *
     * <p>These entries will be used in the h/m/l HTML template. Each will have its own "div" id,
     * where duplicates are not valid HTML. Removing the lower priority duplicate entries will
     * preserve the intent of the sets.</p>
     */
    private void removeDuplicates() {
      this.highContent.remove(this.title);
      this.mediumContent.remove(this.title);
      this.mediumContent.removeAll(this.highContent);
      this.lowContent.remove(this.title);
      this.lowContent.removeAll(this.highContent);
      this.lowContent.removeAll(this.mediumContent);
    }
  }

  /**
   * Formats a "div" HTML section for the field name.
   *
   * <pre>{@code
   * Example assuming tag == "<h1>" and tagClose == "</h1>":
   * <div id='key'>
   *   <p>key:</p>
   *   <h1>%s</h1>
   * </div>}
   * </pre>
   * Where "%s" will be substituted in {@link #apply(Map)} to "value".
   * Note: Remove {@code "<p>key:</p>"} from content line if {@link #includeFieldName} not set.
   *
   * @param key The field name for div id
   * @param tag the priority tag for the field's "value"
   * @param tagClose the priority closing tag
   * @return the HTML string of this "div"
   */
  @VisibleForTesting
  static String getDiv(String key, String tag, String tagClose, boolean includeFieldName) {
    checkArgument(!Strings.isNullOrEmpty(key), "Key cannot be null/empty.");
    checkArgument(!Strings.isNullOrEmpty(tag), "Tag cannot be null/empty.");
    checkArgument(!Strings.isNullOrEmpty(tagClose), "Closing tag cannot be null/empty.");
    StringBuilder html = new StringBuilder();
    html.append("<div id='").append(HTML_ESCAPER.escape(key)).append("'>\n");
    if (includeFieldName) {
      html.append("  <p>").append(HTML_ESCAPER.escape(key)).append(":").append("</p>\n");
    }
    html.append("  ").append(tag).append("%s").append(tagClose).append("\n");
    html.append("</div>\n");
    return html.toString();
  }
}
