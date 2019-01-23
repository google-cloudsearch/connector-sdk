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

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.escape.Escaper;
import com.google.common.net.UrlEscapers;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;

/**
 * Constructs an URL from a {@link MessageFormat}-style format string
 * and a set of columns to include in the URL.
 *
 * <p>
 * Instances are immutable and thread-safe.
 */
public class UrlBuilder {
  public static final String CONFIG_FORMAT = "url.format";
  public static final String CONFIG_COLUMNS = "url.columns";
  public static final String CONFIG_COLUMNS_TO_ESCAPE = "url.columnsToEscape";

  private static final Logger logger = Logger.getLogger(UrlBuilder.class.getName());
  private static final Escaper escaper = UrlEscapers.urlPathSegmentEscaper();

  private final String format;
  private final ImmutableSet<String> columns;
  private final ImmutableSet<String> columnsToEscape;

  private UrlBuilder(Builder builder) {
    this.format = builder.format;
    this.columns = builder.columns;
    this.columnsToEscape = builder.columnsToEscape;
  }

  /**
   * Builds the escaped URL. The given map of values may contain
   * unneeded columns, but it must contain all of the configured
   * {@value #CONFIG_COLUMNS} columns.
   *
   * @param allColumnValues a map from column names to column values
   * @return the escaped URL
   */
  public String buildUrl(Map<String, ?> allColumnValues) {
    Set<String> missing = getMissingColumns(allColumnValues.keySet());
    checkArgument(missing.isEmpty(), "Missing URL column(s): %s", missing);
    List<Object> values = new ArrayList<Object>();
    for (String col : columns) {
      if (columnsToEscape.contains(col)) {
        values.add(escaper.escape(allColumnValues.get(col).toString()));
      } else {
        values.add(allColumnValues.get(col));
      }
    }
    return MessageFormat.format(format, values.toArray());
  }

  /**
   * Gets the difference of the configured URL columns and the given set of all columns.
   * The difference will be empty if all of the URL columns are present in the given set.
   *
   * @param allColumns all columns
   * @return the difference of columns
   */
  public Set<String> getMissingColumns(Set<String> allColumns) {
    return Sets.difference(columns, allColumns);
  }

  /**
   * Constructs an {@code UrlBuilder} from the {@link Configuration}
   *
   * <p>Required configuration parameters:
   *
   * <ul>
   *   <li>{@value #CONFIG_COLUMNS} - Specifies the column names whose values will be
   *       substituted into the format to generate the URL.
   * </ul>
   *
   * <p>Optional configuration parameters:
   *
   * <ul>
   *   <li>{@value #CONFIG_FORMAT} - Specifies the format of the URL. The default value is "{0}".
   *   <li>{@value #CONFIG_COLUMNS_TO_ESCAPE} - Specifies the column names whose
   *       values will be URL escaped. Must be a subset of {@value #CONFIG_COLUMNS}.
   *
   * @return an {@code UrlBuilder}
   */
  public static UrlBuilder fromConfiguration() {
    checkState(Configuration.isInitialized(), "configuration not initialized");
    ImmutableSet<String> columns = makeColumnSet(CONFIG_COLUMNS, null);
    return new Builder()
        .setFormat(Configuration.getString(CONFIG_FORMAT, "{0}").get())
        .setColumns(columns)
        .setColumnsToEscape(makeColumnSet(CONFIG_COLUMNS_TO_ESCAPE, columns))
        .build();
  }

  private static ImmutableSet<String> makeColumnSet(String configKey, Set<String> validColumns) {
    List<String> columnList = Configuration.getMultiValue(configKey, Collections.emptyList(),
        Configuration.STRING_PARSER).get();

    LinkedHashSet<String> columnSet = new LinkedHashSet<>();
    List<String> errors = new ArrayList<>();
    for (String name : columnList) {
      name = name.trim();
      if (validColumns != null && !validColumns.contains(name)) {
        errors.add("invalid: " + name);
      } else if (columnSet.contains(name)) {
        errors.add("duplicate: " + name);
      } else {
        columnSet.add(name);
      }
    }
    if (!errors.isEmpty()) {
      throw new InvalidConfigurationException(configKey + " errors: " + errors);
    }
    return ImmutableSet.copyOf(columnSet);
  }

  /** Builder for constructing instances of {@code UrlBuilder}. */
  public static class Builder {
    private String format;
    private ImmutableSet<String> columns;
    private ImmutableSet<String> columnsToEscape;

    /**
     * Sets the URL format.
     *
     * @param format a {@link MessageFormat}-style format string for the URL
     */
    public Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    /**
     * Sets the column names whose values will be substituted into the
     * format to generate the URL.
     *
     * @param columns a set of column names
     */
    public Builder setColumns(Set<String> columns) {
      this.columns = ImmutableSet.copyOf(columns);
      return this;
    }

    /**
     * Sets the column names whose values will be URL escaped.
     * Must be a subset of the URL columns
     *
     * @param columnsToEscape a set of column names
     */
    public Builder setColumnsToEscape(Set<String> columnsToEscape) {
      this.columnsToEscape = ImmutableSet.copyOf(columnsToEscape);
      return this;
    }

    /**
     * Constructs an instance of {@code UrlBuilder}.
     *
     * @return an instance of {@code UrlBuilder}
     */
    public UrlBuilder build() {
      checkNotNullNotEmpty(format, "URL format");
      checkNotNullNotEmpty(columns, "URL columns");
      if (columnsToEscape == null) {
        columnsToEscape = ImmutableSet.of();
      }
      logger.log(Level.CONFIG,
          "URL format: {0}, URL columns: {1}, URL columns to escape: {2}",
          new Object[] {format, columns, columnsToEscape});
      return new UrlBuilder(this);
    }

    private static void checkNotNullNotEmpty(String value, String msg) {
      checkNotNull(value, msg + " must not be null.");
      checkArgument(!value.trim().isEmpty(), msg + " must not be empty.");
    }

    private static void checkNotNullNotEmpty(Set<String> value, String msg) {
      checkNotNull(value, msg + " must not be null.");
      checkArgument(!isEmptyCollection(value), msg + " must not be empty.");
    }

    private static boolean isEmptyCollection(Set<String> collection) {
      return collection.stream().filter(v -> !Strings.isNullOrEmpty(v)).count() == 0;
    }
  }
}
