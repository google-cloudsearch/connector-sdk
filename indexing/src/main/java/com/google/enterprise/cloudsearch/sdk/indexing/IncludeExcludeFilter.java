/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.enterprise.cloudsearch.sdk.indexing;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Helper utility to check if a particular item should be indexed. Include/exclude
 * patterns are specified using Java regular expressions; see <a
 * href="https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">java.util.regex.Pattern</a>.
 *
 * <p>A connector can choose whether to use this helper; see the connector documentation
 * to find out if this is supported.
 *
 * <p>This filter uses configuration property names with the following patterns:
 * <ul>
 *   <li>include patterns are prefixed with "includeExcludeFilter.include.regex."
 *   <li>exclude patterns are prefixed with "includeExcludeFilter.exclude.regex."
 * </ul>
 * The last part of the property name is not used here but can be used to describe the
 * pattern. Each property name must be unique. For example
 * <ul>
 *   <li>includeExcludeFilter.include.regex.textFiles = .*\\.txt
 *   <li>includeExcludeFilter.include.regex.htmlFiles = .*\\.html
 *   <li>includeExcludeFilter.exclude.regex.pdfFiles = .*\\.pdf
 * </ul>
 * Since the property values are in a Java Properties file, any backslash characters in
 * the regular expression must be escaped.
 */
public class IncludeExcludeFilter {
  private static final Logger logger = Logger.getLogger(IncludeExcludeFilter.class.getName());
  public static final String FILTER_CONFIG_PREFIX = "includeExcludeFilter.";
  public static final String INCLUDE_RULE_PREFIX = FILTER_CONFIG_PREFIX + "include.regex.";
  public static final String EXCLUDE_RULE_PREFIX = FILTER_CONFIG_PREFIX + "exclude.regex.";

  @VisibleForTesting final List<Rule<String>> includeRules;
  @VisibleForTesting final List<Rule<String>> excludeRules;

  public IncludeExcludeFilter(List<Rule<String>> includeRules, List<Rule<String>> excludeRules) {
    this.includeRules = includeRules;
    this.excludeRules = excludeRules;
  }

  public static IncludeExcludeFilter fromConfiguration() {
    checkState(Configuration.isInitialized());
    Set<String> filterProperties = Configuration.getConfig()
        .stringPropertyNames()
        .stream()
        .filter(key -> key.startsWith(FILTER_CONFIG_PREFIX))
        .collect(Collectors.toSet());
    if (filterProperties.isEmpty()) {
      return new IncludeExcludeFilter(ImmutableList.of(), ImmutableList.of());
    }
    List<Rule<String>> includeRules = new ArrayList<IncludeExcludeFilter.Rule<String>>();
    List<Rule<String>> excludeRules = new ArrayList<IncludeExcludeFilter.Rule<String>>();
    for (String propertyName : filterProperties) {
      String propertyValue = Configuration.getString(propertyName, null).get();
      logger.log(Level.FINEST, "Processing include/exclude rule {0}: {1}",
          new Object[] { propertyName, propertyValue });
      try {
        if (propertyName.startsWith(INCLUDE_RULE_PREFIX)) {
          includeRules.add(new Rule<>(new RegexPredicate(propertyValue)));
        } else if (propertyName.startsWith(EXCLUDE_RULE_PREFIX)) {
          excludeRules.add(new Rule<>(new RegexPredicate(propertyValue)));
        }
      } catch (PatternSyntaxException e) {
        throw new InvalidConfigurationException("Invalid regex pattern " + propertyValue, e);
      }
    }
    return new IncludeExcludeFilter(includeRules, excludeRules);
  }

  /**
   * Returns true if the given string is included based on the configured include/exclude
   * patterns.
   *
   * @param value a value to test
   * @return true if the value is included based on the configuration
   */
  public boolean isAllowed(String value) {
    boolean exclude = evaluateRules(excludeRules, value, false /* nothing is excluded */);
    if (exclude) {
      logger.log(Level.FINEST, "excluding " + value);
      return false;
    }
    return evaluateRules(includeRules, value, true /* everything is included */);
  }

  private boolean evaluateRules(List<Rule<String>> rules, String value, boolean emptyRulesOutcome) {
    if (rules.isEmpty()) {
      return emptyRulesOutcome;
    }
    return rules.stream().map(r -> r.eval(value)).filter(e -> e == true).findFirst().orElse(false);
  }

  private static class Rule<T> {
    private final Predicate<T> predicate;

    public Rule(Predicate<T> predicate) {
      this.predicate = checkNotNull(predicate);
    }

    public boolean eval(T val) {
      return predicate.apply(val);
    }
  }

  private static class RegexPredicate implements Predicate<String> {
    private final Pattern p;
    private final String regex;

    private RegexPredicate(String regex) {
      this.p = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
      this.regex = regex;
    }

    @Override
    public boolean apply(@Nullable String input) {
      Matcher matcher = p.matcher(input);
      boolean matches = matcher.matches();
      logger.log(
          Level.FINE,
          "Pattern [{0}] input [{1}] matches Outcome [{2}] with Regex[{3}]",
          new Object[] {p, input, matches, regex});
      return matches;
    }
  }
}
