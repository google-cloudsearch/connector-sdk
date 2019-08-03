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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;

/**
 * Helper utility to check if a particular item should be indexed. Include/exclude
 * patterns are specified using Java regular expressions; see <a
 * href="https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html">java.util.regex.Pattern</a>. Include/exclude
 * patterns are specified in a separate configuration file.
 *
 * <p>Configuration file:
 * <li>the path to the include/exclude pattern file is defined by the configuration property
 *     <b>connector.includeExcludeFilter.includeExcludePatternFile</b>
 * <li>include patterns are prefixed with "include:regex:"
 * <li>exclude patterns are prefixed with "exclude:regex:"
 */
public class IncludeExcludeFilter {
  private static final Logger logger = Logger.getLogger(IncludeExcludeFilter.class.getName());
  private static final String INCLUDE_RULE_PREFIX = "include:regex:";
  private static final String EXCLUDE_RULE_PREFIX = "exclude:regex:";

  @VisibleForTesting final List<Rule<String>> includeRules;
  @VisibleForTesting final List<Rule<String>> excludeRules;

  public IncludeExcludeFilter(List<Rule<String>> includeRules, List<Rule<String>> excludeRules) {
    this.includeRules = includeRules;
    this.excludeRules = excludeRules;
  }

  public static IncludeExcludeFilter fromConfiguration() {
    checkState(Configuration.isInitialized());
    String includeExcludePatternFile =
        Configuration.getString("connector.includeExcludeFilter.includeExcludePatternFile", "").get();
    if (Strings.isNullOrEmpty(includeExcludePatternFile)) {
      return new IncludeExcludeFilter(ImmutableList.of(), ImmutableList.of());
    }
    try {
      return fromFile(includeExcludePatternFile);
    } catch (IOException e) {
      throw new InvalidConfigurationException(
          "Error loading IncludeExcludeFilter configuration.", e);
    }
  }

  public static IncludeExcludeFilter fromFile(String filePath)
      throws FileNotFoundException, IOException {
    File configFile = new File(filePath);
    if (!configFile.exists()) {
      throw new InvalidConfigurationException(String.format("Path [%s] does not exist", filePath));
    }
    if (configFile.isDirectory()) {
      throw new InvalidConfigurationException(
          String.format("Path [%s] points to directory instead of file", filePath));
    }
    List<Rule<String>> includeRules = new ArrayList<IncludeExcludeFilter.Rule<String>>();
    List<Rule<String>> excludeRules = new ArrayList<IncludeExcludeFilter.Rule<String>>();
    try (BufferedReader br = new BufferedReader(new FileReader(configFile))) {
      String line;
      while ((line = br.readLine()) != null) {
        try {
          if (line.startsWith(INCLUDE_RULE_PREFIX)) {
            includeRules.add(
                new Rule<>(new RegexPredicate(line.substring(INCLUDE_RULE_PREFIX.length()))));
          } else if (line.startsWith(EXCLUDE_RULE_PREFIX)) {
            excludeRules.add(
                new Rule<>(new RegexPredicate(line.substring(EXCLUDE_RULE_PREFIX.length()))));
          } else {
            logger.log(Level.WARNING, "Invalid Rule [{0}] for include exclude pattern", line);
          }

        } catch (PatternSyntaxException e) {
          throw new InvalidConfigurationException("Invalid regex pattern " + line, e);
        }
      }
    }
    return new IncludeExcludeFilter(includeRules, excludeRules);
  }

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
