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

import static com.google.common.base.Preconditions.checkState;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Matches values against configured include/exclude rules. A connector can use this to
 * limit which repository items are indexed.
 *
 * <p>See the documentation for a given connector to find out if it includes support for
 * this class. Each connector determines which value(s) are tested against configured
 * rules.
 *
 * <p>Each rule is specified using four configuration properties. A unique, meaningful
 * name is used to group the properties for a rule together.
 * <ul>
 * <li>{@code includeExcludeFilter.<name>.itemType}
 * <li>{@code includeExcludeFilter.<name>.filterType}
 * <li>{@code includeExcludeFilter.<name>.filterPattern}
 * <li>{@code includeExcludeFilter.<name>.action}
 * </ul>
 *
 * <p>{@code itemType} must be one of the valid {@link IndexingItemBuilder.ItemType}
 * values, generally either CONTAINER_ITEM or CONTENT_ITEM.
 *
 * <p>{@code filterType} must be set to REGEX.
 *
 * <p>{@code filterPattern} must be set to a Java regular expression; see <a
 * href="https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html"
 * >java.util.regex.Pattern</a>. This
 * class will use case-insensitive matching. Since the property values are in a Java
 * Properties file, any backslash characters in the regular expression must be escaped.
 * Patterns will be matched using {@code java.util.regex.Matcher.find()}, which looks for
 * a subsequence matching the given pattern, so patterns need not match the entire input
 * value.
 *
 * <p>{@code action} must be set to {@code INCLUDE} or {@code EXCLUDE}.
 *
 * <p>If any EXCLUDE rules are configured for an item type, an item will be excluded if it
 * matches any of those rules. Otherwise, if any INCLUDE rules are configured for an item
 * type, an item must match at least one INCLUDE rule to be included. When no rules are
 * configured, all items of the given type are allowed.
 *
 * <p>Examples:
 * <p>Index text files (by extension)
 * <pre>
 *  includeExcludeFilter.includeText.action = INCLUDE
 *  includeExcludeFilter.includeText.itemType = CONTENT_ITEM
 *  includeExcludeFilter.includeText.filterType = REGEX
 *  includeExcludeFilter.includeText.filterPattern = \\.txt$
 * </pre>
 * This rule includes content items ending in {@code .txt}. If this is the only configured
 * rule, all CONTAINER_ITEM objects are also included, since no rules are defined for that
 * type. Including the containers is important when indexing a hierarchical repository
 * using a listing connector.
 *
 * <p>Index {@code .doc} files within a subfolder under the root
 * <pre>
 * includeExcludeFilter.includePublicFolder.action = INCLUDE
 * includeExcludeFilter.includePublicFolder.itemType = CONTAINER_ITEM
 * includeExcludeFilter.includePublicFolder.filterType = REGEX
 * includeExcludeFilter.includePublicFolder.filterPattern = \
 *     ^/$|^/root[/]?$|^/root/folder[/]?$|^/root/folder/public[/]?$|^/root/folder/public/
 *
 * includeExcludeFilter.includePressFolder.action = INCLUDE
 * includeExcludeFilter.includePressFolder.itemType = CONTAINER_ITEM
 * includeExcludeFilter.includePressFolder.filterType = REGEX
 * includeExcludeFilter.includePressFolder.filterPattern = \
 *     ^/$|^/root[/]?$|^/root/releases[/]?$|^/root/releases/press[/]?$|^/root/releases/press/
 *
 * includeExcludeFilter.includeDocsInFolder.action = INCLUDE
 * includeExcludeFilter.includeDocsInFolder.itemType = CONTENT_ITEM
 * includeExcludeFilter.includeDocsInFolder.filterType = REGEX
 * includeExcludeFilter.includeDocsInFolder.filterPattern = \
 *     ^/root/folder/public/.*\\.doc$|^/root/releases/press/.*\\.doc$
 * </pre>
 * These rules include content below two specified folders, indexing only files ending in
 * {@code .doc}.
 *
 * <p>Exclude files and folders with a folder named ARCHIVE in the path.
 * <pre>
 * includeExcludeFilter.excludeArchiveFolders.action = EXCLUDE
 * includeExcludeFilter.excludeArchiveFolders.itemType = CONTAINER_ITEM
 * includeExcludeFilter.excludeArchiveFolders.filterType = REGEX
 * includeExcludeFilter.excludeArchiveFolders.filterPattern = /ARCHIVE$|/ARCHIVE/
 *
 * includeExcludeFilter.excludeDocsInArchiveFolders.action = EXCLUDE
 * includeExcludeFilter.excludeDocsInArchiveFolders.itemType = CONTENT_ITEM
 * includeExcludeFilter.excludeDocsInArchiveFolders.filterType = REGEX
 * includeExcludeFilter.excludeDocsInArchiveFolders.filterPattern = /ARCHIVE/
 * </pre>
 */
public class IncludeExcludeFilter {
  private static final Logger logger = Logger.getLogger(IncludeExcludeFilter.class.getName());
  private static final String FILTER_CONFIG_PREFIX = "includeExcludeFilter.";

  enum Action { INCLUDE, EXCLUDE };

  enum FilterType { REGEX };

  @VisibleForTesting final ImmutableMap<ItemType, ImmutableList<Rule>> includeRules;
  @VisibleForTesting final ImmutableMap<ItemType, ImmutableList<Rule>> excludeRules;

  public IncludeExcludeFilter(Map<ItemType, List<Rule>> includeRules,
      Map<ItemType, List<Rule>> excludeRules) {
    ImmutableMap.Builder<ItemType, ImmutableList<Rule>> includeBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<ItemType, ImmutableList<Rule>> excludeBuilder = ImmutableMap.builder();
    for (ItemType itemType : ItemType.values()) {
      includeBuilder.put(itemType,
          ImmutableList.copyOf(includeRules.getOrDefault(itemType, ImmutableList.of())));
      excludeBuilder.put(itemType,
          ImmutableList.copyOf(excludeRules.getOrDefault(itemType, ImmutableList.of())));
    }
    this.includeRules = includeBuilder.build();
    this.excludeRules = excludeBuilder.build();
  }

  /**
   * Builds an IncludeExcludeFilter instance from configured properties. With no
   * properties, returns a filter that allows everything.
   *
   * @return an IncludeExcludeFilter
   */
  public static IncludeExcludeFilter fromConfiguration() {
    checkState(Configuration.isInitialized());
    Set<String> filterProperties = Configuration.getConfig()
        .stringPropertyNames()
        .stream()
        .filter(key -> key.startsWith(FILTER_CONFIG_PREFIX))
        .collect(Collectors.toSet());
    if (filterProperties.isEmpty()) {
      return new IncludeExcludeFilter(ImmutableMap.of(), ImmutableMap.of());
    }
    HashMap<String, Rule.Builder> ruleBuilders = new HashMap<>();
    for (String propertyName : filterProperties) {
      String[] propertyNameParts = propertyName.split("\\.");
      if (propertyNameParts.length != 3) {
        throw new InvalidConfigurationException("Unknown property " + propertyName);
      }
      String ruleName = propertyNameParts[1];
      String rulePropertyType = propertyNameParts[2];
      String propertyValue = Configuration.getString(propertyName, null).get();
      Rule.Builder builder = ruleBuilders.get(ruleName);
      if (builder == null) {
        builder = new Rule.Builder(ruleName);
        ruleBuilders.put(ruleName, builder);
      }
      if (rulePropertyType.equals("itemType")) {
        builder.setItemType(propertyValue);
      } else if (rulePropertyType.equals("filterType")) {
        builder.setFilterType(propertyValue);
      } else if (rulePropertyType.equals("filterPattern")) {
        builder.setFilterPattern(propertyValue);
      } else if (rulePropertyType.equals("action")) {
        builder.setAction(propertyValue);
      } else {
        throw new InvalidConfigurationException("Unknown property " + propertyName);
      }
    }

    EnumMap<ItemType, List<Rule>> includeRules = new EnumMap<>(ItemType.class);
    EnumMap<ItemType, List<Rule>> excludeRules = new EnumMap<>(ItemType.class);
    for (ItemType itemType : ItemType.values()) {
      includeRules.put(itemType, new ArrayList<>());
      excludeRules.put(itemType, new ArrayList<>());
    }
    for (Rule.Builder builder : ruleBuilders.values()) {
      try {
        Rule rule = builder.build();
        if (rule.getAction().equals(Action.INCLUDE)) {
          includeRules.get(rule.getItemType()).add(rule);
        } else {
          excludeRules.get(rule.getItemType()).add(rule);
        }
      } catch (IllegalStateException | IllegalArgumentException e) {
        throw new InvalidConfigurationException(e.getMessage());
      }
    }

    logger.log(Level.CONFIG, "Include rules: " + includeRules);
    logger.log(Level.CONFIG, "Exclude rules: " + excludeRules);
    return new IncludeExcludeFilter(includeRules, excludeRules);
  }

  /**
   * Returns true if the given string is included based on the configured include/exclude
   * patterns.
   *
   * @param value a value to test
   * @param itemType the type of item
   * @return true if the value is included based on the configuration
   */
  public boolean isAllowed(String value, ItemType itemType) {
    List<Rule> includeByType = includeRules.get(itemType);
    List<Rule> excludeByType = excludeRules.get(itemType);
    boolean exclude =
        evaluateRules(excludeByType, value, false /* if no rules: nothing is excluded */);
    if (exclude) {
      logger.log(Level.FINEST, "Excluding " + value);
      return false;
    }
    boolean include =
        evaluateRules(includeByType, value, true /* if no rules: everything is included */);
    logger.log(Level.FINEST, (include ? "Including " : "Not including ") + value);
    return include;
  }

  private boolean evaluateRules(List<Rule> rules, String value, boolean emptyRulesOutcome) {
    if (rules.isEmpty()) {
      return emptyRulesOutcome;
    }
    return rules.stream().map(r -> r.eval(value)).anyMatch(e -> e);
  }

  @VisibleForTesting
  static class Rule {
    private final String name;
    private final ItemType itemType;
    private final Action action;
    private final Predicate<String> predicate;

    private Rule(String name, ItemType itemType, Action action, Predicate<String> predicate) {
      this.name = name;
      this.itemType = itemType;
      this.action = action;
      this.predicate = predicate;
    }

    boolean eval(String val) {
      boolean result = predicate.apply(val);
      if (logger.isLoggable(Level.FINEST)) {
        logger.log(Level.FINEST,
            name + ": " + predicate + (result ? " matches " : " does not match ") + val);
      }
      return result;
    }

    String getName() {
      return name;
    }

    ItemType getItemType() {
      return itemType;
    }

    Action getAction() {
      return action;
    }

    @Override
    public String toString() {
      return name + ": " + action + " " + itemType + " matching " + predicate;
    }

    @VisibleForTesting
    static class Builder {
      private String name;
      private String itemTypeConfig;
      private String filterTypeConfig;
      private String filterPatternConfig;
      private String actionConfig;

      Builder(String name) {
        this.name = name;
      }

      Builder setItemType(String itemType) {
        this.itemTypeConfig = itemType;
        return this;
      }

      Builder setFilterType(String filterType) {
        this.filterTypeConfig = filterType;
        return this;
      }

      Builder setFilterPattern(String filterPattern) {
        this.filterPatternConfig = filterPattern;
        return this;
      }

      Builder setAction(String action) {
        this.actionConfig = action;
        return this;
      }

      Rule build()
          throws IllegalArgumentException, IllegalStateException, PatternSyntaxException {
        // checkState throws IllegalStateException
        checkState(!Strings.isNullOrEmpty(name), "Rule name is missing");
        checkState(!Strings.isNullOrEmpty(itemTypeConfig), "Rule item type is missing: " + name);
        checkState(!Strings.isNullOrEmpty(filterTypeConfig),
            "Rule filter type is missing: " + name);
        checkState(!Strings.isNullOrEmpty(filterPatternConfig),
            "Rule filter pattern is missing: " + name);
        checkState(!Strings.isNullOrEmpty(actionConfig), "Rule action is missing: " + name);

        // valueOf throws IllegalArgumentException
        ItemType itemType = ItemType.valueOf(itemTypeConfig.toUpperCase());
        Action action = Action.valueOf(actionConfig.toUpperCase());
        FilterType filterType = FilterType.valueOf(filterTypeConfig.toUpperCase());

        switch (filterType) {
          case REGEX:
            return new Rule(name, itemType, action, new RegexPredicate(filterPatternConfig));
          default:
            throw new IllegalArgumentException(filterTypeConfig);
        }
      }
    }
  }

  private static class RegexPredicate implements Predicate<String> {
    private final Pattern pattern;
    private final String regex;

    private RegexPredicate(String regex) throws PatternSyntaxException {
      this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
      this.regex = regex;
    }

    @Override
    public boolean apply(@Nullable String input) {
      Matcher matcher = pattern.matcher(input);
      return matcher.find();
    }

    @Override
    public String toString() {
      return regex;
    }
  }

  /**
   * Run this class to test patterns against possible values.
   *
   * <p>Pass {@code -Dconfig=<config-file>} to specify a file containing the configuration
   * properties.
   *
   * <p>Test values are read from standard input, either typed or redirected from a file.
   */
  public static void main(String[] args) throws java.io.IOException {
    mainHelper(args, System.in);
  }

  @VisibleForTesting
  static void mainHelper(String[] args, java.io.InputStream inStream) throws java.io.IOException {
    Configuration.initConfig(args);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    System.out.println("Rules:");
    Stream.concat(
        filter.includeRules.values().stream().filter(list -> !list.isEmpty()),
        filter.excludeRules.values().stream().filter(list -> !list.isEmpty()))
        .forEach(list -> list.stream().forEach(rule -> System.out.println("*** " + rule)));
    System.out.println();

    System.out.println("Enter test value(s)");
    try (Scanner in = new Scanner(inStream)) {
      while (true) {
        try {
          String value = in.next();
          System.out.println(value);
          System.out.println("  as content  : " + filter.isAllowed(value, ItemType.CONTENT_ITEM));
          System.out.println("  as container: " + filter.isAllowed(value, ItemType.CONTAINER_ITEM));
          System.out.println();
        } catch (NoSuchElementException e) {
          return;
        }
      }
    }
  }
}
