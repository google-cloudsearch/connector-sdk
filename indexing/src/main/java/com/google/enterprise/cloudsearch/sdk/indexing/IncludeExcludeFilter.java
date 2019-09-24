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
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
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

  enum FilterType { REGEX, FILE_PREFIX, URL_PREFIX };

  @VisibleForTesting final ImmutableList<Rule> prefixIncludeRules;
  @VisibleForTesting final ImmutableList<Rule> prefixExcludeRules;
  @VisibleForTesting final ImmutableMap<ItemType, List<Rule>> regexIncludeRules;
  @VisibleForTesting final ImmutableMap<ItemType, List<Rule>> regexExcludeRules;

  public IncludeExcludeFilter(List<Rule> rules) {
    List<Rule> prefixIncludeList = rules.stream()
        .filter(r -> (r.getFilterType().equals(FilterType.FILE_PREFIX)
                || r.getFilterType().equals(FilterType.URL_PREFIX)))
        .filter(r -> r.getAction().equals(Action.INCLUDE))
        .collect(Collectors.toList());
    prefixIncludeRules = ImmutableList.copyOf(prefixIncludeList);

    List<Rule> prefixExcludeList = rules.stream()
        .filter(r -> (r.getFilterType().equals(FilterType.FILE_PREFIX)
                || r.getFilterType().equals(FilterType.URL_PREFIX)))
        .filter(r -> r.getAction().equals(Action.EXCLUDE))
        .collect(Collectors.toList());
    prefixExcludeRules = ImmutableList.copyOf(prefixExcludeList);

    Map<ItemType, List<Rule>> regexIncludeMap = rules.stream()
        .filter(r -> r.getFilterType().equals(FilterType.REGEX))
        .filter(r -> r.getAction().equals(Action.INCLUDE))
        .collect(Collectors.groupingBy(r -> r.getItemType().get()));
    Arrays.stream(ItemType.values())
        .forEach(t -> regexIncludeMap.putIfAbsent(t, ImmutableList.of()));
    regexIncludeMap.replaceAll((k, v) -> ImmutableList.copyOf(v));
    regexIncludeRules = ImmutableMap.copyOf(regexIncludeMap);

    Map<ItemType, List<Rule>> regexExcludeMap = rules.stream()
        .filter(r -> r.getFilterType().equals(FilterType.REGEX))
        .filter(r -> r.getAction().equals(Action.EXCLUDE))
        .collect(Collectors.groupingBy(r -> r.getItemType().get()));
    Arrays.stream(ItemType.values())
        .forEach(t -> regexExcludeMap.putIfAbsent(t, ImmutableList.of()));
    regexExcludeMap.replaceAll((k, v) -> ImmutableList.copyOf(v));
    regexExcludeRules = ImmutableMap.copyOf(regexExcludeMap);
  }

  @Override
  public String toString() {
    return "Include: \n" + prefixIncludeRules + "\n" + regexIncludeRules
        + "\nExclude: \n" + prefixExcludeRules + "\n" + regexExcludeRules;
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
      return new IncludeExcludeFilter(ImmutableList.of());
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
    List<Rule> rules = new ArrayList<>();
    for (Rule.Builder builder : ruleBuilders.values()) {
      try {
        rules.add(builder.build());
      } catch (IllegalStateException | IllegalArgumentException e) {
        throw new InvalidConfigurationException(e.getMessage());
      }
    }
    return new IncludeExcludeFilter(rules);
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
    List<Rule> excludeRules = Stream.concat(prefixExcludeRules.stream(), regexExcludeRules.get(itemType).stream())
        .collect(Collectors.toList());
    boolean exclude =
        evaluateRules(excludeRules, value, false /* if no rules: nothing is excluded */);
    if (exclude) {
      logger.log(Level.FINEST, "Excluding " + value);
      return false;
    }

    boolean include =
        evaluateRules(prefixIncludeRules, value, true /* if no rules: everything is included */)
        &&
        evaluateRules(regexIncludeRules.get(itemType), value, true /* if no rules: everything is included */);
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
    final FilterType filterType;
    final String name;
    final Optional<ItemType> itemType;
    final Action action;
    final Predicate<String> predicate;

    private Rule(FilterType filterType, String name, Optional<ItemType> itemType,
        Action action, Predicate<String> predicate) {
      this.filterType = filterType;
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

    Action getAction() {
      return action;
    }

    Optional<ItemType> getItemType() {
      return itemType;
    }

    FilterType getFilterType() {
      return filterType;
    }

    Predicate<String> getPredicate() {
      return predicate;
    }

    @Override
    public String toString() {
      return name + ": " + action + " "
          + (itemType.isPresent() ? itemType : "ANY") + " matching " + predicate;
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
        checkState(!Strings.isNullOrEmpty(filterTypeConfig),
            "Rule filter type is missing: " + name);
        checkState(!Strings.isNullOrEmpty(filterPatternConfig),
            "Rule filter pattern is missing: " + name);
        checkState(!Strings.isNullOrEmpty(actionConfig), "Rule action is missing: " + name);

        // valueOf throws IllegalArgumentException
        Action action = Action.valueOf(actionConfig.toUpperCase());
        FilterType filterType = FilterType.valueOf(filterTypeConfig.toUpperCase());

        switch (filterType) {
          case REGEX:
            checkState(!Strings.isNullOrEmpty(itemTypeConfig), "Rule item type is missing: " + name);
            ItemType itemType = ItemType.valueOf(itemTypeConfig.toUpperCase());
            return new Rule(FilterType.REGEX,
                name, Optional.of(itemType), action, new RegexPredicate(filterPatternConfig));
          case FILE_PREFIX:
            checkState(itemTypeConfig == null, "Item type should not be set for prefix rules");
            return new Rule(FilterType.FILE_PREFIX,
                name, Optional.empty(), action, new FilePrefixPredicate(action, filterPatternConfig));
          case URL_PREFIX:
            checkState(itemTypeConfig == null, "Item type should not be set for prefix rules");
            return new Rule(FilterType.URL_PREFIX,
                name, Optional.empty(), action, new UrlPrefixPredicate(action, filterPatternConfig));
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
      if (input == null) {
        return false;
      }
      Matcher matcher = pattern.matcher(input);
      return matcher.find();
    }

    @Override
    public String toString() {
      return regex;
    }
  }

  private static class FilePrefixPredicate implements Predicate<String> {
    private final Action action;
    private final Path prefixPath;
    private final Path prefixRoot;
    private final List<Path> prefixComponents;

    private FilePrefixPredicate(Action action, String prefix) {
      this.action = action;
      prefixPath = Paths.get(prefix);
      if (!prefixPath.isAbsolute()) {
        throw new IllegalArgumentException("file prefix must be absolute");
      }
        prefixRoot = prefixPath.getRoot();
        ImmutableList.Builder<Path> builder = ImmutableList.builder();
        builder.add(prefixRoot);
        for (int i = 1; i <= prefixPath.getNameCount(); i++) {
          builder.add(prefixRoot.resolve(prefixPath.subpath(0, i)));
        }
        prefixComponents = builder.build();
    }

    @Override
    public boolean apply(@Nullable String input) {
      if (input == null) {
        return false;
      }
      Path inputPath = Paths.get(input);
      if (action.equals(Action.INCLUDE)) {
        for (Path p : prefixComponents) {
          if (p.equals(inputPath)) {
            return true;
          }
        }
      }
      return inputPath.startsWith(prefixPath);
    }

    @Override
    public String toString() {
      return prefixPath.toString();
    }
  }

  // Both host and path are matched case-insensitively.
  private static class UrlPrefixPredicate implements Predicate<String> {
    private final Action action;
    private final String configuredPrefix;
    private final String lowercasePrefix;
    //    private final List<String> hostValues;
    private final List<String> pathValues;

    private UrlPrefixPredicate(Action action, String configuredPrefix) {
      this.action = action;
      this.configuredPrefix = configuredPrefix;
      String tmp = configuredPrefix.toLowerCase();
      if (tmp.endsWith("/")) {
        tmp = tmp.substring(0, tmp.length() - 1);
      }
      this.lowercasePrefix = tmp;
      URL url;
      try {
        url = new URL(lowercasePrefix);
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(configuredPrefix + " is not a URL", e);
      }
      ImmutableList.Builder<String> hostBuilder = ImmutableList.builder();
      ImmutableList.Builder<String> pathBuilder = ImmutableList.builder();
      String path = url.getPath();
      if (path.isEmpty() || path.equals("/")) {
        // Just "http://host:port"
        pathBuilder.add(lowercasePrefix);
        pathBuilder.add(lowercasePrefix + "/");
      } else {
        String hostPart = lowercasePrefix.substring(0, lowercasePrefix.indexOf(path));
        pathBuilder.add(hostPart);
        pathBuilder.add(hostPart + "/");

        // For /a/b/c, build prefix matches {/a, /a/, /a/b, /a/b/, /a/b/c, /a/b/c/}
        String[] pathParts = path.substring(1).split("/"); // Avoid empty first element
        for (int i = 1; i <= pathParts.length; i++) {
          String pathPart = hostPart + "/" + String.join("/", Arrays.copyOfRange(pathParts, 0, i));
          pathBuilder.add(pathPart);
          pathBuilder.add(pathPart + "/");
        }
      }
      pathValues = pathBuilder.build();
    }

    @Override
    public boolean apply(@Nullable String input) {
      if (input == null) {
        return false;
      }
      try {
        new URL(input);
      } catch (MalformedURLException e) {
        logger.log(Level.FINE, input + " is not a valid URL", e);
        return false;
      }

      String lowercaseInput = input.toLowerCase();
      if (action.equals(Action.INCLUDE)) {
        if (pathValues.contains(lowercaseInput)) {
          return true;
        }
      } else {
        if (lowercaseInput.equals(lowercasePrefix)) {
          return true;
        }
      }
      // If the input isn't one of the prefix parts, and doesn't start with the
      // prefix, it isn't a match.
      if (!lowercaseInput.startsWith(lowercasePrefix)) {
        return false;
      }

      // If the input starts with the configured prefix, the next character must be a
      // separator; /apple should not match /applesauce, but should match /apple/sauce.
      char c = lowercaseInput.charAt(lowercasePrefix.length());
      switch (c) {
        case '/':
        case ';':
        case '?':
        case '#':
          return true;
        default:
          return false;
      }
    }

    @Override
    public String toString() {
      return configuredPrefix;
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
    /*
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
    */
  }
}
