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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
 * <p>See the documentation for your connector to find out if it includes support for
 * this class. Each connector determines which values are tested against configured
 * rules.
 *
 * <p>This class supports three types of filter rules: REGEX, FILE_PREFIX, and
 * URL_PREFIX. A REGEX rule is used to match values against any Java regular expression. A
 * FILE_PREFIX rule is used to match file system path values against a given absolute file
 * prefix. A URL_PREFIX rule is used to match URL values against a given URL prefix. This
 * class uses case-insensitive matching for all rule types. The connector-provided data to
 * be tested is assumed to be in the JVM's default locale; you can change the connector's
 * locale to control this.
 *
 * <p>Multiple rules can be specified.
 *
 * <p>Each rule is specified using either three or four configuration properties. A
 * unique, meaningful name is used to group the properties for a rule together.

 * <pre>
 * includeExcludeFilter.&lt;name&gt;.itemType
 * includeExcludeFilter.&lt;name&gt;.filterType
 * includeExcludeFilter.&lt;name&gt;.filterPattern
 * includeExcludeFilter.&lt;name&gt;.action
 * </pre>
 *
 * <ul>
 *
 * <li>{@code itemType} may only be specified for REGEX filters. The value must be either
 * CONTAINER_ITEM or CONTENT_ITEM. Specifying VIRTUAL_CONTAINER_ITEM in configuration is
 * an error. If VIRTUAL_CONTAINER_ITEM is passed at runtime to {@link #isAllowed(String,
 * ItemType)}, it will be mapped to CONTAINER_ITEM.
 * <p>
 * Separating REGEX filters by type allows you to write simpler rules, for example, when
 * filtering by file extension, to match files by extension without accidentally excluding
 * all folders from indexing.
 *
 * <li>{@code filterType} must be set to REGEX, FILE_PREFIX, or URL_PREFIX.
 *
 * <li>{@code filterPattern}
 * <ul>
 * <li>For REGEX filters, {@code filterPattern} must be set to a Java regular expression; see <a
 * href="https://docs.oracle.com/javase/7/docs/api/java/util/regex/Pattern.html"
 * >java.util.regex.Pattern</a>.  Since the property values are in a Java
 * Properties file, any backslash characters in the regular expression must be escaped.
 * Patterns will be matched using {@code java.util.regex.Matcher.find()}, which looks for
 * a subsequence matching the given pattern, so patterns need not match the entire input
 * value.
 *
 * <li>For FILE_PREFIX filters, {@code filterPattern} must be set to an absolute file path.
 *
 * <li>For URL_PREFIX filters, {@code filterPattern} must be set to an absolute URL. For
 * URL values, the default port for the URL protocol will match whether or not it's
 * actually present; for example, {@code http://host:80/folder} will match {@code
 * http://host/folder}.
 * </ul>
 *
 * <li>{@code action} must be set to {@code INCLUDE} or {@code EXCLUDE}.
 *
 * <p>If any EXCLUDE rules are configured for an item type, an item will be excluded if it
 * matches any of those rules. Otherwise, if any INCLUDE rules are configured for an item
 * type, an item must match at least one INCLUDE rule to be included. When no rules are
 * configured, all items of the given type are allowed. When combining FILE_PREFIX or
 * URL_PREFIX filters with REGEX filters in INCLUDE rules, an item must match at
 * least one of the FILE_PREFIX or URL_PREFIX rules and at least one of the REGEX filters.
 * </ul>
 *
 * <p>Examples:

 * <p>Index text files (by extension). This rule includes content items ending in {@code
 * .txt}. If this is the only configured rule, all CONTAINER_ITEM objects are also
 * included, since no rules are defined for that type. Including the containers is
 * important when indexing a hierarchical repository using a listing connector.

 * <pre>
 *  includeExcludeFilter.includeText.action = INCLUDE
 *  includeExcludeFilter.includeText.itemType = CONTENT_ITEM
 *  includeExcludeFilter.includeText.filterType = REGEX
 *  includeExcludeFilter.includeText.filterPattern = \\.txt$
 * </pre>
 *
 * <p>Include only content within a specified subfolder.  This rule will include {@code
 * \\host\shareFolder} and {@code \\host\shareFolder\folder} as well as {@code
 * \\host\shareFolder\folder\pathToInclude} and its contents.
 *
 * <pre>
 * includeExcludeFilter.pathToInclude.action = INCLUDE
 * includeExcludeFilter.pathToInclude.filterType = FILE_PREFIX
 * includeExcludeFilter.pathToInclude.filterPattern = \\\\host\\shareFolder\\folder\\pathToInclude
 * </pre>
 *
 * <p>Include only content within a specified subfolder of a web site. This rule will
 * include {@code https://example.com/} and {@code https://example.com/folder/} as well as
 * {@code https://example.com/folder/pathToInclude} and its contents.
 *
 * <pre>
 * includeExcludeFilter.includePublicFolder.action = INCLUDE
 * includeExcludeFilter.includePublicFolder.filterType = URL_PREFIX
 * includeExcludeFilter.includePublicFolder.filterPattern = https://example.com/folder/pathToInclude
 * </pre>
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

  @VisibleForTesting static final EnumSet<ItemType> allowedItemTypes =
      EnumSet.of(ItemType.CONTAINER_ITEM, ItemType.CONTENT_ITEM);

  enum Action { INCLUDE, EXCLUDE };

  enum FilterType { REGEX, FILE_PREFIX, URL_PREFIX };

  @VisibleForTesting final ImmutableList<Rule> prefixIncludeRules;
  @VisibleForTesting final ImmutableList<Rule> prefixExcludeRules;
  @VisibleForTesting final ImmutableMap<ItemType, ImmutableList<Rule>> regexIncludeRules;
  @VisibleForTesting final ImmutableMap<ItemType, ImmutableList<Rule>> regexExcludeRules;

  @VisibleForTesting
  IncludeExcludeFilter(List<Rule> rules) {
    prefixIncludeRules = rules.stream()
        .filter(r -> (r.getFilterType().equals(FilterType.FILE_PREFIX)
                || r.getFilterType().equals(FilterType.URL_PREFIX)))
        .filter(r -> r.getAction().equals(Action.INCLUDE))
        .collect(ImmutableList.toImmutableList());

    prefixExcludeRules = rules.stream()
        .filter(r -> (r.getFilterType().equals(FilterType.FILE_PREFIX)
                || r.getFilterType().equals(FilterType.URL_PREFIX)))
        .filter(r -> r.getAction().equals(Action.EXCLUDE))
        .collect(ImmutableList.toImmutableList());

    Map<ItemType, List<Rule>> regexIncludeMap = rules.stream()
        .filter(r -> r.getFilterType().equals(FilterType.REGEX))
        .filter(r -> r.getAction().equals(Action.INCLUDE))
        .collect(Collectors.groupingBy(r -> r.getItemType().get()));
    allowedItemTypes.stream()
        .forEach(t -> regexIncludeMap.putIfAbsent(t, ImmutableList.of()));
    ImmutableMap.Builder<ItemType, ImmutableList<Rule>> regexIncludeBuilder =
        ImmutableMap.builder();
    for (ItemType itemType : regexIncludeMap.keySet()) {
      regexIncludeBuilder.put(itemType, ImmutableList.copyOf(regexIncludeMap.get(itemType)));
    }
    regexIncludeRules = regexIncludeBuilder.build();

    Map<ItemType, List<Rule>> regexExcludeMap = rules.stream()
        .filter(r -> r.getFilterType().equals(FilterType.REGEX))
        .filter(r -> r.getAction().equals(Action.EXCLUDE))
        .collect(Collectors.groupingBy(r -> r.getItemType().get()));
    allowedItemTypes.stream()
        .forEach(t -> regexExcludeMap.putIfAbsent(t, ImmutableList.of()));
    ImmutableMap.Builder<ItemType, ImmutableList<Rule>> regexExcludeBuilder =
        ImmutableMap.builder();
    for (ItemType itemType : regexExcludeMap.keySet()) {
      regexExcludeBuilder.put(itemType, ImmutableList.copyOf(regexExcludeMap.get(itemType)));
    }
    regexExcludeRules = regexExcludeBuilder.build();
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
   * @param itemType the type of item; values of {@code VIRTUAL_CONTAINER_ITEM} will be
   * mapped to {@code CONTAINER_ITEM}
   * @return true if the value is included based on the configuration
   */
  public boolean isAllowed(String value, ItemType itemType) {
    if (itemType.equals(ItemType.VIRTUAL_CONTAINER_ITEM)) {
      itemType = ItemType.CONTAINER_ITEM;
    }
    ImmutableList<Rule> excludeRules =
        Stream.concat(prefixExcludeRules.stream(), regexExcludeRules.get(itemType).stream())
        .collect(ImmutableList.toImmutableList());
    // If no rules: nothing is excluded
    boolean exclude = evaluateRules(excludeRules, value, false);
    if (exclude) {
      logger.log(Level.FINEST, "Excluding " + value);
      return false;
    }

    // If no rules: everything is included
    boolean include =
        evaluateRules(prefixIncludeRules, value, true)
        && evaluateRules(regexIncludeRules.get(itemType), value, true);
    logger.log(Level.FINEST, (include ? "Including " : "Not including ") + value);
    return include;
  }

  /**
   * Evalues the list of rules for the given value.
   *
   * @return {@code true} when any of the rules evaluates to {@code true} for the given
   * value. When the list of rules is empty, returns {@code emptyRulesOutcome}.
   */
  private boolean evaluateRules(ImmutableList<Rule> rules, String value,
      boolean emptyRulesOutcome) {
    if (rules.isEmpty()) {
      return emptyRulesOutcome;
    }
    // Return true if any of the rule evaulations return true.
    return rules.stream().map(r -> r.eval(value)).anyMatch(evalResult -> evalResult);
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
      return name + ": " + filterType + " " + action + " "
          + (itemType.isPresent() ? itemType.get() : "ANY")
          + " matching " + predicate;
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
        Action action = Action.valueOf(actionConfig.toUpperCase(Locale.US));
        FilterType filterType = FilterType.valueOf(filterTypeConfig.toUpperCase(Locale.US));

        switch (filterType) {
          case REGEX:
            checkState(!Strings.isNullOrEmpty(itemTypeConfig), "Item type is missing: " + name);
            ItemType itemType = ItemType.valueOf(itemTypeConfig.toUpperCase(Locale.US));
            if (!allowedItemTypes.contains(itemType)) {
              throw new IllegalArgumentException(itemTypeConfig);
            }
            return new Rule(FilterType.REGEX,
                name, Optional.of(itemType), action, new RegexPredicate(filterPatternConfig));
          case FILE_PREFIX:
            checkState(itemTypeConfig == null, "Item type should not be set for prefix rules");
            return new Rule(FilterType.FILE_PREFIX, name, Optional.empty(),
                action, new FilePrefixPredicate(action, filterPatternConfig));
          case URL_PREFIX:
            checkState(itemTypeConfig == null, "Item type should not be set for prefix rules");
            return new Rule(FilterType.URL_PREFIX, name, Optional.empty(),
                action, new UrlPrefixPredicate(action, filterPatternConfig));
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
      this.pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
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
    private final String configuredPrefix;
    private final Path prefixPath;
    private final Path prefixRoot;
    private final List<Path> prefixComponents;

    private FilePrefixPredicate(Action action, String prefix) {
      this.action = action;
      configuredPrefix = prefix;
      prefixPath = Paths.get(prefix.toLowerCase());
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
      Path inputPath = Paths.get(input.toLowerCase());
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
      return configuredPrefix;
    }
  }

  // Both host and path are matched case-insensitively.
  private static class UrlPrefixPredicate implements Predicate<String> {
    private final Action action;
    private final String configuredPrefix;
    private final String lowercasePrefixWithoutSlash;
    private final List<String> pathValues;

    private UrlPrefixPredicate(Action action, String configuredPrefix) {
      this.action = action;
      this.configuredPrefix = configuredPrefix;

      URL url;
      String hostValue;
      try {
        url = new URL(configuredPrefix.toLowerCase());
        hostValue = getHostValue(url);
        String tmp = getUrlWithoutPort(url);
        if (tmp.endsWith("/")) {
          tmp = tmp.substring(0, tmp.length() - 1);
        }
        this.lowercasePrefixWithoutSlash = tmp.toLowerCase();
      } catch (MalformedURLException e) {
        throw new IllegalArgumentException(configuredPrefix + " is not a URL", e);
      }
      ImmutableList.Builder<String> pathBuilder = ImmutableList.builder();
      pathBuilder.add(hostValue);
      pathBuilder.add(hostValue + "/");
      String path = url.getPath();
      if (!(path.isEmpty() || path.equals("/"))) {
        // For /a/b/c, build prefix matches {/a, /a/, /a/b, /a/b/, /a/b/c, /a/b/c/}
        String[] pathParts = path.substring(1).split("/"); // Avoid empty first element
        for (int i = 1; i <= pathParts.length; i++) {
          String pathPart = hostValue + "/" + String.join("/", Arrays.copyOfRange(pathParts, 0, i));
          pathBuilder.add(pathPart);
          pathBuilder.add(pathPart + "/");
        }
      }
      pathValues = pathBuilder.build();
    }

    /**
     * If the URL contains a default port value, returns the URL without the port.
     */
    private String getUrlWithoutPort(URL url) throws MalformedURLException {
      return getHostValue(url) + url.getFile();
    }

    /**
     * Returns just the protocol://host[:non-default-port] part of the URL.
     * Example: http://x.com -> http://x.com
     *          http://x.com:80 -> http://x.com
     *          http://x.com:4321 -> http://x.com:4321
     */
    private String getHostValue(URL url) throws MalformedURLException {
      int port = url.getPort();
      int defaultPort = url.getDefaultPort();
      if ((port == -1 && defaultPort != -1) || (port == defaultPort)) {
        return new URL(url.getProtocol(), url.getHost(), "").toString();
      } else {
        return new URL(url.getProtocol(), url.getHost(), url.getPort(), "").toString();
      }
    }

    @Override
    public boolean apply(@Nullable String input) {
      if (input == null) {
        return false;
      }

      String lowercaseInput;
      try {
        // Remove default port values for comparison
        lowercaseInput = getUrlWithoutPort(new URL(input.toLowerCase()));
      } catch (MalformedURLException e) {
        logger.log(Level.FINE, input + " is not a valid URL", e);
        return false;
      }

      if (action.equals(Action.INCLUDE)) {
        if (pathValues.contains(lowercaseInput)) {
          return true;
        }
      } else {
        if (lowercaseInput.equals(lowercasePrefixWithoutSlash)) {
          return true;
        }
      }
      // If the input isn't one of the prefix parts, and doesn't start with the
      // prefix, it isn't a match.
      if (!lowercaseInput.startsWith(lowercasePrefixWithoutSlash)) {
        return false;
      }

      // If the input starts with the configured prefix, the next character must be a
      // separator; /apple should not match /applesauce, but should match /apple/sauce.
      char c = lowercaseInput.charAt(lowercasePrefixWithoutSlash.length());
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
    Configuration.initConfig(args);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    System.out.println("Rules:");
    Stream.concat(filter.prefixIncludeRules.stream(), filter.prefixExcludeRules.stream())
        .forEach(rule -> System.out.println("*** " + rule));
    Stream.concat(
        filter.regexIncludeRules.values().stream().filter(list -> !list.isEmpty()),
        filter.regexExcludeRules.values().stream().filter(list -> !list.isEmpty()))
        .forEach(list -> list.stream().forEach(rule -> System.out.println("*** " + rule)));
    System.out.println();

    System.out.println("Enter test value(s)");

    Scanner in = new Scanner(inStream);
    while (in.hasNext()) {
      String value = in.next();
      System.out.println(value);
      for (ItemType itemType : ItemType.values()) {
        System.out.println("  as " + itemType.name() + ": "
            + filter.isAllowed(value, itemType));
      }
      System.out.println();
    }
  }
}
