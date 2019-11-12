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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;

import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.PatternSyntaxException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for IncludeExcludeFilter.
 */
@RunWith(JUnit4.class)
public class IncludeExcludeFilterTest {
  private static final boolean IS_WINDOWS = System.getProperty("file.separator").equals("\\");

  @Rule public ErrorCollector errorCollector = new ErrorCollector();
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void ruleBuilder_allValuesSet_succeeds() {
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("REGEX")
        .setFilterPattern("file.txt")
        .setAction("INCLUDE")
        .build();
    assertEquals("testRule", rule.getName());
    assertEquals(ItemType.CONTENT_ITEM, rule.getItemType().get());
    assertEquals(IncludeExcludeFilter.Action.INCLUDE, rule.getAction());
  }

  @Test
  public void ruleBuilder_missingName_throwsException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Rule name");
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder(null)
        .build();
  }

  @Test
  public void ruleBuilder_regexMissingItemType_throwsException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Item type");
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setFilterType("REGEX")
        .setFilterPattern("file.txt")
        .setAction("INCLUDE")
        .build();
  }

  @Test
  public void ruleBuilder_regexInvalidItemType_throwsException() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("VIRTUAL_CONTAINER_ITEM");
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("VIRTUAL_CONTAINER_ITEM")
        .setFilterType("REGEX")
        .setFilterPattern("file.txt")
        .setAction("INCLUDE")
        .build();
  }

  @Test
  public void ruleBuilder_prefixMissingItemType_isEmpty() {
    String pattern = IS_WINDOWS ? "\\\\share\\folder" : "/folder";
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setFilterType("FILE_PREFIX")
        .setFilterPattern(pattern)
        .setAction("INCLUDE")
        .build();
    assertTrue(!rule.getItemType().isPresent());
  }

  @Test
  public void ruleBuilder_prefixHasItemType_throwsException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Item type should not be set");
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("FILE_PREFIX")
        .setFilterPattern("/file")
        .setAction("INCLUDE")
        .build();
  }

  @Test
  public void ruleBuilder_missingFilterType_throwsException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Rule filter type");
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .build();
  }

  @Test
  public void ruleBuilder_missingFilterPattern_throwsException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Rule filter pattern");
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("REGEX")
        .build();
  }

  @Test
  public void ruleBuilder_missingAction_throwsException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Rule action");
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("REGEX")
        .setFilterPattern("pattern")
        .build();
  }

  @Test
  public void ruleBuilder_badItemType_throwsException() {
    thrown.expect(IllegalArgumentException.class);
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("DOCUMENT")
        .setFilterType("REGEX")
        .setFilterPattern("pattern")
        .setAction("INCLUDE")
        .build();
  }

  @Test
  public void ruleBuilder_badFilterType_throwsException() {
    thrown.expect(IllegalArgumentException.class);
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("EXACT")
        .setFilterPattern("pattern")
        .setAction("INCLUDE")
        .build();
  }

  @Test
  public void ruleBuilder_badAction_throwsException() {
    thrown.expect(IllegalArgumentException.class);
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("REGEX")
        .setFilterPattern("pattern")
        .setAction("TOSS")
        .build();
  }

  @Test
  public void ruleBuilder_badRegex_throwsException() {
    thrown.expect(PatternSyntaxException.class);
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("REGEX")
        .setFilterPattern("*")
        .setAction("EXCLUDE")
        .build();
  }

  @Test
  public void regexRule_configInvalidPattern_throwsException() {
    thrown.expect(PatternSyntaxException.class);
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("REGEX")
        .setAction("INCLUDE")
        .setFilterPattern("*")
        .build();
  }

  @Test
  public void regexRule_nullInput_returnsFalse() {
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setItemType("CONTENT_ITEM")
        .setFilterType("REGEX")
        .setAction("INCLUDE")
        .setFilterPattern("any")
        .build();
    assertFalse(rule.eval(null));
  }

  @Test
  public void filePrefixRule_configInvalidPath_throwsException() {
    thrown.expect(IllegalArgumentException.class);
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setFilterType("FILE_PREFIX")
        .setAction("INCLUDE")
        .setFilterPattern("not-an-absolute-path")
        .build();
  }

  @Test
  public void filePrefixRule_nullPath_returnsFalse() {
    String pattern = IS_WINDOWS ? "\\\\share\\folder" : "/folder";
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
        .setFilterType("FILE_PREFIX")
        .setFilterPattern(pattern)
        .setAction("INCLUDE")
        .build();
    assertFalse(rule.eval(null));
  }

  @Test
  public void filePrefixIncludeRule_acceptsExpectedPatterns_unix() {
    assumeThat(IS_WINDOWS, is(false));
    List<IncludeExcludeFilter.Rule> rules;

    rules = Arrays.asList(
        filePrefixRule("/", "INCLUDE"));
    eval(true, rules,
        "/",
        "/folder",
        "/folder/",
        "/folder/path",
        "/folder/path/",
        "/folder/path/file.txt",
        "/folder/path/nestedfolder/file.txt");

    rules = Arrays.asList(
        filePrefixRule("/folder", "INCLUDE"),
        filePrefixRule("/folder/", "INCLUDE"));
    eval(true, rules,
        "/",
        "/folder",
        "/folder/",
        "/FOLDER",
        "/FOLDER/",
        "/folder/path",
        "/folder/path/",
        "/folder/path/file.txt",
        "/folder/path/nestedfolder/file.txt");
    eval(false, rules,
        "/folderWithLongerName",
        "/otherFolder",
        "/folder.txt");

    rules = Arrays.asList(
        filePrefixRule("/folder/path", "INCLUDE"),
        filePrefixRule("/folder/path/", "INCLUDE"));
    eval(true, rules,
        "/",
        "/folder",
        "/folder/",
        "/folder/path",
        "/folder/path/",
        "/folder/path/file.txt",
        "/folder/path/nestedfolder/file.txt");
    eval(false, rules,
        "/folderWithLongerName",
        "/folder/otherSubfolder",
        "/folder/pathWithLongerName",
        "/folder/file.txt",
        "/folder/subfolder/file.txt");
  }

  @Test
  public void filePrefixIncludeRule_acceptsExpectedPatterns_windows() {
    assumeThat(IS_WINDOWS, is(true));
    List<IncludeExcludeFilter.Rule> rules;

    rules = Arrays.asList(
        filePrefixRule("\\\\share\\folder", "INCLUDE"),
        filePrefixRule("\\\\share\\folder\\", "INCLUDE"));
    eval(true, rules,
        "\\\\share\\folder",
        "\\\\share\\folder\\path",
        "\\\\share\\folder\\path\\",
        "\\\\share\\folder\\path\\path1",
        "\\\\share\\folder\\path\\path1\\",
        "\\\\share\\folder\\path\\path1\\file.txt");
    eval(false, rules,
        "\\\\share\\fold",
        "\\\\share\\folder1",
        "\\\\share\\folder1\\path");

    rules = Arrays.asList(
        filePrefixRule("\\\\share\\folder\\path", "INCLUDE"),
        filePrefixRule("\\\\share\\folder\\path\\", "INCLUDE"));
    eval(true, rules,
        "\\\\share\\folder",
        "\\\\share\\folder\\path",
        "\\\\share\\folder\\path\\",
        "\\\\share\\folder\\path\\path1",
        "\\\\share\\folder\\path\\path1\\",
        "\\\\share\\folder\\path\\path1\\file.txt");
    eval(false, rules,
        "\\\\share\\folderWithLongerName",
        "\\\\share\\folder\\pathToOtherFolder");
  }

  @Test
  public void filePrefixExcludeRule_acceptsExpectedPatterns_unix() {
    assumeThat(IS_WINDOWS, is(false));
    List<IncludeExcludeFilter.Rule> rules;

    rules = Arrays.asList(
        filePrefixRule("/folder", "EXCLUDE"));
    eval(true, rules,
        "/folder",
        "/folder/",
        "/folder/path",
        "/folder/path/",
        "/folder/path/file.txt",
        "/folder/path/nestedfolder/file.txt");
    eval(false, rules,
        "/",
        "/folderWithLongerName/",
        "/otherFolder/path/nestedfolder/file.txt");
  }

  @Test
  public void filePrefixExcludeRule_acceptsExpectedPatterns_windows() {
    assumeThat(IS_WINDOWS, is(true));
    List<IncludeExcludeFilter.Rule> rules;

    rules = Arrays.asList(
        filePrefixRule("\\\\share\\folder\\path", "EXCLUDE"),
        filePrefixRule("\\\\share\\folder\\path\\", "EXCLUDE"));
    eval(true, rules,
        "\\\\share\\folder\\path",
        "\\\\share\\folder\\path\\",
        "\\\\share\\folder\\path\\nestedfolder",
        "\\\\share\\folder\\path\\nestedfolder\\",
        "\\\\share\\folder\\path\\file.txt");
    eval(false, rules,
        "\\\\share\\folder",
        "\\\\share\\folder\\",
        "\\\\share\\folder\\otherpath",
        "\\\\share\\folder\\otherpath\\",
        "\\\\share\\folder\\file.txt",
        "\\\\share\\folder\\pathWithMoreText\\file.txt");
  }

  @Test
  public void urlPrefixRule_configInvalidUrl_throwsException() {
    thrown.expect(IllegalArgumentException.class);
    urlPrefixRule("not-an-url", "INCLUDE");
  }

  @Test
  public void urlPrefixRule_nullUrl_returnsFalse() {
    IncludeExcludeFilter.Rule rule = urlPrefixRule("http://www.example.com", "INCLUDE");
    assertFalse(rule.eval(null));
  }

  @Test
  public void urlPrefixRule_invalidUrl_returnsFalse() {
    IncludeExcludeFilter.Rule rule = urlPrefixRule("http://www.example.com", "INCLUDE");
    assertFalse(rule.eval("not-an-url"));
  }

  @Test
  public void urlPrefixRule_acceptsExpectedPatterns() {
    List<IncludeExcludeFilter.Rule> rules;

    rules = Arrays.asList(
        urlPrefixRule("http://www.example.com", "INCLUDE"),
        urlPrefixRule("http://www.example.com/", "INCLUDE"));
    eval(true, rules,
        "http://www.example.com",
        "http://www.example.com/",
        "http://WWW.EXAMPLE.COM",
        "http://WWW.EXAMPLE.COM/",
        "http://www.example.com/anyPath");
    eval(false, rules,
        "http://docs.example.com",
        "http://docs.example.com/",
        "https://www.example.com",
        "http://other.example.com");

    rules = Arrays.asList(
        urlPrefixRule("http://www.example.com:1234", "INCLUDE"),
        urlPrefixRule("http://www.example.com:1234/", "INCLUDE"));
    eval(true, rules,
        "http://www.example.com:1234",
        "http://www.example.com:1234/",
        "http://WWW.EXAMPLE.COM:1234",
        "http://WWW.EXAMPLE.COM:1234/",
        "http://www.example.com:1234/?x=y",
        "http://www.example.com:1234/anyPath");
    eval(false, rules,
        "http://www.example.com:123",
        "http://www.example.com:12345",
        "http://www.example.com:12345/",
        "https://www.example.com:1234",
        "http://other.example.com:1234");

    rules = Arrays.asList(
        urlPrefixRule("http://www.example.com/folder", "INCLUDE"),
        urlPrefixRule("http://www.example.com/folder/", "INCLUDE"));
    eval(true, rules,
        "http://www.example.com",
        "http://www.example.com/",
        "http://www.example.com/folder",
        "http://www.example.com/folder/",
        "http://www.example.com/folder/file.txt",
        "http://www.example.com/folder/path/to/file.txt",
        "http://www.example.com/folder?x=11",
        "http://www.example.com/folder/path?x=11",
        "http://www.example.com/folder;x=11",
        "http://www.example.com/folder#id");
    eval(false, rules,
        "http://docs.example.com/",
        "http://www.example.com/otherfolder",
        "http://www.example.com/folderWithLongerName",
        "http://www.example.com/file.txt");
  }

  @Test
  public void urlPrefixRule_defaultPort_acceptsExpectedPatterns() {
    List<IncludeExcludeFilter.Rule> rules;

    rules = Arrays.asList(
        urlPrefixRule("http://www.example.com", "INCLUDE"),
        urlPrefixRule("http://www.example.com/", "INCLUDE"),
        urlPrefixRule("http://www.example.com:80", "INCLUDE"),
        urlPrefixRule("http://www.example.com:80/", "INCLUDE"));
    eval(true, rules,
        "http://www.example.com",
        "http://www.example.com/",
        "http://www.example.com:80",
        "http://www.example.com:80/",
        "http://www.example.com/folder/file.txt",
        "http://www.example.com:80/folder/file.txt"
      );

    rules = Arrays.asList(
        urlPrefixRule("http://www.example.com/folder/path", "INCLUDE"),
        urlPrefixRule("http://www.example.com:80/folder/path", "INCLUDE"));
    eval(true, rules,
        "http://www.example.com",
        "http://www.example.com/",
        "http://www.example.com:80",
        "http://www.example.com:80/",
        "http://www.example.com:80/folder",
        "http://www.example.com/folder/path",
        "http://www.example.com/folder/path/",
        "http://www.example.com:80/folder/path",
        "http://www.example.com:80/folder/path/",
        "http://www.example.com/folder/path/file.txt",
        "http://www.example.com:80/folder/path/file.txt"
      );
    eval(false, rules,
        "http://www.example.com/folder/file.txt",
        "http://www.example.com:80/folder/file.txt",
        "http://www.example.com/folder2/file.txt",
        "http://www.example.com/folder/path2"
      );

    rules = Arrays.asList(
        urlPrefixRule("https://www.example.com", "INCLUDE"),
        urlPrefixRule("https://www.example.com/", "INCLUDE"),
        urlPrefixRule("https://www.example.com:443", "INCLUDE"),
        urlPrefixRule("https://www.example.com:443/", "INCLUDE"));
    eval(true, rules,
        "https://www.example.com",
        "https://www.example.com/",
        "https://www.example.com:443",
        "https://www.example.com:443/",
        "https://www.example.com/folder/file.txt",
        "https://www.example.com:443/folder/file.txt"
      );

    rules = Arrays.asList(
        urlPrefixRule("http://www.example.com:1234", "INCLUDE"),
        urlPrefixRule("http://www.example.com:1234/", "INCLUDE"));
    eval(false, rules,
        "http://www.example.com",
        "http://www.example.com:80");
  }

  @Test
  public void urlPrefixExcludeRule_acceptsExpectedPatterns() {
    List<IncludeExcludeFilter.Rule> rules;

    rules = Arrays.asList(
        urlPrefixRule("http://www.example.com/folder", "EXCLUDE"),
        urlPrefixRule("http://www.example.com/folder/", "EXCLUDE"));
    eval(true, rules,
        "http://www.example.com/folder",
        "http://www.example.com/folder/",
        "http://www.example.com/folder/file.txt");
    eval(false, rules,
        "http://www.example.com",
        "http://www.example.com/",
        "http://www.example.com/folderWithLongerName",
        "http://www.example.com/otherFolder");
  }

  private IncludeExcludeFilter.Rule filePrefixRule(String prefix, String action) {
    return new IncludeExcludeFilter.Rule.Builder("testRule")
        .setFilterType("FILE_PREFIX")
        .setAction(action)
        .setFilterPattern(prefix)
        .build();
  }

  private IncludeExcludeFilter.Rule urlPrefixRule(String prefix, String action) {
    return new IncludeExcludeFilter.Rule.Builder("testRule")
        .setFilterType("URL_PREFIX")
        .setAction(action)
        .setFilterPattern(prefix)
        .build();

  }

  private void eval(boolean expected, List<IncludeExcludeFilter.Rule> rules, String... values) {
    for (IncludeExcludeFilter.Rule rule : rules) {
      for (String s : values) {
        errorCollector.checkThat(rule + " = " + s, rule.eval(s), is(expected));
      }
    }
  }

  @Test
  public void constructor_sortsRules() {
    List<IncludeExcludeFilter.Rule> rules = Arrays.asList(
        new IncludeExcludeFilter.Rule.Builder("rule1").setFilterType("URL_PREFIX")
        .setAction("INCLUDE").setFilterPattern("http://example.com/a").build(),
        new IncludeExcludeFilter.Rule.Builder("rule2").setFilterType("URL_PREFIX")
        .setAction("INCLUDE").setFilterPattern("http://example.com/b").build(),
        new IncludeExcludeFilter.Rule.Builder("rule3").setFilterType("URL_PREFIX")
        .setAction("EXCLUDE").setFilterPattern("http://private.example.com/").build(),
        new IncludeExcludeFilter.Rule.Builder("rule4").setFilterType("REGEX")
        .setAction("INCLUDE").setFilterPattern("\\.html$").setItemType("CONTENT_ITEM").build(),
        new IncludeExcludeFilter.Rule.Builder("rule5").setFilterType("REGEX")
        .setAction("INCLUDE").setFilterPattern("\\.pdf$").setItemType("CONTENT_ITEM").build(),
        new IncludeExcludeFilter.Rule.Builder("rule6").setFilterType("REGEX")
        .setAction("EXCLUDE").setFilterPattern("\\.pdf.bak$").setItemType("CONTENT_ITEM").build(),
        new IncludeExcludeFilter.Rule.Builder("rule6").setFilterType("REGEX")
        .setAction("EXCLUDE").setFilterPattern("\\.doc.bak$").setItemType("CONTENT_ITEM").build(),
        new IncludeExcludeFilter.Rule.Builder("rule6").setFilterType("REGEX")
        .setAction("EXCLUDE").setFilterPattern("\\.txt.bak$").setItemType("CONTENT_ITEM").build());
    IncludeExcludeFilter filter = new IncludeExcludeFilter(rules);

    assertSizeEquals(filter.prefixIncludeRules, 2);
    assertSizeEquals(filter.prefixExcludeRules, 1);
    assertSizeEquals(filter.regexIncludeRules.get(ItemType.CONTENT_ITEM), 2);
    assertSizeEquals(filter.regexExcludeRules.get(ItemType.CONTENT_ITEM), 3);
  }

  @Test
  public void fromConfiguration_notInitialized_throwsException() {
    thrown.expect(IllegalStateException.class);
    IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void fromConfiguration_initializedNoConfig_emptyRulesCreated() {
    setupConfig.initConfig(new Properties());
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertSizeEquals(filter.prefixIncludeRules, 0);
    assertSizeEquals(filter.prefixExcludeRules, 0);
    assertEquals(IncludeExcludeFilter.allowedItemTypes.size(), filter.regexIncludeRules.size());
    assertEquals(IncludeExcludeFilter.allowedItemTypes.size(), filter.regexExcludeRules.size());
    for (ItemType itemType : IncludeExcludeFilter.allowedItemTypes) {
      assertSizeEquals(filter.regexIncludeRules.get(itemType), 0);
      assertSizeEquals(filter.regexExcludeRules.get(itemType), 0);
    }
    assertTrue(filter.isAllowed("anything is allowed", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("anything is allowed", ItemType.CONTAINER_ITEM));
  }

  @Test
  public void fromConfiguration_invalidProperty_throwsException() {
    Properties config = new Properties();
    config.setProperty("includeExcludeFilter.propertyTest.unknownProperty", "prop value");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("Unknown property includeExcludeFilter.propertyTest.unknownProperty");
    IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void fromConfiguration_invalidPropertyFormat_throwsException() {
    Properties config = new Properties();
    config.setProperty(
        "includeExcludeFilter.propertyTest.unknownProperty.unknownElement", "prop value");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "Unknown property includeExcludeFilter.propertyTest.unknownProperty.unknownElement");
    IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void fromConfiguration_regexRules_rulesCreated() throws IOException {
    Properties config = createProperties(
        "includeExcludeFilter.includeText.itemType = CONTENT_ITEM",
        "includeExcludeFilter.includeText.filterType = REGEX",
        "includeExcludeFilter.includeText.filterPattern = \\\\.txt$",
        "includeExcludeFilter.includeText.action = INCLUDE",

        "includeExcludeFilter.includeHtml.itemType = CONTENT_ITEM",
        "includeExcludeFilter.includeHtml.filterType = REGEX",
        "includeExcludeFilter.includeHtml.filterPattern = \\\\.html$",
        "includeExcludeFilter.includeHtml.action = INCLUDE",

        "includeExcludeFilter.excludeHtml.itemType = CONTENT_ITEM",
        "includeExcludeFilter.excludeHtml.filterType = REGEX",
        "includeExcludeFilter.excludeHtml.filterPattern = excluded\\\\.html$",
        "includeExcludeFilter.excludeHtml.action = EXCLUDE"
      );
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    List<IncludeExcludeFilter.Rule> includeRules =
        filter.regexIncludeRules.get(ItemType.CONTENT_ITEM);
    assertSizeEquals(includeRules, 2);

    IncludeExcludeFilter.Rule rule = includeRules.stream()
        .filter(r -> r.getName().equals("includeText"))
        .findFirst()
        .get();
    assertEquals(ItemType.CONTENT_ITEM, rule.getItemType().get());
    assertEquals(IncludeExcludeFilter.Action.INCLUDE, rule.getAction());
    assertEquals("\\.txt$", rule.getPredicate().toString());

    rule = includeRules.stream()
        .filter(r -> r.getName().equals("includeHtml"))
        .findFirst()
        .get();
    assertEquals(ItemType.CONTENT_ITEM, rule.getItemType().get());
    assertEquals(IncludeExcludeFilter.Action.INCLUDE, rule.getAction());
    assertEquals("\\.html$", rule.getPredicate().toString());

    List<IncludeExcludeFilter.Rule> excludeRules =
        filter.regexExcludeRules.get(ItemType.CONTENT_ITEM);
    assertSizeEquals(excludeRules, 1);

    rule = excludeRules.stream()
        .filter(r -> r.getName().equals("excludeHtml"))
        .findFirst()
        .get();
    assertEquals(ItemType.CONTENT_ITEM, rule.getItemType().get());
    assertEquals(IncludeExcludeFilter.Action.EXCLUDE, rule.getAction());
    assertEquals("excluded\\.html$", rule.getPredicate().toString());
  }

  @Test
  public void fromConfiguration_prefixRules_rulesCreated_unix() throws IOException {
    assumeThat(IS_WINDOWS, is(false));
    Properties config = createProperties(
        "includeExcludeFilter.includeText.filterType = FILE_PREFIX",
        "includeExcludeFilter.includeText.filterPattern = /path/to/records",
        "includeExcludeFilter.includeText.action = INCLUDE",

        "includeExcludeFilter.includeHtml.filterType = FILE_PREFIX",
        "includeExcludeFilter.includeHtml.filterPattern = /path/to/records/butNotThese",
        "includeExcludeFilter.includeHtml.action = EXCLUDE"
      );
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    List<IncludeExcludeFilter.Rule> includeRules = filter.prefixIncludeRules;
    assertSizeEquals(includeRules, 1);
    IncludeExcludeFilter.Rule rule = includeRules.get(0);
    assertEquals(IncludeExcludeFilter.Action.INCLUDE, rule.getAction());
    assertEquals("/path/to/records", rule.getPredicate().toString());

    List<IncludeExcludeFilter.Rule> excludeRules = filter.prefixExcludeRules;
    assertSizeEquals(excludeRules, 1);
    rule = excludeRules.get(0);
    assertEquals(IncludeExcludeFilter.Action.EXCLUDE, rule.getAction());
    assertEquals("/path/to/records/butNotThese", rule.getPredicate().toString());
  }

  @Test
  public void fromConfiguration_prefixRules_rulesCreated_windows() throws IOException {
    assumeThat(IS_WINDOWS, is(true));
    Properties config = createProperties(
        "includeExcludeFilter.includeText.filterType = FILE_PREFIX",
        "includeExcludeFilter.includeText.filterPattern = \\\\\\\\share\\\\folder\\\\path",
        "includeExcludeFilter.includeText.action = INCLUDE",

        "includeExcludeFilter.includeHtml.filterType = FILE_PREFIX",
        "includeExcludeFilter.includeHtml.filterPattern = "
            + "\\\\\\\\share\\\\folder\\\\path\\\\notThese",
        "includeExcludeFilter.includeHtml.action = EXCLUDE"
      );
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    List<IncludeExcludeFilter.Rule> includeRules = filter.prefixIncludeRules;
    assertSizeEquals(includeRules, 1);
    IncludeExcludeFilter.Rule rule = includeRules.get(0);
    assertEquals(IncludeExcludeFilter.Action.INCLUDE, rule.getAction());
    assertEquals("\\\\share\\folder\\path", rule.getPredicate().toString());

    List<IncludeExcludeFilter.Rule> excludeRules = filter.prefixExcludeRules;
    assertSizeEquals(excludeRules, 1);
    rule = excludeRules.get(0);
    assertEquals(IncludeExcludeFilter.Action.EXCLUDE, rule.getAction());
    assertEquals("\\\\share\\folder\\path\\notThese", rule.getPredicate().toString());
  }

  @Test
  public void fromConfiguration_regexIncludeRule_succeeds() throws IOException {
    Properties config = createProperties(
        "includeExcludeFilter.rule1.itemType = CONTENT_ITEM",
        "includeExcludeFilter.rule1.filterType = REGEX",
        "includeExcludeFilter.rule1.filterPattern = \\\\.txt$",
        "includeExcludeFilter.rule1.action = INCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.isAllowed("/path", ItemType.VIRTUAL_CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/to", ItemType.VIRTUAL_CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/to", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/to/file.txt", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("file.txt", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("File.TXT", ItemType.CONTENT_ITEM));

    // All containers should be allowed
    assertTrue(filter.isAllowed("filetxt", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("file.txt", ItemType.CONTAINER_ITEM));

    assertFalse(filter.isAllowed("/path/to/file.pdf", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/path/to/file.txt.bak", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("filetxt", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_regexExcludeRule_succeeds() throws IOException {
    Properties config = createProperties(
        "includeExcludeFilter.rule1.itemType = CONTENT_ITEM",
        "includeExcludeFilter.rule1.filterType = REGEX",
        "includeExcludeFilter.rule1.filterPattern = \\\\.txt$",
        "includeExcludeFilter.rule1.action = EXCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    assertTrue(filter.isAllowed("/path", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/to", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("filetxt", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("file.txt", ItemType.CONTAINER_ITEM));

    assertTrue(filter.isAllowed("/path/to/file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/to/file.txt.bak", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("filetxt", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("/path/to/file.txt", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("file.txt", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("File.TXT", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_regexContainerRule_succeedsForVirtualContainer()
      throws IOException {
    Properties config = createProperties(
        "includeExcludeFilter.rule1.itemType = CONTAINER_ITEM",
        "includeExcludeFilter.rule1.filterType = REGEX",
        "includeExcludeFilter.rule1.filterPattern = archive",
        "includeExcludeFilter.rule1.action = EXCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    assertFalse(filter.isAllowed("/path/to/archive", ItemType.VIRTUAL_CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/to/archive", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/to/archive/more/content", ItemType.VIRTUAL_CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/to/archive/more/content", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/archiveFileStore", ItemType.VIRTUAL_CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/archiveFileStore", ItemType.CONTAINER_ITEM));
  }

  @Test
  public void fromConfiguration_filePrefixIncludeRule_succeeds_unix() throws IOException {
    assumeThat(IS_WINDOWS, is(false));
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = FILE_PREFIX",
        "includeExcludeFilter.rule1.filterPattern = /folder1/folder2",
        "includeExcludeFilter.rule1.action = INCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.isAllowed("/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder2", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder2", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder2/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder2/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder2/file.pdf", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder2/file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder2/folder3/file.pdf", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder2/folder3/file.pdf", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("/folder3", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/folder3", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder3", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder3", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_filePrefixIncludeRule_succeeds_windows() throws IOException {
    assumeThat(IS_WINDOWS, is(true));
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = FILE_PREFIX",
        "includeExcludeFilter.rule1.filterPattern = \\\\\\\\host\\\\share\\\\folder1",
        "includeExcludeFilter.rule1.action = INCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.isAllowed("\\\\host\\share", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\folder2", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\folder2", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\folder2\\", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\folder2\\", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed(
            "\\\\host\\share\\folder1\\folder2\\file.pdf", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed(
            "\\\\host\\share\\folder1\\folder2\\file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed(
            "\\\\host\\share\\folder1\\folder2\\folder3\\file.pdf", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed(
            "\\\\host\\share\\folder1\\folder2\\folder3\\file.pdf", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("\\\\host\\share\\folder3", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("\\\\host\\share\\folder3", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("\\\\host\\othershare\\folder3", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("\\\\host\\othershare\\folder3", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_filePrefixExcludeRule_succeeds_unix() throws IOException {
    assumeThat(IS_WINDOWS, is(false));
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = FILE_PREFIX",
        "includeExcludeFilter.rule1.filterPattern = /folder1/folder2",
        "includeExcludeFilter.rule1.action = EXCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    assertTrue(filter.isAllowed("/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder3", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder3", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder3", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1/folder3", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("/folder1/folder2", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder2", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder2/", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder2/", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder2/file.pdf", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder2/file.pdf", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder2/folder3/file.pdf", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/folder1/folder2/folder3/file.pdf", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_filePrefixExcludeRule_succeeds_windows() throws IOException {
    assumeThat(IS_WINDOWS, is(true));
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = FILE_PREFIX",
        "includeExcludeFilter.rule1.filterPattern = \\\\\\\\host\\\\share\\\\folder1\\\\folder2",
        "includeExcludeFilter.rule1.action = EXCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    assertTrue(filter.isAllowed("\\\\host\\share", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder\\", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder3", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\folder3", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\folder3", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("\\\\host\\share\\folder1\\folder2", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("\\\\host\\share\\folder1\\folder2", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("\\\\host\\share\\folder1\\folder2\\", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("\\\\host\\share\\folder1\\folder2\\", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed(
            "\\\\host\\share\\folder1\\folder2\\file.pdf", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed(
            "\\\\host\\share\\folder1\\folder2\\file.pdf", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed(
            "\\\\host\\share\\folder1\\folder2\\folder3\\file.pdf", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed(
            "\\\\host\\share\\folder1\\folder2\\folder3\\file.pdf", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_urlPrefixIncludeRule_succeeds() throws IOException {
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = URL_PREFIX",
        "includeExcludeFilter.rule1.filterPattern = http://example.com/folder1",
        "includeExcludeFilter.rule1.action = INCLUDE",

        "includeExcludeFilter.rule2.filterType = URL_PREFIX",
        "includeExcludeFilter.rule2.filterPattern = http://example.com/folder2",
        "includeExcludeFilter.rule2.action = INCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.isAllowed("http://example.com", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("http://example.com/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder1", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder1/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder1/file.txt", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder1/folder3", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder2", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder2/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder2/file.txt", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder2/folder3", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("http://example.com/folder3", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder3", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder12", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder12", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_urlPrefixExcludeRule_succeeds() throws IOException {
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = URL_PREFIX",
        "includeExcludeFilter.rule1.filterPattern = http://example.com/folder1",
        "includeExcludeFilter.rule1.action = EXCLUDE",

        "includeExcludeFilter.rule2.filterType = URL_PREFIX",
        "includeExcludeFilter.rule2.filterPattern = http://example.com/folder2",
        "includeExcludeFilter.rule2.action = EXCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.isAllowed("http://example.com", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("http://example.com/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com/", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder3", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder3", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder12", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("http://example.com/folder12", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("http://example.com/folder1", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder1/", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder1/file.txt", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder1/folder3", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder2", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder2/", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder2/file.txt", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("http://example.com/folder2/folder3", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_excludeFolders_succeeds() throws IOException {
    Properties config = createProperties(
        "includeExcludeFilter.rule1-container.filterType = REGEX",
        "includeExcludeFilter.rule1-container.itemType = CONTAINER_ITEM",
        "includeExcludeFilter.rule1-container.filterPattern = /[^/]+-ARCHIVE$|/[^/]+-ARCHIVE/",
        "includeExcludeFilter.rule1-container.action = EXCLUDE",

        "includeExcludeFilter.rule1-content.filterType = REGEX",
        "includeExcludeFilter.rule1-content.itemType = CONTENT_ITEM",
        "includeExcludeFilter.rule1-content.filterPattern = /[^/]+-ARCHIVE/",
        "includeExcludeFilter.rule1-content.action = EXCLUDE",

        "includeExcludeFilter.rule2.filterType = REGEX",
        "includeExcludeFilter.rule2.itemType = CONTENT_ITEM",
        "includeExcludeFilter.rule2.filterPattern = \\\\.(txt|text|htm|html)$",
        "includeExcludeFilter.rule2.action = INCLUDE");
    setupConfig.initConfig(config);

    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.isAllowed("/path", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/current", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.txt", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.text", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.htm", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.html", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/last-month-ARCHIVE-notAnArchive/file.txt",
            ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("/path/last-month-ARCHIVE", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/last-month-ARCHIVE/", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/last-month-ARCHIVE/reports", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/last-month-ARCHIVE/file.txt", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/path/current/file.pdf", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_includeFilesFolders_succeeds_unix() throws IOException {
    assumeThat(IS_WINDOWS, is(false));
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = REGEX",
        "includeExcludeFilter.rule1.itemType = CONTENT_ITEM",
        "includeExcludeFilter.rule1.filterPattern = \\\\.(pdf|doc)$",
        "includeExcludeFilter.rule1.action = INCLUDE",

        "includeExcludeFilter.rule2.filterType = FILE_PREFIX",
        "includeExcludeFilter.rule2.filterPattern = /folder1",
        "includeExcludeFilter.rule2.action = INCLUDE",

        "includeExcludeFilter.rule3.filterType = FILE_PREFIX",
        "includeExcludeFilter.rule3.filterPattern = /folder2",
        "includeExcludeFilter.rule3.action = INCLUDE");
    setupConfig.initConfig(config);

    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.isAllowed("/folder1", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder2", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/folder1/file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder2/file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder2/file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/path/to/file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder1/path/to/file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder2/path/to/file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/folder2/path/tofile.pdf", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("/folder3", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/folder12", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/folder1/file.txt", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/folder2/path/to/file.txt", ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_includeFilesFolders_succeeds_windows() throws IOException {
    assumeThat(IS_WINDOWS, is(true));
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = REGEX",
        "includeExcludeFilter.rule1.itemType = CONTENT_ITEM",
        "includeExcludeFilter.rule1.filterPattern = \\\\.(pdf|doc)$",
        "includeExcludeFilter.rule1.action = INCLUDE",

        "includeExcludeFilter.rule2.filterType = FILE_PREFIX",
        "includeExcludeFilter.rule2.filterPattern = \\\\\\\\host\\\\share\\\\folder1",
        "includeExcludeFilter.rule2.action = INCLUDE",

        "includeExcludeFilter.rule3.filterType = FILE_PREFIX",
        "includeExcludeFilter.rule3.filterPattern = \\\\\\\\host\\\\share\\\\folder2",
        "includeExcludeFilter.rule3.action = INCLUDE");
    setupConfig.initConfig(config);

    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder2", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder1\\file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder2\\file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("\\\\host\\share\\folder2\\file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed(
            "\\\\host\\share\\folder1\\path\\to\\file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed(
            "\\\\host\\share\\folder1\\path\\to\\file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed(
            "\\\\host\\share\\folder2\\path\\to\\file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed(
            "\\\\host\\share\\folder2\\path\\tofile.pdf", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("\\\\host\\share\\folder3", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("\\\\host\\share\\folder12", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("\\\\host\\share\\folder1\\file.txt", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed(
            "\\\\host\\share\\folder2\\path\\to\\file.txt", ItemType.CONTENT_ITEM));
  }

  @Test
  public void toString_succeeds() throws Exception {
    Properties config = createProperties(
        "includeExcludeFilter.rule1.filterType = REGEX",
        "includeExcludeFilter.rule1.itemType = CONTENT_ITEM",
        "includeExcludeFilter.rule1.filterPattern = \\\\.(pdf|doc)$",
        "includeExcludeFilter.rule1.action = INCLUDE"
      );
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue(filter.toString().startsWith("Include"));
  }

  @Test
  public void mainHelper_succeeds() throws Exception {
    // No rules, one test value.
    IncludeExcludeFilter.mainHelper(new String[0],
        new ByteArrayInputStream("/path/to/doc.txt".getBytes()));
  }

  private Properties createProperties(String... propertyLines) throws IOException {
    String in = String.join(System.getProperty("line.separator"), propertyLines);
    Properties p = new Properties();
    p.load(new java.io.StringReader(in));
    return p;
  }

  private void assertSizeEquals(List list, int expectedSize) {
    assertEquals("Checking size for " + list, expectedSize, list.size());
  }
}
