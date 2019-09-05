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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import java.util.Properties;
import java.util.regex.PatternSyntaxException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests for IncludeExcludeFilter.
 */
@RunWith(JUnit4.class)
public class IncludeExcludeFilterTest {
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
    assertEquals(ItemType.CONTENT_ITEM, rule.getItemType());
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
  public void ruleBuilder_missingItemType_throwsException() {
    thrown.expect(IllegalStateException.class);
    thrown.expectMessage("Rule item type");
    IncludeExcludeFilter.Rule rule = new IncludeExcludeFilter.Rule.Builder("testRule")
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
  public void fromConfiguration_notInitialized_throwsException() {
    thrown.expect(IllegalStateException.class);
    IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void fromConfiguration_initializedNoConfig_emptyRulesCreated() {
    setupConfig.initConfig(new Properties());
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertEquals(ItemType.values().length, filter.includeRules.size());
    assertEquals(ItemType.values().length, filter.excludeRules.size());
    for (ItemType itemType : ItemType.values()) {
      assertEquals(0, filter.includeRules.get(itemType).size());
      assertEquals(0, filter.excludeRules.get(itemType).size());
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
  public void fromConfiguration_includeRule_succeeds() {
    Properties config = new Properties();
    config.setProperty("includeExcludeFilter.includeText.itemType", "CONTENT_ITEM");
    config.setProperty("includeExcludeFilter.includeText.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.includeText.filterPattern", "\\.txt$");
    config.setProperty("includeExcludeFilter.includeText.action", "INCLUDE");
    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertEquals(1, filter.includeRules.get(ItemType.CONTENT_ITEM).size());
    assertEquals(0, filter.excludeRules.get(ItemType.CONTENT_ITEM).size());
    IncludeExcludeFilter.Rule rule = filter.includeRules.get(ItemType.CONTENT_ITEM).get(0);
    assertEquals("includeText", rule.getName());
    assertEquals(ItemType.CONTENT_ITEM, rule.getItemType());
    assertEquals(IncludeExcludeFilter.Action.INCLUDE, rule.getAction());

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
  public void fromConfiguration_multipleRules_succeeds() {
    Properties config = new Properties();

    // Include files with given extensions
    config.setProperty("includeExcludeFilter.includeTextFiles.itemType", "CONTENT_ITEM");
    config.setProperty("includeExcludeFilter.includeTextFiles.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.includeTextFiles.filterPattern",
        "\\.(txt|text|htm|html)$");
    config.setProperty("includeExcludeFilter.includeTextFiles.action", "INCLUDE");

    // Exclude files named XXX-ARCHIVE.YYY
    config.setProperty("includeExcludeFilter.excludeArchiveFiles.itemType", "CONTENT_ITEM");
    config.setProperty("includeExcludeFilter.excludeArchiveFiles.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.excludeArchiveFiles.filterPattern",
        "-ARCHIVE\\.[^/]+$");
    config.setProperty("includeExcludeFilter.excludeArchiveFiles.action", "EXCLUDE");

    // Exclude folders named XXX-ARCHIVE
    config.setProperty("includeExcludeFilter.excludeArchiveFolders.itemType", "CONTAINER_ITEM");
    config.setProperty("includeExcludeFilter.excludeArchiveFolders.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.excludeArchiveFolders.filterPattern",
        "/[^/]+-ARCHIVE$|/[^/]+-ARCHIVE/"); // assumes path separator is "/"
    config.setProperty("includeExcludeFilter.excludeArchiveFolders.action", "EXCLUDE");

    // Exclude files with a folder named XXX-ARCHIVE in their path. In a listing
    // connector, the folder would have been excluded by a previous rule, but if the rules
    // were changed, the file might be polled for re-indexing sooner than the folder; this
    // rule would allow the connector to delete it.
    config.setProperty(
        "includeExcludeFilter.excludeFilesInArchiveFolders.itemType", "CONTENT_ITEM");
    config.setProperty("includeExcludeFilter.excludeFilesInArchiveFolders.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.excludeFilesInArchiveFolders.filterPattern",
        "/[^/]+-ARCHIVE/");
    config.setProperty("includeExcludeFilter.excludeFilesInArchiveFolders.action", "EXCLUDE");

    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertEquals(1, filter.includeRules.get(ItemType.CONTENT_ITEM).size());
    assertEquals(2, filter.excludeRules.get(ItemType.CONTENT_ITEM).size());
    assertEquals(1, filter.excludeRules.get(ItemType.CONTAINER_ITEM).size());

    assertTrue(filter.isAllowed("/path", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/current", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.txt", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.text", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.htm", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.html", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("/path/current/file-ARCHIVE.html", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/path/last-month-ARCHIVE", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/last-month-ARCHIVE/", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/last-month-ARCHIVE/reports", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/last-month-ARCHIVE/file.txt", ItemType.CONTENT_ITEM));

    assertTrue(filter.isAllowed("/path/last-month-ARCHIVE-notAnArchive/file.txt",
            ItemType.CONTENT_ITEM));
  }

  @Test
  public void fromConfiguration_excludeFolder_succeeds() {
    Properties config = new Properties();

    // Exclude folders with paths containing folders named /ARCHIVE/.
    config.setProperty("includeExcludeFilter.excludeArchiveFolders.itemType", "CONTAINER_ITEM");
    config.setProperty("includeExcludeFilter.excludeArchiveFolders.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.excludeArchiveFolders.filterPattern",
        "/ARCHIVE$|/ARCHIVE/"); // assumes path separator is "/"
    config.setProperty("includeExcludeFilter.excludeArchiveFolders.action", "EXCLUDE");

    // Exclude docs with paths containing folders named /ARCHIVE/.
    config.setProperty("includeExcludeFilter.excludeDocsInArchiveFolders.itemType", "CONTENT_ITEM");
    config.setProperty("includeExcludeFilter.excludeDocsInArchiveFolders.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.excludeDocsInArchiveFolders.filterPattern",
        "/ARCHIVE/"); // assumes path separator is "/"
    config.setProperty("includeExcludeFilter.excludeDocsInArchiveFolders.action", "EXCLUDE");

    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    assertTrue(filter.isAllowed("/path", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/current", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.txt", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.pdf", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/path/current/file.html", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("/ARCHIVE", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/ARCHIVE", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/ARCHIVE/", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/ARCHIVE/reports", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/path/ARCHIVE/reports/last-month.doc", ItemType.CONTENT_ITEM));
    assertFalse(filter.isAllowed("/path/is/longer/with/ARCHIVE/folder", ItemType.CONTAINER_ITEM));
  }

  @Test
  public void fromConfiguration_includeFolders_succeeds() {
    Properties config = new Properties();

    // Include only some folders below the root. The parent paths must also be included in
    // the pattern for a hierarchical repository.
    config.setProperty("includeExcludeFilter.includePublicFolder.itemType", "CONTAINER_ITEM");
    config.setProperty("includeExcludeFilter.includePublicFolder.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.includePublicFolder.filterPattern",
        "^/$|^/root[/]?$|^/root/folder[/]?$|^/root/folder/public[/]?$|^/root/folder/public/");
    config.setProperty("includeExcludeFilter.includePublicFolder.action", "INCLUDE");

    config.setProperty("includeExcludeFilter.includePressFolder.itemType", "CONTAINER_ITEM");
    config.setProperty("includeExcludeFilter.includePressFolder.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.includePressFolder.filterPattern",
        "^/$|^/root[/]?$|^/root/releases[/]?$|^/root/releases/press[/]?$|^/root/releases/press/");
    config.setProperty("includeExcludeFilter.includePressFolder.action", "INCLUDE");

    // Include .doc files in the public, press folders.
    config.setProperty("includeExcludeFilter.includeDocsInPublicFolder.itemType", "CONTENT_ITEM");
    config.setProperty("includeExcludeFilter.includeDocsInPublicFolder.filterType", "REGEX");
    config.setProperty("includeExcludeFilter.includeDocsInPublicFolder.filterPattern",
        "^/root/folder/public/.*\\.doc$|^/root/releases/press/.*\\.doc$");
    config.setProperty("includeExcludeFilter.includeDocsInPublicFolder.action", "INCLUDE");

    setupConfig.initConfig(config);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();

    assertTrue(filter.isAllowed("/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/folder", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/folder/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/folder/public", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/folder/public/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/folder/public/path/to/more/content",
            ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/releases", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/releases/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/releases/press", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/releases/press/", ItemType.CONTAINER_ITEM));
    assertTrue(filter.isAllowed("/root/releases/press/path/to/more/content",
            ItemType.CONTAINER_ITEM));

    assertFalse(filter.isAllowed("/other-folder", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/root/other-folder", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/other-folder/public", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/other-folder/public/path", ItemType.CONTAINER_ITEM));
    assertFalse(filter.isAllowed("/other-folder/public/path", ItemType.CONTAINER_ITEM));

    assertTrue(filter.isAllowed("/root/folder/public/file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/root/folder/public/path/to/file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/root/releases/press/file.doc", ItemType.CONTENT_ITEM));
    assertTrue(filter.isAllowed("/root/releases/press/path/to/file.doc", ItemType.CONTENT_ITEM));

    assertFalse(filter.isAllowed("/root/folder/public/path/to/file.pdf", ItemType.CONTENT_ITEM));
  }

  @Test
  public void mainHelper_succeeds() throws Exception {
    IncludeExcludeFilter.mainHelper(new String[0],
        new java.io.ByteArrayInputStream("/path/to/doc.txt".getBytes()));
  }
}
