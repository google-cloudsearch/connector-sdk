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
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
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
  public void fromConfiguration_notInitialized_throwsException() {
    thrown.expect(IllegalStateException.class);
    IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void fromConfiguration_initializedNoConfig_succeeds() {
    setupConfig.initConfig(new Properties());
    IncludeExcludeFilter.fromConfiguration();
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertEquals(0, filter.includeRules.size());
    assertEquals(0, filter.excludeRules.size());
  }

  @Test
  public void fromConfiguration_fileMissing_throwsException() {
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile", "/no/such/file");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void fromConfiguration_fileIsDirectory_throwsException() throws IOException {
    File dir = tempFolder.newFolder("testdir");
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile", dir.getAbsolutePath());
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void invalidPatternPrefix_throwsException() throws IOException {
    File patternsFile = tempFolder.newFile("patterns.txt");
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile",
        patternsFile.getAbsolutePath());
    setupConfig.initConfig(config);
    try (FileWriter writer = new FileWriter(patternsFile)) {
      writer.write("notAPattern");
    }
    thrown.expect(InvalidConfigurationException.class);
    IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void invalidRegex_throwsException() throws IOException {
    File patternsFile = tempFolder.newFile("patterns.txt");
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile",
        patternsFile.getAbsolutePath());
    setupConfig.initConfig(config);
    try (FileWriter writer = new FileWriter(patternsFile)) {
      writer.write("include:regex:*");
    }
    thrown.expect(InvalidConfigurationException.class);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void fileNotReadable_throwsException() throws IOException {
    File patternsFile = tempFolder.newFile("patterns.txt");
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile",
        patternsFile.getAbsolutePath());
    setupConfig.initConfig(config);
    try (FileWriter writer = new FileWriter(patternsFile)) {
      writer.write("include:regex:foo");
    }
    patternsFile.setReadable(false);
    thrown.expect(InvalidConfigurationException.class);
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
  }

  @Test
  public void validPattern_succeeds() throws IOException {
    File patternsFile = tempFolder.newFile("patterns.txt");
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile",
        patternsFile.getAbsolutePath());
    setupConfig.initConfig(config);
    try (FileWriter writer = new FileWriter(patternsFile)) {
      writer.write("include:regex:.*\\.txt" + System.lineSeparator());
      writer.write("include:regex:.*\\.html" + System.lineSeparator());
      writer.write("exclude:regex:.*\\.pdf" + System.lineSeparator());
      writer.write("exclude:regex:.*\\.doc" + System.lineSeparator());
    }
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertEquals(2, filter.includeRules.size());
    assertEquals(2, filter.excludeRules.size());
  }

  @Test
  public void include_succeeds() throws IOException {
    File patternsFile = tempFolder.newFile("patterns.txt");
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile",
        patternsFile.getAbsolutePath());
    setupConfig.initConfig(config);
    try (FileWriter writer = new FileWriter(patternsFile)) {
      writer.write("include:regex:.*\\.txt$" + System.lineSeparator());
      writer.write("include:regex:.*\\.html$" + System.lineSeparator());
    }
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue("/path/to/file.txt", filter.isAllowed("/path/to/file.txt"));
    assertTrue("/path/to/file.html", filter.isAllowed("/path/to/file.html"));
    // uses case-insensitive match
    assertTrue("/path/to/file.HTML", filter.isAllowed("/path/to/file.HTML"));
    assertFalse("/path/to/file.other", filter.isAllowed("/path/to/file.other"));
    assertFalse("/path/to/file.txt.bak", filter.isAllowed("/path/to/file.txt.bak"));
  }

  @Test
  public void exclude_succeeds() throws IOException {
    File patternsFile = tempFolder.newFile("patterns.txt");
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile",
        patternsFile.getAbsolutePath());
    setupConfig.initConfig(config);
    try (FileWriter writer = new FileWriter(patternsFile)) {
      writer.write("exclude:regex:.*\\.txt$" + System.lineSeparator());
      writer.write("exclude:regex:.*\\.html$" + System.lineSeparator());
    }
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertFalse("/path/to/file.txt", filter.isAllowed("/path/to/file.txt"));
    assertFalse("/path/to/file.html", filter.isAllowed("/path/to/file.html"));
    // uses case-insensitive match
    assertFalse("/path/to/file.HTML", filter.isAllowed("/path/to/file.HTML"));
    assertTrue("/path/to/file.other", filter.isAllowed("/path/to/file.other"));
    assertTrue("/path/to/file.txt.bak", filter.isAllowed("/path/to/file.txt.bak"));
  }

  @Test
  public void includeExclude_succeeds() throws IOException {
    File patternsFile = tempFolder.newFile("patterns.txt");
    Properties config = new Properties();
    config.put("connector.includeExcludeFilter.includeExcludePatternFile",
        patternsFile.getAbsolutePath());
    setupConfig.initConfig(config);
    try (FileWriter writer = new FileWriter(patternsFile)) {
      writer.write("include:regex:.*\\.txt$" + System.lineSeparator());
      writer.write("exclude:regex:.*DEVELOPMENT.*" + System.lineSeparator());
    }
    IncludeExcludeFilter filter = IncludeExcludeFilter.fromConfiguration();
    assertTrue("/path/to/file.txt", filter.isAllowed("/path/to/file.txt"));
    assertFalse("/path/to/file-DEVELOPMENT.txt",
        filter.isAllowed("/path/to/file-DEVELOPMENT.txt"));
  }
}
