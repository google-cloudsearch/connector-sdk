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
package com.google.enterprise.cloudsearch.csvconnector;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.io.CharStreams;
import com.google.enterprise.cloudsearch.sdk.CloseableIterable;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.UrlBuilder;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.apache.commons.csv.CSVRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link CSVFileManager}. */
@RunWith(MockitoJUnitRunner.class)
public class CSVFileManagerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static String testCSVSingle = "term, definition, author\n"
      + "moma search, Google internal search , ID1\n\n\n\n";

  private static String testCSVSingleWithoutNewLinesAtTheEndOfFile = "term, definition, author\n"
      + "moma search, Google internal search , ID1";

  private static String testCSVSingleWithQuotesDiffFormat = "term, definition, author\n"
      + "moma search, \"Super, \"\"luxurious\"\" truck\",ID2\n";

  private static String testCSVSingleWithOnlyHeaderNoRecords = "term, definition, author\n";

  private static String testCSVEmptyField = "term, definition, author\n"
      + "moma search , , ID1\n";

  private static String testCSVSingleWithStructuredDataEmptyDateTime =
      "term, definition, author, updated\r\n"
          + "momaSearch, Google internal search, ID1, \r\n";

  private static String testCSVSingleWithStructuredDataMultiDateTime =
      "term, definition, author, updated\r\n"
          + "momaSearch, Google internal search, ID1, "
          + "\"2017-06-18 14:01:23.400-07:00, 2017-06-19 08:01:23.400-07:00\"\r\n";

  private static final String CONTENT_HIGH = "contentTemplate.csv.quality.high";
  private static final String CONTENT_LOW = "contentTemplate.csv.quality.low";
  private static final String CONTENT_TITLE = "contentTemplate.csv.title";

  private static String testCSVSingleWithMultiValueFields = "term, definition, author, updated\r\n"
      + "momaSearch, Google internal search, \"ID1; ID2,A;\", "
      + "\"2017-06-18 14:01:23.400-07:00, 2017-06-19 08:01:23.400-07:00\"\r\n";

  private void createFile(File file, Charset charset, String... content) throws IOException {
    try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file), charset)) {
      for (String line : content) {
        out.write(line);
        // Let test embed custom line separators if desired, else write a separator
        if (content.length > 1) {
          out.write(System.lineSeparator());
        }
      }
    }
  }

  @Test
  public void testCsvFileManagerEmptyFilePath() {
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, "");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("csv.filePath"));
    CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerWithIncorrectFilePath() {
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, "invalid/path/to/file.java");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("File does not exist: csv.filePath=invalid/path/to/file.java");
    CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerCSVFileNotExist() {
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, "testNotExist.csv");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("File does not exist: csv.filePath=testNotExist.csv");
    CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerEmptyColumnStringSkipHeader() throws IOException {
    File tmpfile = temporaryFolder.newFile("test.csv");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.SKIP_HEADER, "true");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term");
    setupConfig.initConfig(config);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(containsString(
        "csv.csvColumns property must be specified if csv.skipHeaderRecord is true"));
    CSVFileManager.fromConfiguration();
  }

  @Test
  public void testGetCSVFileIterator() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCreateItemWithKey.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    csvFile.iterator();
    thrown.expect(IllegalStateException.class);
    csvFile.iterator();
  }

  @Test
  public void testCSVSingleWithoutNewLinesAtTheEndOfFile() throws IOException {
    File tmpfile = temporaryFolder.newFile("SkipHeaderFalseCsvColumnsEmpty.csv");
    createFile(tmpfile, UTF_8, testCSVSingleWithoutNewLinesAtTheEndOfFile);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.CSVCOLUMNS, "");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("moma search", item.getName());
    assertEquals(null, item.getAcl());
    assertEquals("moma search", item.getMetadata().getSourceRepositoryUrl());
  }

  @Test
  public void testCsvFileManagerSkipHeaderTrueCsvColumnsEmpty() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCsvFileManagerSkipHeaderTrueCsvColumnsEmpty.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    config.put(CSVFileManager.CSVCOLUMNS, "");

    setupConfig.initConfig(config);

    thrown.expect(IllegalStateException.class);
    thrown.expectMessage(containsString(
        "csv.csvColumns property must be specified if csv.skipHeaderRecord is true"));
    CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerSkipHeaderFalseCsvColumnsEmpty() throws IOException {
    File tmpfile = temporaryFolder.newFile("SkipHeaderFalseCsvColumnsEmpty.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.CSVCOLUMNS, "");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("moma search", item.getName());
    assertEquals(null, item.getAcl());
    assertEquals("moma search", item.getMetadata().getSourceRepositoryUrl());
  }

  @Test
  public void testCsvFileManagerSkipHeaderFalseCsvFileWithOnlyHeaderNoRecord() throws IOException {
    File tmpfile = temporaryFolder.newFile("SkipHeaderFalseCsvFileWithOnlyHeaderNoRecord.csv");
    createFile(tmpfile, UTF_8, testCSVSingleWithOnlyHeaderNoRecords);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term");
    config.put(CSVFileManager.SKIP_HEADER, "false");
    config.put(CSVFileManager.CSVCOLUMNS, "term,definition");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("term", item.getName());
    assertEquals(null, item.getAcl());
    assertEquals("term", item.getMetadata().getSourceRepositoryUrl());
  }

  @Test
  public void testCsvFileManagerWithQuotesDiffFormatData() throws IOException {
    File tmpfile = temporaryFolder.newFile("invertedCommasTest2.csv");
    createFile(tmpfile, UTF_8, testCSVSingleWithQuotesDiffFormat);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    config.put(CSVFileManager.CSVCOLUMNS, "term,definition");

    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("moma search", item.getName());
    assertEquals(null, item.getAcl());
    assertEquals("moma search", item.getMetadata().getSourceRepositoryUrl());
    assertEquals("Super, \"luxurious\" truck", csvRecord.get("definition"));
  }

  @Test
  public void testCsvFileManagerCreateItemWithKey() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCreateItemWithKey.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("moma search", item.getName());
    assertEquals(null, item.getAcl());
    assertEquals("moma search", item.getMetadata().getSourceRepositoryUrl());
  }

  @Test
  public void testCsvFileManagerCreateItemWithMultiKey() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCreateItemWithMultiKey.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term, definition");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("moma search||Google internal search", item.getName());
    assertEquals(null, item.getAcl());
    assertEquals("moma search", item.getMetadata().getSourceRepositoryUrl());
  }

  @Test
  public void testCsvFileManagerCreateItemWithOutKey() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCreateItemWithoutKey.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals(null, item.getAcl());
  }

  @Test
  public void testCsvFileManagerCreateContent() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCreateContent.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    ByteArrayContent content = csvFileManager.createContent(csvRecord);
    String html = CharStreams
          .toString(new InputStreamReader(content.getInputStream(), UTF_8));
    assertThat(html, containsString("moma search"));
    assertThat(html, containsString("ID1"));

  }

  @Test
  public void testCsvFileManagerCsvFormatWithCsvColumns() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCreateCsvFormat.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "title");
    config.put(CONTENT_TITLE, "title");
    config.put(CONTENT_HIGH, "title,description");
    config.put(CSVFileManager.CSVCOLUMNS, "title, description, Id");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    assertEquals(ImmutableSet.of("description", "Id", "title"),
        csvRecord.toMap().keySet());
    assertEquals("Google internal search", csvRecord.get("description"));
  }

  @Test
  public void testCsvFileManagerCsvFormatWithEmptyCsvColumns() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCreateCsvFormat.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.CSVCOLUMNS, "");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    assertEquals(ImmutableSet.of("term", "definition", "author"),
        csvRecord.toMap().keySet());
    assertEquals("Google internal search", csvRecord.get("definition"));
  }

  @Test
  public void testCsvFileManagerVerifyColumns() throws IOException {
    File tmpfile = temporaryFolder.newFile("testVerifyColumns.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term,author");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    csvFileManager.getCSVFile();
  }

  @Test
  public void testCsvFileManagerVerifyColumnsOverride() throws IOException {
    File tmpfile = temporaryFolder.newFile("testVerifyColumnsOverride.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "title,id");
    config.put(CONTENT_TITLE, "title");
    config.put(CONTENT_HIGH, "title,description");
    // Header in file is "term, definition, author"
    config.put(CSVFileManager.CSVCOLUMNS, "title, description, id");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CSVRecord csvRecord = getOnlyElement(csvFileManager.getCSVFile());
    // The record should use the values from csvColumns
    assertEquals(ImmutableSet.of("title", "description", "id"),
        csvRecord.toMap().keySet());
  }

  @Test
  public void testCsvFileManagerVerifyColumnsWithError() throws IOException {
    File tmpfile = temporaryFolder.newFile("testVerifyColumnsWithError.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term,author");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "title");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("[title]"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerVerifyColumnsCaseSensitiveUniqueKey() throws IOException {
    File tmpfile = temporaryFolder.newFile("testVerifyColumnsWithError.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term,author");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "TERM");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(CSVFileManager.UNIQUE_KEY_COLUMNS));
    thrown.expectMessage(containsString("[TERM]"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerVerifyColumnsCaseSensitiveUrl() throws IOException {
    File tmpfile = temporaryFolder.newFile("testVerifyColumnsWithError.csv");
    createFile(tmpfile, UTF_8, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "TERM,author");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(UrlBuilder.CONFIG_COLUMNS));
    thrown.expectMessage(containsString("[TERM]"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerEmptyField() throws IOException {
    File tmpfile = temporaryFolder.newFile("testEmptyField.csv");
    createFile(tmpfile, UTF_8, testCSVEmptyField);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term,author");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    ByteArrayContent content = csvFileManager.createContent(csvRecord);
    String html = CharStreams
        .toString(new InputStreamReader(content.getInputStream(), UTF_8));
    //definition part has empty value
    assertThat(html, containsString("<p>definition:</p>\n" + "  <h1></h1>"));
  }

  @Test
  public void testCsvFileManagerMultiDateTime() throws IOException {
    File tmpfile = temporaryFolder.newFile("testMultiDateTime.csv");
    createFile(tmpfile, UTF_8, testCSVSingleWithStructuredDataMultiDateTime);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term,author");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CSVFileManager.MULTIVALUE_COLUMNS, "updated");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Multimap<String, Object> multimap = csvFileManager.generateMultiMap(csvRecord);
    assertEquals(multimap.get("updated").toString(), 2, multimap.get("updated").size());
  }

  @Test
  public void testCsvFileManagerEmptyDateTimeFields() throws IOException {
    File tmpfile = temporaryFolder.newFile("testEmptyDateTimeFields.csv");
    createFile(tmpfile, UTF_8, testCSVSingleWithStructuredDataEmptyDateTime);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term,author");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Multimap<String, Object> multimap = csvFileManager.generateMultiMap(csvRecord);
    assertEquals(Collections.emptyList(), multimap.get("updated"));
  }

  @Test
  public void testCsvFileManagerMultiValueFields() throws IOException {
    File tmpfile = temporaryFolder.newFile("testMultiValueFields.csv");
    createFile(tmpfile, UTF_8, testCSVSingleWithMultiValueFields);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term,author");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CSVFileManager.MULTIVALUE_COLUMNS, "updated, author");
    config.put("csv.multiValue.author", ";");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Multimap<String, Object> multimap = csvFileManager.generateMultiMap(csvRecord);
    assertThat(multimap.get("author"), hasItem("ID1"));
    assertThat(multimap.get("author"), hasItem("ID2,A"));
  }

  @Test
  public void multiValueColumns_withCsvColumns_invalidColumnName_throwsException()
      throws IOException {
    File tmpfile = temporaryFolder.newFile("testMultiValueFields.csv");
    createFile(tmpfile, UTF_8,
        "term, synonyms",
        "small, \"diminutive,little,slight\"");

    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.CSVCOLUMNS, "term, synonyms");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.MULTIVALUE_COLUMNS, "notAColumn");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(CSVFileManager.MULTIVALUE_COLUMNS));
    thrown.expectMessage(containsString("[notAColumn]"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void multiValueColumns_noCsvColumns_invalidColumnName_throwsException()
      throws IOException {
    File tmpfile = temporaryFolder.newFile("testMultiValueFields.csv");
    createFile(tmpfile, UTF_8,
        "term, synonyms",
        "small, \"diminutive,little,slight\"");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.MULTIVALUE_COLUMNS, "notAColumn");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(CSVFileManager.MULTIVALUE_COLUMNS));
    thrown.expectMessage(containsString("[notAColumn]"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void multiValueColumns_noMultiValueColumns_configuredDelimiterColumns_throwsException()
      throws IOException {
    File tmpfile = temporaryFolder.newFile("testMultiValueFields.csv");
    createFile(tmpfile, UTF_8,
        "term, synonyms",
        "small, \"diminutive,little,slight\"");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.CSVCOLUMNS, "term, synonyms");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    // Don't configure multi-value columns; configured delimiters should fail
    //config.put(CSVFileManager.MULTIVALUE_COLUMNS, "");
    config.put("csv.multiValue.synonyms", "*");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(
        "Multi-value separators are configured but no multi-value columns are configured");
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void multiValueColumns_withCsvColumns_invalidDelimiterColumnName_throwsException()
      throws IOException {
    File tmpfile = temporaryFolder.newFile("testMultiValueFields.csv");
    createFile(tmpfile, UTF_8,
        "term, synonyms",
        "small, \"diminutive,little,slight\"");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.CSVCOLUMNS, "term, synonyms");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.MULTIVALUE_COLUMNS, "synonyms");
    config.put("csv.multiValue.synonyms", "*");
    config.put("csv.multiValue.notAColumn", "*");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("csv.multiValue.*"));
    thrown.expectMessage(containsString("[notAColumn]"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void multiValueColumns_noCsvColumns_invalidDelimiterColumnName_throwsException()
      throws IOException {
    File tmpfile = temporaryFolder.newFile("testMultiValueFields.csv");
    createFile(tmpfile, UTF_8,
        "term, synonyms",
        "small, \"diminutive,little,slight\"");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.MULTIVALUE_COLUMNS, "synonyms");
    config.put("csv.multiValue.notAColumn", "*");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("csv.multiValue.*"));
    thrown.expectMessage(containsString("[notAColumn]"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  private Properties getCSVFormatConfig(String input, String csvFormat) throws IOException {
    File tmpfile = temporaryFolder.newFile("CSVFormat.csv");
    createFile(tmpfile, UTF_8, input);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    config.put(CSVFileManager.CSVCOLUMNS, "term, definition");
    config.put(CSVFileManager.CSV_FORMAT, csvFormat);
    return config;
  }

  @Test
  public void testCSVDefaultFormat() throws IOException {
    Properties config = getCSVFormatConfig(testCSVSingle, "default");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("moma search", item.getName());
    assertEquals(" Google internal search ", csvRecord.get("definition"));
  }

  @Test
  public void testCSVInvalidFormat() throws IOException {
    Properties config = getCSVFormatConfig(testCSVSingle, "unknownformat");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("Invalid CSVFormat unknownformat"));
    CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCSVExcelFormat() throws IOException {
    Properties config = getCSVFormatConfig(testCSVSingle, "excel");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();

    Iterator<CSVRecord> iter = csvFile.iterator();
    Item item = csvFileManager.createItem(iter.next());
    assertEquals("moma search", item.getName());
    int count = 0;
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    assertEquals(3, count);
  }

  @Test
  public void testCSVExcelFormat_withMethods() throws IOException {
    String input = "term; definition; author\n"
        + "moma, search; Google internal search ; ID1\n\n\n\n";
    Properties config = getCSVFormatConfig(input, "excel");
    config.put(String.format(CSVFileManager.CSV_FORMAT_METHOD_VALUE, "withDelimiter"), ";");
    config.put(String.format(CSVFileManager.CSV_FORMAT_METHOD_VALUE, "withIgnoreEmptyLines"),
        "true");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("moma, search", item.getName());
  }

  @Test
  public void testCSVExcelFormat_withMethodsInvalidCharValue() throws IOException {
    Properties config = getCSVFormatConfig(testCSVSingle, "excel");
    config.put(String.format(CSVFileManager.CSV_FORMAT_METHOD_VALUE, "withDelimiter"), ".:;");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage("must be a single character");
    CSVFileManager.fromConfiguration();
  }

  // Smoke test to make sure the "withRecordSeparator(char)" overload method is not invoked.
  @Test
  public void testCSVExcelFormat_withRecordSeparator() throws IOException {
    Properties config = getCSVFormatConfig(testCSVSingle, "default");
    config.put(String.format(CSVFileManager.CSV_FORMAT_METHOD_VALUE, "withRecordSeparator"),
        "\t\r\n");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();
    CSVRecord csvRecord = getOnlyElement(csvFile);

    Item item = csvFileManager.createItem(csvRecord);
    assertEquals("moma search", item.getName());
  }

  // Write, read using UTF-8
  @Test
  public void testCsvFileManagerEncodingUtf8() throws IOException {
    String utf8euro = "\u20ac";
    File tmpfile = temporaryFolder.newFile("testEncoding.csv");
    createFile(tmpfile, UTF_8,
        "term, definition",
        "euro, symbol=" + utf8euro);

    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.FILE_ENCODING, UTF_8.name());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CSVRecord csvRecord = getOnlyElement(csvFileManager.getCSVFile());

    assertEquals("symbol=" + utf8euro,  csvRecord.get("definition"));
  }

  // Write using Cp1252, but read using UTF-8; character should not survive
  @Test
  public void testCsvFileManagerEncodingMismatch() throws IOException {
    String utf8euro = "\u20ac";
    File tmpfile = temporaryFolder.newFile("testEncoding.csv");
    createFile(tmpfile, Charset.forName("Cp1252"),
        "term, definition",
        "euro, symbol=" + utf8euro);

    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.FILE_ENCODING, UTF_8.name()); // Read using a different encoding
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CSVRecord csvRecord = getOnlyElement(csvFileManager.getCSVFile());

    assertNotEquals("symbol=" + utf8euro, csvRecord.get("definition"));
    assertThat(csvRecord.get("definition"), endsWith(UTF_8.newDecoder().replacement()));
  }

  // Write, read a character that exists in Cp1252, using Cp1252; it should come back as
  // the expected character
  @Test
  public void testCsvFileManagerEncodingCp1252() throws IOException {
    String utf8euro = "\u20ac";
    File tmpfile = temporaryFolder.newFile("testEncoding.csv");
    createFile(tmpfile, Charset.forName("Cp1252"),
        "term, definition",
        "euro, symbol=" + utf8euro);

    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.FILE_ENCODING, "Cp1252");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CSVRecord csvRecord = getOnlyElement(csvFileManager.getCSVFile());

    assertEquals("symbol=" + utf8euro,  csvRecord.get("definition"));
  }

  // Write, read a character that does not exist in Cp1252; it should not survive
  @Test
  public void testCsvFileManagerEncodingNonCp1252Character() throws IOException {
    String utf8devanagarishorta = "\u0904";
    File tmpfile = temporaryFolder.newFile("testEncoding.csv");
    createFile(tmpfile, Charset.forName("Cp1252"),
        "term, definition",
        "devanagarishorta, symbol=" + utf8devanagarishorta);

    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.FILE_ENCODING, "Cp1252");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CSVRecord csvRecord = getOnlyElement(csvFileManager.getCSVFile());

    assertNotEquals("symbol=" + utf8devanagarishorta, csvRecord.get("definition"));
  }

  // Write, read a character that does not exist in Cp1252 using UTF-8; another check that
  // setting the file encoding property allows UTF-8 characters on Windows
  @Test
  public void testCsvFileManagerEncodingNonCp1252CharacterWithUtf8() throws IOException {
    String utf8devanagarishorta = "\u0904";
    File tmpfile = temporaryFolder.newFile("testEncoding.csv");
    createFile(tmpfile, UTF_8,
        "term, definition",
        "devanagarishorta, symbol=" + utf8devanagarishorta);

    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.FILE_ENCODING, UTF_8.name());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CSVRecord csvRecord = getOnlyElement(csvFileManager.getCSVFile());

    assertEquals("symbol=" + utf8devanagarishorta, csvRecord.get("definition"));
  }

  @Test
  public void testCsvFileManagerEncodingInvalid() throws IOException {
    File tmpfile = temporaryFolder.newFile("testEncoding.csv");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(CSVFileManager.FILE_ENCODING, "NoSuchEncoding");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(CSVFileManager.FILE_ENCODING));
    thrown.expectMessage(containsString("NoSuchEncoding"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerEncodingNotSet() throws IOException {
    File tmpfile = temporaryFolder.newFile("testEncoding.csv");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.CSVCOLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);

    // Should not throw an exception
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }
}
