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
import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;

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
  public void testCsvFileManagerCSVFileNotExist() {
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, "testNotExist.csv");
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(
        "csv file testNotExist.csv does not exists"));
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
    assertTrue(html.contains("moma search"));
    assertTrue(html.contains("ID1"));

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
    config.put(UrlBuilder.CONFIG_COLUMNS, "title,Id");
    config.put(CONTENT_TITLE, "title");
    config.put(CONTENT_HIGH, "title,description");
    config.put(CSVFileManager.CSVCOLUMNS, "title, description, Id");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    csvFileManager.getCSVFile();
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

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("[title]"));
    csvFileManager.getCSVFile();
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

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(CSVFileManager.UNIQUE_KEY_COLUMNS));
    thrown.expectMessage(containsString("[TERM]"));
    csvFileManager.getCSVFile();
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

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString(UrlBuilder.CONFIG_COLUMNS));
    thrown.expectMessage(containsString("[TERM]"));
    csvFileManager.getCSVFile();
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
    assertTrue(html.contains("<p>definition:</p>\n" + "  <h1></h1>"));
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
    assertTrue(multimap.get("updated").size() == 2);
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
    assertTrue(multimap.get("author").contains("ID1"));
    assertTrue(multimap.get("author").contains("ID2,A"));
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

    assertFalse(("symbol=" + utf8euro).equals(csvRecord.get("definition")));
    assertTrue(csvRecord.get("definition").endsWith(UTF_8.newDecoder().replacement()));
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

    assertFalse(("symbol=" + utf8devanagarishorta).equals(csvRecord.get("definition")));
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
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCsvFileManagerEncodingNotSet() throws IOException {
    File tmpfile = temporaryFolder.newFile("testEncoding.csv");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    setupConfig.initConfig(config);

    // Should not throw an exception
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }
}
