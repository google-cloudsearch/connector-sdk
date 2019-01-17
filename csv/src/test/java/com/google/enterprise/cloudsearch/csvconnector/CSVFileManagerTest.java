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
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
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

  private void createFile(File file, String content) throws IOException {
    PrintWriter pw = new PrintWriter(new FileWriter(file));
    pw.write(content);
    pw.close();
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
  public void testCsvFileManagerCreateItemWithKey() throws IOException {
    File tmpfile = temporaryFolder.newFile("testCreateItemWithKey.csv");
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVSingle);
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
    createFile(tmpfile, testCSVEmptyField);
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
    createFile(tmpfile, testCSVSingleWithStructuredDataMultiDateTime);
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
    createFile(tmpfile, testCSVSingleWithStructuredDataEmptyDateTime);
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
    createFile(tmpfile, testCSVSingleWithMultiValueFields);
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

  @Test
  public void testCSVDefaultFormat() throws IOException {
    File tmpfile = temporaryFolder.newFile("CSVDefaultFormat.csv");
    createFile(tmpfile, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    config.put(CSVFileManager.CSVCOLUMNS, "term, definition");
    config.put(CSVFileManager.CSV_FORMAT, "default");
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
    File tmpfile = temporaryFolder.newFile("CSVInvalidFormat.csv");
    createFile(tmpfile, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    config.put(CSVFileManager.CSVCOLUMNS, "term, definition");
    config.put(CSVFileManager.CSV_FORMAT, "unknownformat");
    setupConfig.initConfig(config);

    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(containsString("Invalid CSVFormat unknownformat"));
    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
  }

  @Test
  public void testCSVExcelFormat() throws IOException {
    File tmpfile = temporaryFolder.newFile("CSVExcelFormat.csv");
    createFile(tmpfile, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    config.put(CSVFileManager.CSVCOLUMNS, "term,definition");
    config.put(CSVFileManager.CSV_FORMAT, "excel");
    setupConfig.initConfig(config);

    CSVFileManager csvFileManager = CSVFileManager.fromConfiguration();
    CloseableIterable<CSVRecord> csvFile = csvFileManager.getCSVFile();

    Item item = csvFileManager.createItem(csvFile.iterator().next());
    assertEquals("moma search", item.getName());
    int count = 0;
    Iterator<CSVRecord> iter = csvFile.iterator();
    while (iter.hasNext()) {
      iter.next();
      count++;
    }
    assertEquals(3, count);
  }
}



