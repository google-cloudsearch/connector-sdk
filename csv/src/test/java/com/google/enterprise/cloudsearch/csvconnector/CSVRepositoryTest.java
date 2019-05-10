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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.util.DateTime;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemMetadata;
import com.google.api.services.cloudsearch.v1.model.ItemStructuredData;
import com.google.api.services.cloudsearch.v1.model.ObjectDefinition;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.StructuredDataObject;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterable;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.DefaultAcl.DefaultAclMode;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.ContentFormat;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService.RequestMode;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import com.google.enterprise.cloudsearch.sdk.indexing.UrlBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.template.ApiOperation;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryContext;
import com.google.enterprise.cloudsearch.sdk.indexing.template.RepositoryDoc;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.TimeZone;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link CSVRepostitory}. */

// TODO(tvartak) : Restructure tests. Current set of tests are using functionality internal to
// template connector implementation.

@RunWith(MockitoJUnitRunner.class)
public class CSVRepositoryTest {

  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public TestName testName = new TestName();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock IndexingService mockIndexingService;
  @Mock private RepositoryContext mockRepositoryContext;

  private static final String CONTENT_HIGH = "contentTemplate.csv.quality.high";
  private static final String CONTENT_LOW = "contentTemplate.csv.quality.low";
  private static final String CONTENT_TITLE = "contentTemplate.csv.title";

  private static String testCSV =
      "term, definition, author\n"
          + "moma search, Google Internal Search, ID1\r\n\n\n"
          + "test search1, Google's test search engine, testId2\n"
          + "test search2, Google's test search engine, testId3\n"
          + "test search3, Google's test search engine, testId4\n"
          + "test search4, Google's test search engine, testId5\n";

  private static String testCSVSingleWithStructuredData =
      "term, definition, author, created, modified\r\n"
          + "momaSearch, Google internal search, ID1, 2017-06-18T14:01:23.400-07:00, "
          + "2017-06-19T08:01:23.400-07:00\r\n";

  private static String testCSVSingleWithStructuredDataDefaultDateTime =
      "term, definition, author, created, modified\r\n"
          + "momaSearch, Google internal search, ID1, 2017-06-18 14:01:23.400-07:00, "
          + "2017-06-19 08:01:23.400-07:00\r\n";

  private static String testCSVSingle =
      "term, definition, author\r\n" + "moma search, Google internal search, ID1\r\n";

  private static String testCSVMultiValue =
      "term, definition, author\r\n"
          + "moma search, Google internal search, \"ID1, ID2, ID3,    ,ID4,    \"\r\n";

  private static String testCSVSingleWithStructuredDataEmptyDateTime =
      "term, definition, author, created, modified\r\n"
          + "momaSearch, Google internal search, ID1, , "
          + "\r\n";

  private static String testCSVSingleWithStructuredDataMultiDateTime =
      "term, definition, author, updated\r\n"
          + "momaSearch, Google internal search, ID1, "
          + "\"2017-06-18 14:01:23.400-07:00, 2017-06-19 08:01:23.400-07:00\"\r\n";

  @Before
  public void init() throws Exception {
    when(mockRepositoryContext.getDefaultAclMode()).thenReturn(DefaultAclMode.FALLBACK);
  }

  private void createFile(File file, String content) throws IOException {
    try (PrintWriter pw = new PrintWriter(new FileWriter(file))) {
      pw.write(content);
    }
  }

  @Test
  public void testGetAllDocs_unreadableFile() throws IOException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    setupConfig.initConfig(config);

    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    tmpfile.delete();
    thrown.expect(RepositoryException.class);
    csvRepository.getAllDocs(null);
  }

  @Test
  public void testGetAllDocs_empty() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, "term, definition, author\n");
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    setupConfig.initConfig(config);

    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      Iterator<ApiOperation> it = allDocs.iterator();
      assertFalse(it.hasNext());
      thrown.expect(NoSuchElementException.class);
      it.next();
    }
  }

  @Test
  public void testGetAllDocs_iterateTwice() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSV);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    setupConfig.initConfig(config);

    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      HashSet<String> names = new HashSet<>();
      allDocs.iterator();
      thrown.expect(IllegalStateException.class);
      allDocs.iterator();
    }
  }

  @Test
  public void testGetAllDocs() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    setupConfig.initConfig(config);

    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      for (ApiOperation operation : allDocs) {
        RepositoryDoc doc = (RepositoryDoc) operation;

        assertEquals(ContentFormat.HTML, doc.getContentFormat());
        assertEquals(RequestMode.UNSPECIFIED, doc.getRequestMode());
        assertEquals(null, doc.getItem().getAcl());
        assertEquals("moma search", doc.getItem().getName());

        String html = CharStreams.toString(
            new InputStreamReader(doc.getContent().getInputStream(), UTF_8));
        assertThat(html, containsString("<title>moma search</title>"));
        assertThat(html, containsString("<h1>moma search</h1>"));
        assertThat(html, containsString("<h1>Google internal search</h1>"));
      }
    }
  }

  @Test
  public void testGetAllDocsMoreData() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSV);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "title");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "title");
    config.put(CONTENT_TITLE, "title");
    config.put(CONTENT_HIGH, "title,description");
    config.put(CSVFileManager.CSVCOLUMNS, "title,description, ID");
    config.put(CSVFileManager.SKIP_HEADER, "true");
    setupConfig.initConfig(config);
    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      ImmutableList.Builder<String> nameBuilder = ImmutableList.builder();
      for (ApiOperation operation : allDocs) {
        RepositoryDoc doc = (RepositoryDoc) operation;
        nameBuilder.add(doc.getItem().getName());
      }
      assertEquals(
          ImmutableList.of(
              "moma search", "test search1", "test search2", "test search3", "test search4"),
          nameBuilder.build());
    }
  }

  // TODO(jlacey): Actually populate the structured data in these tests.
  @Test
  public void testGetAllDocsWithStructuredData() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSVSingleWithStructuredDataDefaultDateTime);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    config.put(IndexingItemBuilder.TITLE_FIELD, "term");
    config.put(IndexingItemBuilder.UPDATE_TIME_FIELD, "modified");
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, "created");
    config.put(IndexingItemBuilder.CONTENT_LANGUAGE_VALUE, "english");
    config.put(IndexingItemBuilder.OBJECT_TYPE_VALUE, "String");
    setupConfig.initConfig(config);

    // initialize structured data
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Collections.singletonList(
            new ObjectDefinition()
                .setName("String")
                .setPropertyDefinitions(Collections.emptyList())));
    when(mockIndexingService.getSchema()).thenReturn(schema);
    StructuredData.initFromConfiguration(mockIndexingService);
    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      for (ApiOperation operation : allDocs) {
        RepositoryDoc doc = (RepositoryDoc) operation;

        assertEquals(null, doc.getItem().getAcl());

        ItemMetadata expectedItemMetadata =
            new ItemMetadata()
            .setSourceRepositoryUrl("momaSearch")
            .setTitle("momaSearch")
            .setUpdateTime(new DateTime("2017-06-19T08:01:23.400-07:00").toString())
            .setCreateTime(new DateTime("2017-06-18T14:01:23.400-07:00").toString())
            .setContentLanguage("english")
            .setObjectType("String");
        StructuredDataObject structuredData =
            new StructuredDataObject().setProperties(Collections.emptyList());
        Item expectedItem =
            new Item()
            .setName("momaSearch")
            .setMetadata(expectedItemMetadata)
            .setStructuredData(new ItemStructuredData().setObject(structuredData))
            .setItemType("CONTENT_ITEM");

        assertEquals(expectedItem, doc.getItem());

        String html = CharStreams.toString(
            new InputStreamReader(doc.getContent().getInputStream(), UTF_8));
        assertThat(html, containsString("<title>momaSearch</title>"));
        assertThat(html, containsString("<h1>momaSearch</h1>"));
        assertThat(html, containsString("<h1>Google internal search</h1>"));
        assertThat(html, containsString("2017-06-18 14:01:23.400-07:00"));
      }
    }
  }

  @Test
  public void testGetAllDocsWithMissingStructuredData() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSVSingleWithStructuredData);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    config.put(IndexingItemBuilder.TITLE_FIELD, "term");
    config.put(IndexingItemBuilder.UPDATE_TIME_FIELD, "modified");
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, "created");
    config.put(IndexingItemBuilder.OBJECT_TYPE_VALUE, "Object Type");
    setupConfig.initConfig(config);

    // initialize structured data
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Collections.singletonList(
            new ObjectDefinition()
                .setName("Object Type")
                .setPropertyDefinitions(Collections.emptyList())));
    when(mockIndexingService.getSchema()).thenReturn(schema);
    StructuredData.initFromConfiguration(mockIndexingService);
    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      for (ApiOperation operation : allDocs) {
        RepositoryDoc doc = (RepositoryDoc) operation;

        assertEquals(null, doc.getItem().getAcl());

        ItemMetadata expectedItemMetadata =
            new ItemMetadata()
            .setSourceRepositoryUrl("momaSearch")
            .setTitle("momaSearch")
            .setUpdateTime(new DateTime("2017-06-19T08:01:23.400-07:00").toString())
            .setCreateTime(new DateTime("2017-06-18T14:01:23.400-07:00").toString())
            .setObjectType("Object Type");
        Item expectedItem =
            new Item()
            .setName("momaSearch")
            .setMetadata(expectedItemMetadata)
            .setStructuredData(
                new ItemStructuredData()
                .setObject(new StructuredDataObject().setProperties(Collections.emptyList())))
            .setItemType("CONTENT_ITEM");

        assertEquals(expectedItem, doc.getItem());
        assertEquals(
            expectedItem.getMetadata().getUpdateTime(),
            doc.getItem().getMetadata().getUpdateTime());

        String html = CharStreams.toString(
            new InputStreamReader(doc.getContent().getInputStream(), UTF_8));
        assertThat(html, containsString("<title>momaSearch</title>"));
        assertThat(html, containsString("<h1>momaSearch</h1>"));
        assertThat(html, containsString("<h1>Google internal search</h1>"));
      }
    }
  }

  @Test
  public void testGetAllDocsWithEmptyDateTime() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSVSingleWithStructuredDataEmptyDateTime);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    config.put(IndexingItemBuilder.TITLE_FIELD, "term");
    config.put(IndexingItemBuilder.UPDATE_TIME_FIELD, "modified");
    config.put(IndexingItemBuilder.CREATE_TIME_FIELD, "created");
    config.put(IndexingItemBuilder.OBJECT_TYPE_VALUE, "Object Type");
    setupConfig.initConfig(config);

    // initialize structured data
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Collections.singletonList(
            new ObjectDefinition()
                .setName("Object Type")
                .setPropertyDefinitions(Collections.emptyList())));
    when(mockIndexingService.getSchema()).thenReturn(schema);
    StructuredData.initFromConfiguration(mockIndexingService);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      for (ApiOperation operation : allDocs) {
        RepositoryDoc doc = (RepositoryDoc) operation;

        assertEquals(null, doc.getItem().getAcl());

        ItemMetadata expectedItemMetadata =
            new ItemMetadata()
            .setSourceRepositoryUrl("momaSearch")
            .setTitle("momaSearch")
            .setObjectType("Object Type");
        StructuredDataObject structuredData =
            new StructuredDataObject().setProperties(Collections.emptyList());
        Item expectedItem =
            new Item()
            .setName("momaSearch")
            .setMetadata(expectedItemMetadata)
            .setStructuredData(new ItemStructuredData().setObject(structuredData))
            .setItemType("CONTENT_ITEM");

        assertEquals(expectedItem, doc.getItem());
        assertNull(doc.getItem().getMetadata().getUpdateTime());
        assertNull(doc.getItem().getMetadata().getCreateTime());
        String html = CharStreams.toString(
            new InputStreamReader(doc.getContent().getInputStream(), UTF_8));
        assertThat(html, containsString("<title>momaSearch</title>"));
        assertThat(html, containsString("<h1>momaSearch</h1>"));
        assertThat(html, containsString("<h1>Google internal search</h1>"));
      }
    }
  }

  @Test
  public void testGetAllDocsWithMultiDateTime() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSVSingleWithStructuredDataMultiDateTime);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    config.put(IndexingItemBuilder.TITLE_FIELD, "term");
    config.put(IndexingItemBuilder.OBJECT_TYPE_VALUE, "Object Type");
    config.put(CSVFileManager.MULTIVALUE_COLUMNS, "updated");
    setupConfig.initConfig(config);

    // initialize structured data
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Collections.singletonList(
            new ObjectDefinition()
                .setName("Object Type")
                .setPropertyDefinitions(Collections.emptyList())));
    when(mockIndexingService.getSchema()).thenReturn(schema);
    StructuredData.initFromConfiguration(mockIndexingService);

    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      for (ApiOperation operation : allDocs) {
        RepositoryDoc doc = (RepositoryDoc) operation;

        assertEquals(null, doc.getItem().getAcl());

        ItemMetadata expectedItemMetadata =
            new ItemMetadata()
            .setSourceRepositoryUrl("momaSearch")
            .setTitle("momaSearch")
            .setObjectType("Object Type");
        Item expectedItem =
            new Item()
            .setName("momaSearch")
            .setMetadata(expectedItemMetadata)
            .setStructuredData(
                new ItemStructuredData()
                .setObject(new StructuredDataObject().setProperties(Collections.emptyList())))
            .setItemType("CONTENT_ITEM");

        assertEquals(expectedItem, doc.getItem());
        String html = CharStreams.toString(
            new InputStreamReader(doc.getContent().getInputStream(), UTF_8));
        assertThat(html, containsString("<title>momaSearch</title>"));
        assertThat(html, containsString("<h1>momaSearch</h1>"));
        assertThat(html, containsString("<h1>Google internal search</h1>"));
      }
    }
  }

  @Test
  public void testGetAllDocsMultiValue() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSVMultiValue);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CSVFileManager.UNIQUE_KEY_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    config.put(CONTENT_HIGH, "term,definition");
    config.put(CONTENT_LOW, "author");
    config.put(CSVFileManager.MULTIVALUE_COLUMNS, "author");
    setupConfig.initConfig(config);

    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      for (ApiOperation operation : allDocs) {
        RepositoryDoc doc = (RepositoryDoc) operation;

        assertEquals(null, doc.getItem().getAcl());
        assertEquals("moma search", doc.getItem().getName());

        String html = CharStreams.toString(
            new InputStreamReader(doc.getContent().getInputStream(), UTF_8));
        assertThat(html, containsString("<title>moma search</title>"));
        assertThat(html, containsString("<h1>moma search</h1>"));
        assertThat(html, containsString("<h1>Google internal search</h1>"));
        assertThat(html, containsString("<p><small>ID1, ID2, ID3, ID4</small></p>"));
      }
    }
  }

  /** Run the full traversal without a unique ID, twice, and verify that the IDs match. */
  @Test
  public void testGetAllDocsNoUniqueKey_consistentName() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSVSingle);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    setupConfig.initConfig(config);

    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> firstDocs = csvRepository.getAllDocs(null);
        CheckpointCloseableIterable<ApiOperation> secondDocs = csvRepository.getAllDocs(null)) {
      assertEquals(
          ((RepositoryDoc) firstDocs.iterator().next()).getItem().getName(),
          ((RepositoryDoc) secondDocs.iterator().next()).getItem().getName());
    }
  }

  /** Verify that the IDs of each row are unique. */
  @Test
  public void testGetAllDocsNoUniqueKey_distinctNames() throws IOException, InterruptedException {
    File tmpfile = temporaryFolder.newFile(testName.getMethodName() + ".csv");
    createFile(tmpfile, testCSV);
    Properties config = new Properties();
    config.put(CSVFileManager.FILEPATH, tmpfile.getAbsolutePath());
    config.put(UrlBuilder.CONFIG_COLUMNS, "term");
    config.put(CONTENT_TITLE, "term");
    setupConfig.initConfig(config);

    CSVRepository csvRepository = new CSVRepository();
    csvRepository.init(mockRepositoryContext);

    try (CheckpointCloseableIterable<ApiOperation> allDocs = csvRepository.getAllDocs(null)) {
      HashSet<String> names = new HashSet<>();
      for (ApiOperation operation : allDocs) {
        String name = ((RepositoryDoc) operation).getItem().getName();
        assertThat(names, not(hasItem(name)));
        names.add(name);
      }
      assertTrue("Not enough names to test uniqueness: " + names, names.size() > 1);
    }
  }

  @Test
  public void testGetAllDocs_AclModeNONE() throws IOException, InterruptedException {
    setupConfig.initConfig(new Properties());
    RepositoryContext localMockRepositoryContext = mock(RepositoryContext.class);
    when(localMockRepositoryContext.getDefaultAclMode()).thenReturn(DefaultAclMode.NONE);

    CSVRepository csvRepository = new CSVRepository();
    thrown.expect(InvalidConfigurationException.class);
    csvRepository.init(localMockRepositoryContext);
  }

  @Test
  public void getChanges_emptyIterable() throws Exception {
    CSVRepository csvRepository = new CSVRepository();
    try (CheckpointCloseableIterable<ApiOperation> iterable = csvRepository.getChanges(null)) {
      assertNull(iterable.getCheckpoint());
      assertFalse(iterable.hasMore());
      Iterator<ApiOperation> iterator = iterable.iterator();
      assertNotNull(iterator);
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  public void getDoc_throwsException() throws Exception {
    CSVRepository csvRepository = new CSVRepository();
    thrown.expect(UnsupportedOperationException.class);
    csvRepository.getDoc(null);
  }

  @Test
  public void exists_throwsException() throws Exception {
    CSVRepository csvRepository = new CSVRepository();
    thrown.expect(UnsupportedOperationException.class);
    csvRepository.exists(null);
  }

  @Test
  public void getIds_returnsNull() throws Exception {
    CSVRepository csvRepository = new CSVRepository();
    assertNull(csvRepository.getIds(null));
  }

  @Test
  public void close_doesNothing() throws Exception {
    CSVRepository csvRepository = new CSVRepository();
    csvRepository.close();
  }
}
