package com.google.enterprise.cloudsearch.sdk.indexing.util;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.Json;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.common.io.CharStreams;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.indexing.util.Uploader.UploaderHelper;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Tests the {@link Uploader} utility.
 */
@RunWith(MockitoJUnitRunner.class)
public class UploaderTest {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
  private static final CredentialFactory CREDENTIAL_FACTORY =
      scopes ->
          new MockGoogleCredential.Builder()
              .setTransport(new MockHttpTransport())
              .setJsonFactory(JSON_FACTORY)
              .build();
  private static final Path SERVICE_ACCOUNT_FILE_PATH = Paths.get("./service_account.json");

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private UploaderHelper uploaderHelper;

  @Before
  public void setup() throws Exception {
    when(uploaderHelper.createCredentialFactory(SERVICE_ACCOUNT_FILE_PATH))
        .thenReturn(CREDENTIAL_FACTORY);
    when(uploaderHelper.createTransport()).thenReturn(new MockHttpTransport());
  }

  @Test
  public void testDeleteItem() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items/item1";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                assertEquals("DELETE", method);
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.DeleteRequest deleteRequest = new UploadRequest.DeleteRequest();
    deleteRequest.name = "item1";
    assertEquals("item1", deleteRequest.getName());
    uploadRequest.requests = Collections.singletonList(deleteRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testGetItem() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items/item1";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                assertEquals("GET", method);
                return buildApiRequest(200, new Item());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.GetRequest getRequest = new UploadRequest.GetRequest();
    getRequest.name = "item1";
    uploadRequest.requests = Collections.singletonList(getRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testPushItem() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                return buildApiRequest(200, new Item());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.PushItemRequest pushRequest = new UploadRequest.PushItemRequest();
    pushRequest.name = "item1";
    pushRequest.pushItem = new PushItem();
    uploadRequest.requests = Collections.singletonList(pushRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testUnreserve() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("POST", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items:unreserve";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.UnreserveRequest unreserveRequest = new UploadRequest.UnreserveRequest();
    unreserveRequest.queue = "default";
    assertEquals("default", unreserveRequest.getName());
    uploadRequest.requests = Collections.singletonList(unreserveRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testGetSchema() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("GET", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/datasources/ds1/schema";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Schema());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.GetSchemaRequest getRequest = new UploadRequest.GetSchemaRequest();
    assertEquals("default", getRequest.getName());
    uploadRequest.requests = Collections.singletonList(getRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testDeleteSchema() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("DELETE", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/datasources/ds1/schema";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Schema());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.DeleteSchemaRequest getRequest = new UploadRequest.DeleteSchemaRequest();
    assertEquals("default", getRequest.getName());
    uploadRequest.requests = Collections.singletonList(getRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testUpdateSchema() throws Exception {
    File schemaFile = tempFolder.newFile("schema.json");
    try (FileOutputStream outputStream =
        new FileOutputStream(
            schemaFile,
            // append is false. Overwrite file
            false)) {
      outputStream.write(new Schema().toPrettyString().getBytes(StandardCharsets.UTF_8));
      outputStream.flush();
    }
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("PUT", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/datasources/ds1/schema";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .setBaseUri(tempFolder.getRoot().toURI())
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.UpdateSchemaRequest updateRequest = new UploadRequest.UpdateSchemaRequest();
    // special processing for windows file paths
    updateRequest.schemaJsonFile = "file:///" + schemaFile.getAbsolutePath().replace("\\", "/");
    assertEquals("default", updateRequest.getName());
    uploadRequest.requests = Collections.singletonList(updateRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testUpdateSchemaEmpty() throws Exception {
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .setBaseUri(tempFolder.getRoot().toURI())
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.UpdateSchemaRequest updateRequest = new UploadRequest.UpdateSchemaRequest();
    updateRequest.schemaJsonFile = null;
    uploadRequest.requests = Collections.singletonList(updateRequest);
    thrown.expect(IOException.class);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testUpdateSchemaMissing() throws Exception {
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .setBaseUri(tempFolder.getRoot().toURI())
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.UpdateSchemaRequest updateRequest = new UploadRequest.UpdateSchemaRequest();
    updateRequest.schemaJsonFile = "no-such-file.json";
    uploadRequest.requests = Collections.singletonList(updateRequest);
    thrown.expect(IOException.class);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testIndexItem() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("POST", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items/item1:index";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.IndexItemRequest indexRequest = new UploadRequest.IndexItemRequest();
    indexRequest.item = new Item().setName("item1");
    indexRequest.isIncremental = true;
    uploadRequest.requests = Collections.singletonList(indexRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testIndexItemAndContent() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("POST", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items/item1:index";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.MediaContent mediaContent = new UploadRequest.MediaContent();
    mediaContent.contentType = "text/html";
    mediaContent.contentString = "<html><body><p>Hello world</p></body></html>";
    UploadRequest.IndexItemAndContentRequest indexRequest =
        new UploadRequest.IndexItemAndContentRequest();
    indexRequest.item = new Item().setName("item1");
    indexRequest.isIncremental = true;
    indexRequest.mediaContent = mediaContent;
    uploadRequest.requests = Collections.singletonList(indexRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testIndexItemAndContentNullContent() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("POST", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items/item1:index";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.IndexItemAndContentRequest indexRequest =
        new UploadRequest.IndexItemAndContentRequest();
    indexRequest.item = new Item().setName("item1").setVersion("1");
    indexRequest.isIncremental = true;
    uploadRequest.requests = Collections.singletonList(indexRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testIndexItemAndContentMissingContent() throws Exception {
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.IndexItemAndContentRequest indexRequest =
        new UploadRequest.IndexItemAndContentRequest();
    indexRequest.item = new Item().setName("item1");
    indexRequest.isIncremental = true;
    indexRequest.mediaContent = new UploadRequest.MediaContent();
    // Set mediaContent to non-empty but still missing both url and contentString.
    indexRequest.mediaContent.contentType = "text/plain";
    uploadRequest.requests = Collections.singletonList(indexRequest);
    thrown.expect(IOException.class);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testPollItems() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("POST", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items:poll";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.PollItemsRequest pollRequest = new UploadRequest.PollItemsRequest();
    pollRequest.limit = 10;
    pollRequest.queue = "default";
    pollRequest.statusCodes = Collections.singletonList("MODIFIED");
    assertEquals(Collections.singletonList("MODIFIED").toString(), pollRequest.getName());
    uploadRequest.requests = Collections.singletonList(pollRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testListItems() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("GET", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items?pageSize=10";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.ListRequest listRequest = new UploadRequest.ListRequest();
    listRequest.pageSize = 10;
    assertEquals("default pageToken", listRequest.getName());
    uploadRequest.requests = Collections.singletonList(listRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testDatasourcesList() throws Exception {
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                assertEquals("GET", method);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/settings/datasources";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                return buildApiRequest(200, new Operation());
              }
            });
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();
    UploadRequest uploadRequest = new UploadRequest();
    uploadRequest.sourceId = "ds1";
    UploadRequest.DatasourcesListRequest listRequest = new UploadRequest.DatasourcesListRequest();
    assertEquals("default", listRequest.getName());
    uploadRequest.requests = Collections.singletonList(listRequest);
    uploader.execute(uploadRequest);
  }

  @Test
  public void testUploaderHelper() throws Exception {
    UploaderHelper helper = UploaderHelper.getInstance();
    assertThat(helper.createTransport(), instanceOf(HttpTransport.class));

    File file = tempFolder.newFile("acc.json");
    assertThat(helper.createCredentialFactory(file.toPath()),
        instanceOf(CredentialFactory.class));
  }

  @Test
  public void testUrlInputStreamContent() throws Exception {
    String fileContent = "This is the file content";
    File file = tempFolder.newFile("content.txt");
    try (FileWriter writer = new FileWriter(file)) {
      writer.write(fileContent);
    }
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .build();

    Uploader.UrlInputStreamContent input = uploader.new UrlInputStreamContent(
        "text/plain", file.toURI().toURL());
    assertTrue(input.retrySupported());
    assertEquals(fileContent.length(), input.getLength());
    try (InputStream in = input.getInputStream()) {
      String content = CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8));
      assertEquals(fileContent, content);
    }
  }

  @Test
  public void testBuilder() throws Exception {
    Uploader uploader =
        new Uploader.Builder()
            .setServiceAccountKeyFilePath(SERVICE_ACCOUNT_FILE_PATH)
            .setUploaderHelper(uploaderHelper)
            .setRootUrl("http://example.com/")
            .setTransport(new MockHttpTransport())
            .setRequestTimeout(11, 11)
            .build();
    assertEquals("http://example.com/", uploader.cloudSearchService.getRootUrl());
    assertThat(uploader.cloudSearchService.getRequestFactory().getTransport(),
        instanceOf(MockHttpTransport.class));
  }

  @Test
  public void testMainMissingPayload() throws Exception {
    System.clearProperty("payload");
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("Missing input json file for the requests");
    Uploader.main(null);
  }

  @Test
  public void testMainMissingServiceAccountFile() throws Exception {
    try {
      System.setProperty("payload", "requests.json");
      System.clearProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
      thrown.expect(NullPointerException.class);
      thrown.expectMessage("Missing " + Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
      Uploader.main(null);
    } finally {
      System.clearProperty("payload");
    }
  }

  @Test
  public void testMainInvalidFile() throws Exception {
    try {
      System.setProperty("payload", "no-such-file.json");
      System.setProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG, "acc.json");
      thrown.expect(IOException.class);
      thrown.expectMessage(containsString("does not exist"));
      Uploader.main(null);
    } finally {
      System.clearProperty("payload");
      System.clearProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
    }
  }

  @Test
  public void testMainInvalidPayloadFileMissing() throws Exception {
    try {
      System.setProperty("payload", "no-such-file.json");
      System.setProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG, "acc.json");
      thrown.expect(IOException.class);
      thrown.expectMessage(containsString("does not exist"));
      Uploader.main(null);
    } finally {
      System.clearProperty("payload");
      System.clearProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
    }
  }

  @Test
  public void testMainInvalidPayloadFileDirectory() throws Exception {
    tempFolder.newFolder("folder");
    String userDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempFolder.getRoot().toString());
      System.setProperty("payload", "folder");
      System.setProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG, "acc.json");
      thrown.expect(IOException.class);
      thrown.expectMessage(containsString("is a directory"));
      Uploader.main(null);
    } finally {
      System.setProperty("user.dir", userDir);
      System.clearProperty("payload");
      System.clearProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
    }
  }

  @Test
  public void testMainInvalidPayloadFileNotReadable() throws Exception {
    File tempFile = tempFolder.newFile("requests.json");
    assumeThat(tempFile.setReadable(false), is(true));
    String userDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempFolder.getRoot().toString());
      System.setProperty("payload", "requests.json");
      System.setProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG, "acc.json");
      thrown.expect(IOException.class);
      thrown.expectMessage(containsString("is not readable"));
      Uploader.main(null);
    } finally {
      System.setProperty("user.dir", userDir);
    }
  }

  @Test
  public void testMainSetTimeoutInvalid() throws Exception {
    File tempFile = tempFolder.newFile("requests.json");
    String userDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempFolder.getRoot().toString());
      System.setProperty("payload", "file.json");
      System.setProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG, "acc.json");
      System.setProperty("contentUpload.requestTimeout", "not-a-number");
      thrown.expect(NumberFormatException.class);
      Uploader.main(null);
    } finally {
      System.setProperty("user.dir", userDir);
      System.clearProperty("payload");
      System.clearProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
      System.clearProperty("contentUpload.requestTimeout");
    }
  }

  @Test
  public void testMainPayloadEmpty() throws Exception {
    File tempFile = tempFolder.newFile("requests.json");
    String userDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempFolder.getRoot().toString());
      System.setProperty("payload", "requests.json");
      System.setProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG, "acc.json");
      thrown.expect(IllegalArgumentException.class);
      Uploader.main(null);
    } finally {
      System.setProperty("user.dir", userDir);
      System.clearProperty("payload");
      System.clearProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
    }
  }

  @Test
  public void testMain() throws Exception {
    String fileContent =
        "{"
        + "\"sourceId\" : \"ds1\","
        + "\"requests\" : ["
        + "  {\"name\" : \"item1\", \"type\" : \"items.delete\"}"
        + "] "
        + "}";
    File requestsFile = tempFolder.newFile("requests.json");
    try (FileWriter writer = new FileWriter(requestsFile)) {
      writer.write(fileContent);
    }
    AtomicBoolean sentRequest = new AtomicBoolean(false);
    when(uploaderHelper.createTransport())
        .thenReturn(
            new MockHttpTransport() {
              @Override
              public MockLowLevelHttpRequest buildRequest(String method, String url)
                  throws IOException {
                sentRequest.set(true);
                String expectedUrl =
                    "https://cloudsearch.googleapis.com/v1/indexing/"
                        + "datasources/ds1/items/item1";
                assertTrue(url + " not starting with " + expectedUrl, url.startsWith(expectedUrl));
                assertEquals("DELETE", method);
                return buildApiRequest(200, new Operation());
              }
            });

    String userDir = System.getProperty("user.dir");
    try {
      System.setProperty("user.dir", tempFolder.getRoot().toString());
      System.setProperty("payload", "requests.json");
      // main will resolve the service account against user.dir, so adjust the mock accordingly
      Path serviceAccountFilePath = tempFolder.getRoot().toPath().resolve("service_account.json");
      when(uploaderHelper.createCredentialFactory(serviceAccountFilePath))
          .thenReturn(CREDENTIAL_FACTORY);
      System.setProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG, "service_account.json");
      Uploader.UploaderHelper.setInstance(uploaderHelper);
      Uploader.main(null);
      assertTrue(sentRequest.get());
    } finally {
      System.setProperty("user.dir", userDir);
      System.clearProperty("payload");
      System.clearProperty(Uploader.SERVICE_ACCOUNT_KEY_FILE_CONFIG);
    }
  }

  @Test
  public void testGetUploadRequest() throws Exception {
    String testJsonFile =
        "{"
        + "\"sourceId\" : \"ds1\","
        + "\"requests\" : ["
        + "  {\"name\" : \"item1\", \"type\" : \"items.delete\"}"
        + "] "
        + "}";

    UploadRequest request =
        Uploader.getUploadRequest(new InputStreamReader(
                new ByteArrayInputStream(testJsonFile.getBytes(StandardCharsets.UTF_8))));
    UploadRequest.DeleteRequest deleteRequest = new UploadRequest.DeleteRequest();
    deleteRequest.name = "item1";
    deleteRequest.type = "items.delete";
    assertEquals("ds1", request.sourceId);
    assertEquals(Collections.singletonList(deleteRequest), request.requests);
  }

  private static MockLowLevelHttpRequest buildApiRequest(
      int responseCode, GenericJson apiResponse) {
    return new MockLowLevelHttpRequest("https://www.googleapis.com/mock/v1") {
      @Override
      public MockLowLevelHttpResponse execute() throws IOException {
        MockLowLevelHttpResponse response = new MockLowLevelHttpResponse();
        response
            .setStatusCode(responseCode)
            .setContentType(Json.MEDIA_TYPE)
            .setContent(JSON_FACTORY.toString(apiResponse));
        return response;
      }
    };
  }
}
