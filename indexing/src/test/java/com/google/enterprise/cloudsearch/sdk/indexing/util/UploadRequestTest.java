package com.google.enterprise.cloudsearch.sdk.indexing.util;
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

import static java.nio.charset.Charset.defaultCharset;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.enterprise.cloudsearch.sdk.indexing.util.UploadRequest.DeleteRequest;
import com.google.enterprise.cloudsearch.sdk.indexing.util.UploadRequest.GetRequest;
import com.google.enterprise.cloudsearch.sdk.indexing.util.UploadRequest.IndexItemAndContentRequest;
import com.google.enterprise.cloudsearch.sdk.indexing.util.UploadRequest.IndexItemRequest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.junit.Test;

/** Tests for {@link UploadRequest}. */
public class UploadRequestTest {

  private static String testJsonFile =
      "{\"requests\":[{\"id\":\"deleteId123\", \"type\":\"items.delete\"}, "
          + "{\"id\":\"getId213\", \"type\":\"items.get\"}, "
          + "{\"item\":{\"name\":\"updateItem123\"}, \"type\":\"items.indexItem\"}, "
          + "{\"mediaContent\":{\"contentType\":\"test/json\", "
          + "\"contentString\":\"testContentString\"}, "
          + "\"item\":{\"name\":\"updateItemAndConentId123\"}, "
          + "\"type\":\"items.indexItemAndContent\"}], "
          + "\"sourceId\":\"testSourceId\"}";

  @Test
  public void testReadFile() throws IOException {
    InputStream inputStream = new ByteArrayInputStream(testJsonFile.getBytes("UTF-8"));
    UploadRequest upload =
        new JsonObjectParser(JacksonFactory.getDefaultInstance()).parseAndClose(
            new InputStreamReader(inputStream, defaultCharset()), UploadRequest.class);

    assertEquals("testSourceId", upload.sourceId);
    assertEquals(4, upload.requests.size());
    assertThat(upload.requests.get(0), instanceOf(DeleteRequest.class));
    assertThat(upload.requests.get(1), instanceOf(GetRequest.class));
    assertThat(upload.requests.get(2), instanceOf(IndexItemRequest.class));
    assertThat(upload.requests.get(3), instanceOf(IndexItemAndContentRequest.class));
  }
}
