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
package com.google.enterprise.cloudsearch.sdk.indexing;

import com.google.api.client.http.AbstractInputStreamContent;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import java.io.IOException;

/**
 * Service for uploading media content using Google Cloud Search ByteStream API.
 */
public interface ContentUploadService extends Service {
  /**
   * Uploads {@link AbstractInputStreamContent} content for specified resource name obtained using
   * the Cloud Search upload method.
   *
   * @param resourceName resource name for content to be uploaded.
   * @param content {@link AbstractInputStreamContent} to upload
   * @return {@link ListenableFuture} representing upload result
   * @throws IOException if request fails
   */
  ListenableFuture<Void> uploadContent(String resourceName, AbstractInputStreamContent content)
      throws IOException;
}
