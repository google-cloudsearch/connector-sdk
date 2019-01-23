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
package com.google.enterprise.cloudsearch.sdk.indexing.template;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.enterprise.cloudsearch.sdk.RepositoryException;
import java.io.IOException;
import java.util.function.Supplier;

/** Base class for checkpoints, with generic code for parsing and generating them. */
abstract class JsonCheckpoint extends GenericJson implements Supplier<byte[]> {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /**
   * Default constructor for Json parsing
   *
   * <p>This class and constructor must be public for the JSON parser to run correctly.
   */
  public JsonCheckpoint() {
    setFactory(JSON_FACTORY);
  }

  @Override
  public byte[] get() {
    try {
      return this.toPrettyString().getBytes(UTF_8);
    } catch (IOException e) {
      throw new RuntimeException("error encoding checkpoint", e);
    }
  }

  static <T extends JsonCheckpoint> T parse(byte[] payload, Class<T> clazz)
      throws RepositoryException {
    if (payload == null) {
      return null;
    }
    String checkpoint = new String(payload, UTF_8);
    try {
      return JSON_FACTORY.fromString(checkpoint, clazz);
    } catch (IOException e) {
      throw new RepositoryException.Builder()
          .setErrorMessage("Error parsing checkpoint " + checkpoint + " as "
              + clazz.getSimpleName())
          .setCause(e).build();
    }
  }
}
