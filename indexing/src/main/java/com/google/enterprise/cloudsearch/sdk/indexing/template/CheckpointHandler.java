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

import java.io.IOException;

/**
 * Wrapper object to read / write a checkpoint. An implementation is expected to throw {@link
 * IOException} if reading or writing of the checkpoint fails.
 */
public interface CheckpointHandler {
  /**
   * Read current value of saved checkpoint.
   *
   * @param checkpointName the name of the checkpoint to read
   * @return current value of checkpoint. {@code null} if checkpoint is empty or not available
   * @throws IOException if checkpoint read fails
   */
  byte[] readCheckpoint(String checkpointName) throws IOException;

  /**
   * Saves checkpoint value.
   *
   * @param checkpointName the name of the checkpoint to save
   * @param checkpoint value to save
   * @throws IOException if checkpoint save fails
   */
  void saveCheckpoint(String checkpointName, byte[] checkpoint) throws IOException;
}
