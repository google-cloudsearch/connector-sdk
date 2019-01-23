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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/** CheckpointHandler implementation to maintain in-memory checkpoint values. */
public class InMemoryCheckpointHandler implements CheckpointHandler {

  private final Map<String, byte[]> checkpointHolder = new HashMap<>();

  /** Creates an instance of {@link InMemoryCheckpointHandler} */
  public InMemoryCheckpointHandler() {}

  /** Reads checkpoint value for given checkpoint name. */
  @Override
  public synchronized byte[] readCheckpoint(String checkpointName) throws IOException {
    return checkpointHolder.get(checkpointName);
  }

  /** Saves given checkpoint value. */
  @Override
  public synchronized void saveCheckpoint(String checkpointName, byte[] checkpoint)
      throws IOException {
    if (checkpoint == null) {
      checkpointHolder.remove(checkpointName);
    } else {
      checkpointHolder.put(checkpointName, Arrays.copyOf(checkpoint, checkpoint.length));
    }
  }
}
