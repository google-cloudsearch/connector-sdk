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
package com.google.enterprise.cloudsearch.sdk;

import java.io.IOException;

/**
 * Interface for handling repository changes.
 *
 * <p>The main benefit for having the {@link Connector} implement this interface is that it enables
 * the framework to schedule polling of incremental changes.
 */
public interface IncrementalChangeHandler {
  /**
   * Handles the {@link Connector} specific implementation of incremental change notifications.
   *
   * @throws IOException if getting changes fails
   * @throws InterruptedException if an IO operations throws it
   */
  public void handleIncrementalChanges() throws IOException, InterruptedException;
}
