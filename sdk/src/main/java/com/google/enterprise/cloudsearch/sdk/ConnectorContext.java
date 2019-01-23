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

/**
 * Interface for a context object created by the SDK to pass to the {@link Connector} code.
 *
 * <p>The {@link Application} object creates a context instance containing {@link ExceptionHandler}
 * instances. It calls {@link Connector#init(ConnectorContext)} method to pass the context to the
 * connector code.
 */
public interface ConnectorContext {
  /**
   * Returns the exception handler used by the
   * {@link IncrementalChangeHandler#handleIncrementalChanges()} method call.
   *
   * @return {@link ExceptionHandler} to handle incremental change traversal exceptions
   */
  ExceptionHandler getIncrementalTraversalExceptionHandler();

  /**
   * Returns the exception handler used by the {@link Connector#traverse()} method call.
   *
   * @return {@link ExceptionHandler} to handle traversal exceptions
   */
  ExceptionHandler getTraversalExceptionHandler();
}