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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;

/** Context for {@link Connector} . */
// TODO(bmj): revoke visibility when splitting ConnectorContext.
@VisibleForTesting
public class ConnectorContextImpl implements ConnectorContext {
  private static final ExceptionHandler DEFAULT_EXCEPTION_HANDLER =
      new ExponentialBackoffExceptionHandler(10, 5, TimeUnit.SECONDS);
  private final ExceptionHandler traversalExceptionHandler;
  private final ExceptionHandler incrementalTraversalExceptionHandler;

  protected ConnectorContextImpl(AbstractBuilder builder) {
    this.traversalExceptionHandler = checkNotNull(builder.traversalExceptionHandler);
    this.incrementalTraversalExceptionHandler = checkNotNull(
        builder.incrementalTraversalExceptionHandler);
  }

  protected abstract static class AbstractBuilder
        <B extends AbstractBuilder<B, T>, T extends ConnectorContext> {
    private ExceptionHandler traversalExceptionHandler = DEFAULT_EXCEPTION_HANDLER;
    private ExceptionHandler incrementalTraversalExceptionHandler = DEFAULT_EXCEPTION_HANDLER;

    protected abstract B getThis();
    public abstract T build();

    public B setTraversalExceptionHandler(ExceptionHandler handler) {
      this.traversalExceptionHandler = checkNotNull(handler);
      return getThis();
    }

    public B setIncrementalTraversalExceptionHandler(ExceptionHandler handler) {
      this.incrementalTraversalExceptionHandler = checkNotNull(handler);
      return getThis();
    }
  }

  static class Builder extends AbstractBuilder<Builder, ConnectorContext> {
    @Override
    protected Builder getThis() {
      return this;
    }

    @Override
    public ConnectorContext build() {
      return new ConnectorContextImpl(this);
    }
  }

  /**
   * Backoff policy for exceptions from {@link Connector#traverse}
   *
   * @return {@link ExceptionHandler} to handle traversal exceptions.
   */
  @Override
  public ExceptionHandler getTraversalExceptionHandler() {
    return traversalExceptionHandler;
  }

  /**
   * Backoff policy for exceptions from {@link IncrementalChangeHandler#handleIncrementalChanges}
   *
   * @return {@link ExceptionHandler} to handle incremental change handling exceptions.
   */
  @Override
  public ExceptionHandler getIncrementalTraversalExceptionHandler() {
    return incrementalTraversalExceptionHandler;
  }
}
