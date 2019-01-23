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

import static com.google.common.base.Preconditions.checkState;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_GATEWAY_TIMEOUT;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;

/**
 * Object to store policy parameters used for backoff and retry recoverable errors.
 *
 * <p>Store the maximum allowable number of retries and the back-off algorithm (typically an
 * exponential back-off) to use when recoverable HTTP errors occur.
 */
public class RetryPolicy {

  public static final String CONFIG_MAXIMUM_RETRIES = "maxRetryLimit";
  public static final int DEFAULT_MAXIMUM_RETRIES = 5;

  // Recoverable errors returned by the Cloud Search API.
  private static final int HTTP_TOO_MANY_REQUESTS = 429;
  private static final ImmutableSet<Integer> RETRYABLE_STATUS_CODES =
      ImmutableSet.of(
          HTTP_CONFLICT, HTTP_GATEWAY_TIMEOUT, HTTP_UNAVAILABLE, HTTP_TOO_MANY_REQUESTS);
  private final int maxRetries;
  private final BackOffFactory backOffFactory;

  private RetryPolicy(Builder builder) {
    this.maxRetries = builder.maxRetries;
    this.backOffFactory = builder.backOffFactory;
  }

  /**
   * Creates a retry policy using parameters specified in connector configuration file.
   *
   * <ul>
   *    <li>{@code maxRetryLimit=5} - maximum number of
   *        retries of failed (but recoverable) API calls.
   * </ul>
   */
  public static RetryPolicy fromConfiguration() {
    checkState(Configuration.isInitialized(), "config not initialized");
    return new RetryPolicy.Builder()
        .setMaxRetryLimit(
            Configuration.getInteger(CONFIG_MAXIMUM_RETRIES, DEFAULT_MAXIMUM_RETRIES).get())
        .build();
  }

  /** Creates an instance of {@link BackOff} */
  public interface BackOffFactory {
    /**
     * Returns {@link BackOff} instance used for implementing exponential back off for failed
     * requests.
     */
    BackOff createBackOffInstance();
  }

  /**
   * Default factory object used to create an {@link ExponentialBackOff} with an initial delay of
   * 1 second(s) and a multiplier of 2.
   */
  public static class DefaultBackOffFactoryImpl implements BackOffFactory {
    /** Default initial back off delay for retrying request after error. */
    public static final int INITIAL_DELAY_SECONDS = 1;

    /**
     * Default multiplier to compute back off duration as per {@link
     * ExponentialBackOff#getMultiplier}
     */
    public static final double MULTIPLIER = 2;

    /**
     * Returns {@link BackOff} instance used for implementing exponential back off for failed
     * requests.
     */
    @Override
    public BackOff createBackOffInstance() {
      return new ExponentialBackOff.Builder()
          .setInitialIntervalMillis(INITIAL_DELAY_SECONDS * 1000)
          .setMultiplier(MULTIPLIER)
          .build();
    }
  }

  /** Builder for creating an instance of {@link RetryPolicy} */
  public static final class Builder {
    private BackOffFactory backOffFactory = new DefaultBackOffFactoryImpl();
    private int maxRetries = DEFAULT_MAXIMUM_RETRIES;

    /**
     * Sets maximum retry limit for failed requests.
     *
     * @param maxRetries maximum retry limit for failed requests.
     */
    public Builder setMaxRetryLimit(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets {@link BackOffFactory} used for computing exponential back off while retrying failed
     * requests.
     *
     * @param factory {@link BackOffFactory} used for computing exponential back off
     */
    public Builder setBackOffFactory(BackOffFactory factory) {
      this.backOffFactory = factory;
      return this;
    }

    /** Builds an instance of {@link RetryPolicy} */
    public RetryPolicy build() {
      return new RetryPolicy(this);
    }
  }

  /**
   * Gets maximum number of times a failed request would be retried before marking request as
   * failed.
   */
  public int getMaxRetryLimit() {
    return maxRetries;
  }

  /** Gets {@link BackOffFactory} instance used for cmputing exponential back off. */
  public BackOffFactory getBackOffFactory() {
    return this.backOffFactory;
  }

  /**
   * Checks if request with {@code statusCode} response is retryable.
   *
   * @param statusCode to check
   * @return true if request with {@code statusCode} response is retryable, false otherwise.
   */
  public boolean isRetryableStatusCode(int statusCode) {
    return RETRYABLE_STATUS_CODES.contains(statusCode);
  }
}
