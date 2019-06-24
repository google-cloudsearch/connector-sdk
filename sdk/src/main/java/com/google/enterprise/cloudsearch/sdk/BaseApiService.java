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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.logging.Level.FINE;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.WARNING;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient;
import com.google.api.client.googleapis.services.json.AbstractGoogleJsonClientRequest;
import com.google.api.client.http.HttpBackOffIOExceptionHandler;
import com.google.api.client.http.HttpBackOffUnsuccessfulResponseHandler;
import com.google.api.client.http.HttpIOExceptionHandler;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.HttpUnsuccessfulResponseHandler;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.BackOff;
import com.google.api.client.util.Beta;
import com.google.api.client.util.ExponentialBackOff;
import com.google.api.client.util.FieldInfo;
import com.google.api.client.util.escape.Escaper;
import com.google.api.client.util.escape.PercentEscaper;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats.Event;
import java.net.URLDecoder;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Base object encapsulating common functionality for interacting with Google APIs such as setting
 * credentials, request timeouts, error handling and retry etc.
 */
public abstract class BaseApiService<T extends AbstractGoogleJsonClient>
    extends AbstractIdleService {

  protected final T service;
  protected final RetryPolicy retryPolicy;

  protected BaseApiService(AbstractBuilder<? extends AbstractBuilder<?, T>, T> builder) {
    this.service = checkNotNull(builder.service);
    this.retryPolicy = checkNotNull(builder.retryPolicy);
  }

  /** Builder object for creating an instance of {@link BaseApiService}. */
  public abstract static class AbstractBuilder<
      B extends AbstractBuilder<B, T>, T extends AbstractGoogleJsonClient> {
    protected T service;
    protected String rootUrl;
    protected HttpTransport transport;
    protected HttpRequestInitializer requestInitializer;
    protected JsonFactory jsonFactory;
    protected CredentialFactory credentialFactory;
    protected BatchPolicy batchPolicy;
    protected RetryPolicy retryPolicy = new RetryPolicy.Builder().build();
    protected HttpRequestInitializer requestTimeoutInitializer;
    protected GoogleProxy googleProxy = new GoogleProxy.Builder().build();

    /**
     * Sets root URL for Google API client as set on {@link
     * AbstractGoogleJsonClient.Builder#setRootUrl}
     *
     * @param rootUrl root URL for Google API client
     * @return this Builder instance
     */
    public B setRootUrl(String rootUrl) {
      this.rootUrl = rootUrl;
      return getThis();
    }

    /**
     * Sets instance of {@link HttpTransport} to be used for creating an instance of {@link
     * AbstractGoogleJsonClient}.
     *
     * @param transport {@link HttpTransport} to be used for creating an instance of {@link
     *     AbstractGoogleJsonClient}
     * @return this Builder instance
     */
    public B setTransport(HttpTransport transport) {
      this.transport = transport;
      return getThis();
    }

    /**
     * Sets instance of {@link GoogleProxy} to be used for creating an instance of {@link
     * AbstractGoogleJsonClient}.
     *
     * @param proxy {@link GoogleProxy} to be used for creating an instance of {@link
     *     AbstractGoogleJsonClient}
     * @return this Builder instance
     */
    public B setProxy(GoogleProxy proxy) {
      this.googleProxy = proxy;
      return getThis();
    }

    /**
     * Sets an instance of {@link HttpRequestInitializer} to be used to initialize each Google API
     * request.
     *
     * @param requestInitializer instance of {@link HttpRequestInitializer} to be used
     * @return this Builder instance
     */
    public B setRequestInitializer(HttpRequestInitializer requestInitializer) {
      this.requestInitializer = requestInitializer;
      return getThis();
    }

    /**
     * Sets {@link JsonFactory} instance to be used for constructing {@link
     * AbstractGoogleJsonClient}.
     *
     * @param jsonFactory {@link JsonFactory} instance to be used
     * @return this Builder instance
     */
    public B setJsonFactory(JsonFactory jsonFactory) {
      this.jsonFactory = jsonFactory;
      return getThis();
    }

    /**
     * Sets {@link CredentialFactory} to be used to obtained credentials while making Google API
     * requests.
     *
     * @param credentialFactory {@link CredentialFactory} to be used
     * @return this Builder instance
     */
    public B setCredentialFactory(CredentialFactory credentialFactory) {
      this.credentialFactory = credentialFactory;
      return getThis();
    }

    /**
     * Sets {@link BatchPolicy} to be used for request batching.
     *
     * @param batchPolicy {@link BatchPolicy} to be used for request batching.
     * @return this Builder instance
     */
    public B setBatchPolicy(BatchPolicy batchPolicy) {
      this.batchPolicy = batchPolicy;
      return getThis();
    }

    /**
     * Sets request {@link RetryPolicy} to be used for making Google API request.
     *
     * @param retryPolicy {@link RetryPolicy} to be used for making Google API request.
     * @return this Builder instance
     */
    public B setRetryPolicy(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
      return getThis();
    }

    /**
     * Sets pre-built instance of {@link AbstractGoogleJsonClient}.
     *
     * @param service pre-built instance of {@link AbstractGoogleJsonClient}.
     * @return this Builder instance
     */
    public B setService(T service) {
      this.service = service;
      return getThis();
    }

    /**
     * Sets request timeouts for making Google API requests.
     *
     * @param connectTimeoutSeconds socket timeouts in seconds as consumed by {@link
     *     HttpRequest#setConnectTimeout}.
     * @param readTimeoutSeconds read timeout in seconds as consumed by {@link
     *     HttpRequest#setReadTimeout}.
     * @return this Builder instance
     */
    public B setRequestTimeout(int connectTimeoutSeconds, int readTimeoutSeconds) {
      this.requestTimeoutInitializer =
          getRequestTimeoutInitializer(connectTimeoutSeconds, readTimeoutSeconds);
      return getThis();
    }

    /**
     * Get current builder instance.
     *
     * @return current builder instance
     */
    public abstract B getThis();

    /**
     * Get scopes to be used while making Google API requests.
     *
     * @return set of scopes to be used.
     */
    public abstract Set<String> getApiScopes();

    /**
     * Get an instance of {@link AbstractGoogleJsonClient.Builder}
     *
     * @param transport HttpTranport to be used for creating AbstractGoogleJsonClient
     * @param jsonFactory JsonFactory to be used for creating AbstractGoogleJsonClient
     * @param requestInitializer HttpRequestInitializer to be used for creating
     *     AbstractGoogleJsonClient
     * @return an instance of {@link AbstractGoogleJsonClient.Builder}
     */
    @SuppressWarnings("javadoc")
    public abstract AbstractGoogleJsonClient.Builder getServiceBuilder(
        HttpTransport transport,
        JsonFactory jsonFactory,
        HttpRequestInitializer requestInitializer);

    /**
     * Get instance of {@link BaseApiService} implementation. Implementation should call {@link
     * #setupServiceAndCredentials} as part of build implementation.
     *
     * @return Specific {@link BaseApiService} implementation
     */
    @SuppressWarnings("javadoc")
    public abstract BaseApiService<T> build() throws GeneralSecurityException, IOException;

    @SuppressWarnings("unchecked")
    protected GoogleCredential setupServiceAndCredentials()
        throws GeneralSecurityException, IOException {
      checkArgument(credentialFactory != null, "Credential Factory cannot be null.");
      GoogleCredential credential = credentialFactory.getCredential(getApiScopes());
      if (this.service == null) {
        if (jsonFactory == null) {
          jsonFactory = JacksonFactory.getDefaultInstance();
        }

        if (transport == null) {
          transport = googleProxy.getHttpTransport();
        }

        if (requestInitializer == null) {
          requestInitializer =
              new RetryRequestInitializer(
                  checkNotNull(retryPolicy, "retry policy can not be null"));
        }

        AbstractGoogleJsonClient.Builder serviceBuilder =
            getServiceBuilder(
                transport,
                jsonFactory,
                chainedHttpRequestInitializer(
                    credential,
                    requestInitializer,
                    requestTimeoutInitializer,
                    googleProxy.getHttpRequestInitializer()));

        if (!Strings.isNullOrEmpty(rootUrl)) {
          serviceBuilder.setRootUrl(rootUrl);
        }
        service = (T) serviceBuilder.setApplicationName(this.getClass().getName()).build();
      }
      return credential;
    }
  }

  /** Adds a backoff and retry response and exception handlers to the {@code HttpRequest}. */
  @Beta
  public static class RetryRequestInitializer implements HttpRequestInitializer {
    private final RetryPolicy retryPolicy;

    /**
     * Create an instance of {@link RetryRequestInitializer} based on specified {@link RetryPolicy}.
     *
     * @param retryPolicy to setup exponential back off and automatic retries.
     */
    public RetryRequestInitializer(RetryPolicy retryPolicy) {
      this.retryPolicy = retryPolicy;
    }

    /** Initialize {@link HttpRequest} to setup exponential back off and automatic retries. */
    @Override
    public void initialize(HttpRequest request) throws IOException {
      BackOff backOff = new ExponentialBackOff();
      request.setUnsuccessfulResponseHandler(new LoggingResponseHandler(retryPolicy, backOff));
      request.setIOExceptionHandler(new LoggingIOExceptionHandler(backOff));
    }
  }

  /** A wrapper for {@code HttpBackOffUnsuccessfulResponseHandler} that logs the calls. */
  @Beta
  private static class LoggingResponseHandler implements HttpUnsuccessfulResponseHandler {
    private final Logger logger = Logger.getLogger(LoggingResponseHandler.class.getName());
    private final HttpUnsuccessfulResponseHandler delegate;

    public LoggingResponseHandler(RetryPolicy retryPolicy, BackOff backOff) {
      delegate =
          new HttpBackOffUnsuccessfulResponseHandler(backOff)
              .setBackOffRequired(r -> retryPolicy.isRetryableStatusCode(r.getStatusCode()));
    }

    @Override
    public boolean handleResponse(HttpRequest request, HttpResponse response, boolean supportsRetry)
        throws IOException {
      String event =
          logger.isLoggable(INFO)
              ? String.format(
                  "%s %s for %s",
                  response.getStatusCode(),
                  response.getStatusMessage(),
                  request.getUrl().buildRelativeUrl())
              : null;
      if (supportsRetry) {
        logger.log(FINE, "Handling {0}", event);
        boolean retry = delegate.handleResponse(request, response, supportsRetry);
        logger.log(retry ? INFO : WARNING, retry ? "Retrying {0}" : "Not retrying {0}", event);
        return retry;
      } else {
        logger.log(WARNING, "Unable to retry {0}", event);
        return false;
      }
    }
  }

  /** A wrapper for {@code HttpBackOffIOExceptionHandler} that logs the calls. */
  @Beta
  private static class LoggingIOExceptionHandler implements HttpIOExceptionHandler {
    private final Logger logger = Logger.getLogger(LoggingIOExceptionHandler.class.getName());
    private final HttpIOExceptionHandler delegate;

    public LoggingIOExceptionHandler(BackOff backOff) {
      delegate = new HttpBackOffIOExceptionHandler(backOff);
    }

    @Override
    public boolean handleIOException(HttpRequest request, boolean supportsRetry)
        throws IOException {
      String event =
          logger.isLoggable(INFO)
              ? String.format("IOException for %s", request.getUrl().buildRelativeUrl())
              : null;
      if (supportsRetry) {
        logger.log(FINE, "Handling {0}", event);
        boolean retry = delegate.handleIOException(request, supportsRetry);
        logger.log(retry ? INFO : WARNING, retry ? "Retrying {0}" : "Not retrying {0}", event);
        return retry;
      } else {
        logger.log(WARNING, "Unable to retry {0}", event);
        return false;
      }
    }
  }

  private static HttpRequestInitializer chainedHttpRequestInitializer(
      HttpRequestInitializer... initializer) {
    return request -> {
      for (HttpRequestInitializer i : initializer) {
        if (i != null) {
          i.initialize(request);
        }
      }
    };
  }

  private static HttpRequestInitializer getRequestTimeoutInitializer(
      int connectTimeout, int readTimeout) {
    return request -> {
      request.setConnectTimeout(connectTimeout * 1000);
      request.setReadTimeout(readTimeout * 1000);
    };
  }

  private static Object setDefaultValuesForPrimitiveTypes(Object object) {
    if (!(object instanceof GenericJson)) {
      return object;
    }
    GenericJson genericJson = (GenericJson) object;
    for (FieldInfo f : genericJson.getClassInfo().getFieldInfos()) {
      if (Boolean.class.equals(f.getType())) {
        f.setValue(genericJson, Optional.ofNullable(f.getValue(genericJson)).orElse(false));
      } else if (Integer.class.equals(f.getType())) {
        f.setValue(genericJson, Optional.ofNullable(f.getValue(genericJson)).orElse(0));
      } else if (GenericJson.class.isAssignableFrom(f.getType())) {
        setDefaultValuesForPrimitiveTypes(f.getValue(genericJson));
      } else if (Collection.class.isAssignableFrom(f.getType())) {
        Object collection = f.getValue(genericJson);
        if (collection == null) {
          f.setValue(genericJson, Collections.emptyList());
        } else {
          Collection<?> values = (Collection<?>) collection;
          for (Object v : values) {
            setDefaultValuesForPrimitiveTypes(v);
          }
        }
      }
    }
    return object;
  }

  /**
   * This escaper behaves the same as CharEscapers.escapeUriPath,
   * except that semi-colons are also escaped, and the same as
   * UrlEscapers.urlPathSegmentEscaper, except that semi-colons and
   * plus-signs are also escaped. Both characters cause problems
   * somewhere in the Google API Client for Java.
   *
   * @see com.google.api.client.util.escape.CharEscapers#escapeUriPath
   * @see com.google.common.net.UrlEscapers#urlPathSegmentEscaper
   */
  private static final Escaper URL_PATH_SEGMENT_ESCAPER =
      new PercentEscaper(".-~_@:!$&'()*,=", false);

  protected static String escapeResourceName(String name) {
    return URL_PATH_SEGMENT_ESCAPER.escape(name);
  }

  protected static String decodeResourceName(String name) {
    try {
      return URLDecoder.decode(name.replace("+", "%2B"), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new IllegalArgumentException("unable to decode resource name " + name, e);
    }
  }

  /**
   * Common execute method for all api requests.
   *
   * @param request the service API request to perform
   * @param stats OperationStats to update
   * @param initializeDefaults if default values to be initialized for primitive types
   * @return the response result from executing the request
   * @throws IOException when the service throws an exception
   */
  protected static <T> T executeRequest(
      AbstractGoogleJsonClientRequest<T> request, OperationStats stats, boolean initializeDefaults)
      throws IOException {
    Event event = stats.event(request.getClass().getName()).start();
    try {
      T result = request.execute();
      setDefaultValuesForPrimitiveTypes(result);
      event.success();
      return result;
    } catch (Exception e) {
      event.failure();
      throw e;
    }
  }
}
