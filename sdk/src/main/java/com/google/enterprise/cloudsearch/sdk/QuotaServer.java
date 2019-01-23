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
import static com.google.common.base.Preconditions.checkState;

import com.google.common.util.concurrent.RateLimiter;
import com.google.enterprise.cloudsearch.sdk.config.ConfigValue;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility object to enforce quota requirements.
 *
 * <p>Create an instance to enforce individual quota maximums on a set of operations defined by an
 * enumeration. Before executing a quota restricted operation, call the {@link QuotaServer#acquire}
 * method to restrict the rate at which the operation is allowed to execute. Internally, quota is
 * enforced using a {@link RateLimiter} object. The calling thread is blocked if the token cannot be
 * granted immediately.
 */
public class QuotaServer<T extends Enum<T>> {
  public static final double DEFAULT_QPS = 10;
  private static final String CONFIG_QUOTA_SERVER_ENUM_KEY_FORMAT = "quotaServer.%s.%s";
  private static final String CONFIG_QUOTA_SERVER_DEFAULT_QPS_FORMAT = "quotaServer.%s.defaultQps";

  private final Map<T, RateLimiter> quotaMap;

  private QuotaServer(Builder<T, ? extends QuotaServer<T>> builder) {
    quotaMap = new HashMap<T, RateLimiter>();
    builder
        .operationQps
        .keySet()
        .forEach(k -> quotaMap.put(k, RateLimiter.create(builder.operationQps.get(k))));
  }

  /**
   * Acquires a token before allowing an operation to execute.
   *
   * <p>Note: The calling thread is blocked if the token cannot be granted immediately.
   *
   * @param operation the enumeration operation to rate limit base on its quota
   * @return time spent sleeping to enforce quota in seconds (0.0 if not rate limited)
   */
  public double acquire(T operation) {
    checkArgument(quotaMap.containsKey(operation), "undefined operation");
    return quotaMap.get(operation).acquire();
  }

  /**
   * Returns current QPS rate limit for the specified enumeration operation.
   *
   * @param operation for rate limit lookup
   * @return current configured rate limit for the operation
   */
  public double getRate(T operation) {
    checkArgument(quotaMap.containsKey(operation), "undefined operation");
    return quotaMap.get(operation).getRate();
  }

  /**
   * Creates a {@link QuotaServer} instance using parameter values from the configuration file.
   *
   * <p>The configuration file should have QPS value parameters in the format of:
   * <ul>
   *   <li>quotaServer.[quotaPrefix].defaultQps = 10
   *   <li>quotaServer.[quotaPrefix].ENUM_OPERATION1 = 2;
   *   <li>quotaServer.[quotaPrefix].ENUM_OPERATION2 = 5;
   *   <li>quotaServer.[quotaPrefix].ENUM_OPERATION3 = 20;
   * </ul>
   * <p>Where {@code quotaPrefix} is defined by the connector and the {@code ENUM_*} values are
   * operation types defined in an enumeration. Any unspecified {@code ENUM_*} values take the QPS
   * value from the {@code defaultQps} parameter.
   *
   * @param quotaPrefix prefix for configuration keys related to quota server parameters
   * @param enumClass class for enumerations representing operations
   * @return a {@link QuotaServer} instance
   */
  public static <T extends Enum<T>> QuotaServer<T> createFromConfiguration(
      String quotaPrefix, Class<T> enumClass) {
    checkState(Configuration.isInitialized(), "configuration not initialized yet");
    checkNotNull(enumClass);
    T[] enumValues = enumClass.getEnumConstants();
    checkNotNull(enumValues, "class " + enumClass + " is not an enum class");
    double defaultQps =
        Configuration.getValue(
                String.format(CONFIG_QUOTA_SERVER_DEFAULT_QPS_FORMAT, quotaPrefix),
                DEFAULT_QPS,
                Configuration.DOUBLE_PARSER)
            .get();
    QuotaServer.Builder<T, QuotaServer<T>> builder =
        new QuotaServer.Builder<T, QuotaServer<T>>(enumClass);
    for (T operation : enumValues) {
      String configKey = String.format(CONFIG_QUOTA_SERVER_ENUM_KEY_FORMAT, quotaPrefix, operation);
      ConfigValue<Double> qps =
          Configuration.getValue(configKey, defaultQps, Configuration.DOUBLE_PARSER);
      if (qps.get() <= 0) {
        throw new InvalidConfigurationException(
            "QPS for " + configKey + " can not be 0 or negative. Configured as " + qps.get());
      }
      builder.addQuota(operation, qps.get());
    }
    return builder.build();
  }

  /** Builder for {@link QuotaServer} instances. */
  public static class Builder<T extends Enum<T>, K extends QuotaServer<T>> {
    private Map<T, Double> operationQps = new HashMap<T, Double>();
    private double defaultQps = DEFAULT_QPS;
    private Set<T> allOperations;

    /** Sets the enum class of the supported operations. */
    public Builder(Class<T> enumClass) {
      checkNotNull(enumClass, "enumClass can not be null");
      T[] enumValues = enumClass.getEnumConstants();
      checkArgument(enumValues != null, "class " + enumClass + " is not an enum class");
      allOperations = Arrays.stream(enumValues).collect(Collectors.toSet());
    }

    /** Sets the {@code qps} quota for a given {@code operation}. */
    public Builder<T, K> addQuota(T operation, double qps) {
      checkArgument(allOperations.contains(operation), "undefined operation");
      checkArgument(qps > 0, "qps can not be 0 or negative");
      operationQps.put(operation, qps);
      return this;
    }

    /** Sets the default qps {@code qps} if such setting is not provided for an operation. */
    public Builder<T, K> setDefaultQps(double defaultQps) {
      checkArgument(defaultQps > 0, "defaultQps can not be 0 or negative");
      this.defaultQps = defaultQps;
      return this;
    }

    /** Builds an instance of {@link QuotaServer}. */
    public QuotaServer<T> build() {
      allOperations
          .stream()
          .filter(k -> !operationQps.containsKey(k))
          .forEach(k -> operationQps.put(k, defaultQps));
      return new QuotaServer<T>(this);
    }
  }
}
