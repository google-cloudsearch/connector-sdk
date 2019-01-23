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
package com.google.enterprise.cloudsearch.sdk.config;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.common.base.Strings;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.Parser;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Placeholder object for {@link Configuration} values.
 *
 * <p>Obtain an instance of {@link ConfigValue} by using {@link Configuration} as a static factory.
 * Before retrieving the configured value, execute {@link Configuration#initConfig}. To retrieve a
 * configuration value once it's initialized, use {@link ConfigValue#get}.
 */
public class ConfigValue<T> {
  private final String configKey;
  private final T defaultValue;
  private final ConfigValue<T> fallback;
  private T configuredValue;
  private final Parser<T> parser;
  private final AtomicBoolean initialized;

  private ConfigValue() {
    throw new AssertionError("Use Builder to create instance of ConfigValue");
  }

  private ConfigValue(Builder<T> builder) {
    configKey = builder.configKey;
    defaultValue = builder.defaultValue;
    fallback = builder.fallback;
    parser = builder.parser;
    initialized = new AtomicBoolean();
  }

  /**
   * Gets the configured value.
   *
   * <p>This can only be called after {@link Configuration#initConfig(Properties)} initialization
   * has occurred.
   *
   * @return configured value
   * @throws IllegalStateException if not initialized.
   */
  public T get() {
    checkState(initialized.get(), String.format("Config Key %s not initialized", getConfigKey()));
    return configuredValue;
  }

  /**
   * Get default value for configuration.
   *
   * @return default value for configuration.
   */
  public T getDefault() {
    return defaultValue;
  }

  /**
   * Checks whether the value has been initialized.
   *
   * @return {@code true} if value is initialized
   */
  public boolean isInitialized() {
    return initialized.get();
  }

  /**
   * Initializes the configuration value as available in config file.
   *
   * @param value configured value
   */
  synchronized void initialize(String value) {
    if (initialized.get()) {
      return;
    }
    if (!Strings.isNullOrEmpty(value)) {
      try {
        T parsed = parser.parse(value);
        configuredValue = parsed;
      } catch (InvalidConfigurationException e) {
        throw new InvalidConfigurationException(
            String.format(
                "Failed to parse configured value [%s] for ConfigKey [%s]", value, configKey),
            e);
      }
    } else {
      if (fallback != null) {
        configuredValue = fallback.get();
      } else {
        if (defaultValue == null) {
          throw new InvalidConfigurationException(
              String.format("Required Config Key %s not initialized", getConfigKey()));
        }
        configuredValue = defaultValue;
      }
    }
  }

  String getConfigKey() {
    return configKey;
  }

  synchronized void freeze() {
    initialized.set(true);
  }

  synchronized void reset() {
    if (!isInitialized()) {
      return;
    }
    configuredValue = null;
    initialized.set(false);
  }

  static final class Builder<T> {
    private String configKey;
    private T defaultValue;
    public ConfigValue<T> fallback;
    private Parser<T> parser;

    public Builder<T> setConfigKey(String configKey) {
      this.configKey = configKey;
      return this;
    }

    public Builder<T> setDefaultValue(T defaultValue) {
      this.defaultValue = defaultValue;
      return this;
    }

    public Builder<T> setFallback(ConfigValue<T> fallback) {
      this.fallback = fallback;
      this.parser = fallback.parser;
      return this;
    }

    public Builder<T> setParser(Parser<T> parser) {
      this.parser = parser;
      return this;
    }

    public ConfigValue<T> build() {
      checkArgument(!Strings.isNullOrEmpty(configKey), "configKey can not be empty or null");
      checkNotNull(parser, "parser can not be null.");
      checkArgument((defaultValue == null || fallback == null),
          "both defaultValue && fallback can not be set together");
      return new ConfigValue<T>(this);
    }
  }
}
