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

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Static factory for handling connector configurations.
 *
 * <p>The caller can use one of the available factory methods to create an instance of {@link
 * ConfigValue}. If {@link Configuration#initConfig} is not yet executed, this factory holds a
 * references to a registered {@link ConfigValue} instances to initialize later. Once the {@link
 * Configuration} is initialized, each new {@link ConfigValue} instance is initialized immediately
 * according to the loaded configuration values. Use {@link ResetConfigRule} in unit tests to reset
 * the {@link Configuration} object. Configuration values read from properties file are trimmed
 * before parsing.
 *
 * <p>For example, caller could use one of the factory methods to define {@link ConfigValue}.
 *
 * <pre>{@code
 * ConfigValue<Boolean> booleanParam = Configuration.getBoolean("config.key1", false);
 * ConfigValue<Integer> integerParam = Configuration.getInteger("config.key2", 10);
 * ConfigValue<String> stringParam = Configuration.getString("required.config.key3", null);
 * Parser<URL> urlParser =
 *     value -> {
 *       try {
 *         return new URL(value);
 *       } catch (MalformedURLException e) {
 *         throw new InvalidConfigurationException(e);
 *       }
 *     };
 * ConfigValue<URL> configuredUrl =
 *     Configuration.getValue("required.config.key4", null, urlParser);
 * }</pre>
 */
public class Configuration {
  private static final Logger logger = Logger.getLogger(Configuration.class.getName());
  private static final String CONNECTOR_CONFIG_FILE = "connector-config.properties";
  private static final String ARGS_KEY = "-D";
  private static final String ARGS_CONFIGFILE = "config";
  @SuppressWarnings("rawtypes")
  private static final List<ConfigValue> configurations = new ArrayList<>();
  private static final AtomicBoolean initialized = new AtomicBoolean();
  private static Properties loadedConfig;

  /**
   * Initializes the {@link Configuration} instance using the properties file specified in
   * the command line arguments. Properties can also be specified on the command line as
   * "-DpropertyName=propertyValue". A property given on the command line will override
   * the same property from the config file.
   *
   * <p>Caller should check {@link Configuration#isInitialized()} before calling
   * {@link Configuration#initConfig}.
   *
   * @param args command line arguments
   */
  public static void initConfig(String[] args) throws IOException {
    checkNotNull(args, "arguments can not be null");
    Properties props = parseArgs(args);
    String configFilePath = props.getProperty(ARGS_CONFIGFILE, CONNECTOR_CONFIG_FILE);
    File configFile = new File(configFilePath);
    Properties configuredProperties = new Properties();
    if (configFile.exists()) {
      try (FileInputStream in = new FileInputStream(configFile)) {
        configuredProperties.load(in);
      }
    }
    // Override config file with command line values.
    configuredProperties.putAll(props);

    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    //decode each property value
    for (Entry<Object, Object> entry : configuredProperties.entrySet()) {
      String key = (String) entry.getKey();
      String value = (String) entry.getValue();
      configuredProperties.setProperty(key, sensitiveDataCodec.decodeData(value.trim()));
    }
    initConfig(configuredProperties);
  }

  /**
   * Initializes the {@link Configuration} instance using the provided {@link Properties}.
   *
   * <p>Caller should check {@link Configuration#isInitialized()} before calling
   * {@link Configuration#initConfig}
   *
   * @param config {@link Properties} properties to initialize
   */
  @SuppressWarnings("rawtypes")
  public static synchronized void initConfig(Properties config) {
    checkNotNull(config, "config can not be null");
    if (initialized.get()) {
      Map<String, String> loadedMap = flatten(loadedConfig);
      Map<String, String> newMap = flatten(config);
      if (loadedMap.equals(newMap)) {
        logger.log(Level.CONFIG, "Attempt to reload config with same values; ignoring.");
        return;
      } else {
        logger.log(Level.CONFIG, "Properties in only one config: "
            + Sets.symmetricDifference(loadedMap.keySet(), newMap.keySet()));
        logger.log(Level.CONFIG, "Properties with different values in the configs: "
            + Maps.difference(loadedMap, newMap).entriesDiffering());
        checkState(false, "Attempt to reload config with different properties.");
      }
    }
    loadedConfig = config;
    synchronized (configurations) {
      try {
        for (ConfigValue value : configurations) {
          initializeConfigValue(value);
        }
        initialized.set(true);
      } finally {
        if (initialized.get()) {
          configurations.forEach(v -> v.freeze());
        } else {
          // Reset ConfigValue if initialization fails.
          configurations.forEach(v -> v.reset());
          loadedConfig = null;
        }
      }
    }
    return;
  }

  /**
   * Properties objects can have an enclosed default Properties, but Properties doesn't
   * override equals() to take that into account.
   *
   * @param p a Properties object
   * @return a map with the properties, including any defaults not overridden
   */
  private static Map<String, String> flatten(Properties p) {
    return p.stringPropertyNames()
        .stream()
        .collect(ImmutableMap.toImmutableMap(name -> name, name -> p.getProperty(name)));
  }

  /**
   * Retrieves all of the configuration properties.
   *
   * <p>Caller should check {@link Configuration#isInitialized()} before calling.
   *
   * @return configuration properties
   */
  public static Properties getConfig() {
    checkState(initialized.get(), "configuration not initialized yet");
    Properties copyToReturn = new Properties();
    copyToReturn.putAll(loadedConfig);
    return copyToReturn;
  }

  /**
   * Checks whether the {@link Configuration} is initialized.
   *
   * @return true if configuration has been initialized. false otherwise.
   */
  public static boolean isInitialized() {
    return initialized.get();
  }

  /**
   * General purpose {@link ConfigValue} parser.
   */
  public interface Parser<T> {
    /**
     * Parses input String to required type.
     *
     * @param value to parse
     * @return Converted value
     * @throws InvalidConfigurationException if parsing fails
     */
    T parse(String value) throws InvalidConfigurationException;
  }

  private Configuration() {
    throw new AssertionError();
  }

  /**
   * {@link Configuration.Parser} for parsing string values as boolean. This parser supports only
   * 'true' and 'false' as valid values, ignoring case.
   */
  public static final Parser<Boolean> BOOLEAN_PARSER =
      value -> {
        checkArgument(!Strings.isNullOrEmpty(value), "value to parse can not be null or empty.");
        if ("true".equalsIgnoreCase(value)) {
          return true;
        }
        if ("false".equalsIgnoreCase(value)) {
          return false;
        }
        throw new InvalidConfigurationException(
            String.format(
                "Invalid configuration value [%s] for boolean configuration property", value));
      };

  /** {@link Configuration.Parser} for parsing string values as integer. */
  public static final Parser<Integer> INTEGER_PARSER =
      value -> {
        checkArgument(!Strings.isNullOrEmpty(value), "value to parse can not be null or empty.");
        try {
          return Integer.parseInt(value);
        } catch (NumberFormatException e) {
          throw new InvalidConfigurationException(e);
        }
      };

  /** {@link Configuration.Parser} for parsing string values. */
  public static final Parser<String> STRING_PARSER =
      value -> {
        checkNotNull(value, "value to parse can not be null.");
        return value;
      };

  /** {@link Configuration.Parser} for parsing string values as double. */
  public static final Parser<Double> DOUBLE_PARSER =
      value -> {
        checkArgument(!Strings.isNullOrEmpty(value));
        try {
          return Double.parseDouble(value);
        } catch (NumberFormatException e) {
          throw new InvalidConfigurationException(e);
        }
      };

  /** {@link Configuration.Parser} for parsing delimited string as list of objects. */
  private static class ListParser<T> implements Parser<List<T>> {
    private final Parser<T> valueParser;
    private final String delimiter;

    ListParser(Parser<T> valueParser, String delimiter) {
      this.valueParser = valueParser;
      this.delimiter = delimiter;
    }

    @Override
    public List<T> parse(String value) {
      checkNotNull(value);
      ImmutableList.Builder<T> parsed = new ImmutableList.Builder<T>();
      Splitter.on(delimiter).trimResults().split(value)
          .forEach(v -> parsed.add(valueParser.parse(v)));
      return parsed.build();
    }
  }

  /**
   * Allows for chaining default configuration values.
   *
   * <p>This is used to allow fetching a configuration value while using a previously defined
   * configuration value as a default value. That default can also be the result of a another
   * parameter, and so on.
   *
   * <p>For example:
   * <pre>{@code
   *   // set configDefaultValue to the configuration file value, or 10 if not defined
   *   ConfigValue<Integer> configDefaultValue = Configuration.getInteger("connector.count", 10);
   *
   *   // set configSpecificValue to the configuration file value or configDefaultValue
   *   ConfigValue<Integer> configSpecificValue =
   *       Configuration.getOverriden("specific.connector.count", configDefaultValue);
   *
   *   // at this point configSpecificValue is set to:
   *   //    1) the configuration value for "specific.connector.count"; if not defined then:
   *   //    2) the configuration value for "connector.count"; if that is not defined then:
   *   //    3) 10, which is the default value for "connector.count".
   * } </pre>
   */
  public static <T> ConfigValue<T> getOverriden(String configKey, ConfigValue<T> defaultValue) {
    ConfigValue<T> toReturn =
        new ConfigValue.Builder<T>()
            .setConfigKey(configKey)
            .setFallback(defaultValue)
            .build();
    initializeOrRegister(toReturn);
    return toReturn;
  }

  /**
   * Get configuration values parsed as boolean for supplied configuration key.
   *
   * @param configKey configuration key to read value for
   * @param defaultValue default value to return if configuration key value doesn't exist.
   */
  public static ConfigValue<Boolean> getBoolean(String configKey, Boolean defaultValue) {
    ConfigValue<Boolean> toReturn =
        new ConfigValue.Builder<Boolean>()
            .setConfigKey(configKey)
            .setDefaultValue(defaultValue)
            .setParser(BOOLEAN_PARSER)
            .build();
    initializeOrRegister(toReturn);
    return toReturn;
  }

  /**
   * Get configuration values parsed as string for supplied configuration key.
   *
   * @param configKey configuration key to read value for
   * @param defaultValue default value to return if configuration key value doesn't exist.
   */
  public static ConfigValue<String> getString(String configKey, String defaultValue) {
    ConfigValue<String> toReturn =
        new ConfigValue.Builder<String>()
            .setConfigKey(configKey)
            .setDefaultValue(defaultValue)
            .setParser(STRING_PARSER)
            .build();
    initializeOrRegister(toReturn);
    return toReturn;
  }

  /**
   * Get configuration values parsed as integer for supplied configuration key.
   *
   * @param configKey configuration key to read value for
   * @param defaultValue default value to return if configuration key value doesn't exist.
   */
  public static ConfigValue<Integer> getInteger(String configKey, Integer defaultValue) {
    ConfigValue<Integer> toReturn =
        new ConfigValue.Builder<Integer>()
            .setConfigKey(configKey)
            .setDefaultValue(defaultValue)
            .setParser(INTEGER_PARSER)
            .build();
    initializeOrRegister(toReturn);
    return toReturn;
  }

  /**
   * Allows for retrieving a parameter value that consists of a comma delimited list.
   *
   * <p>The most common usage is for a configuration parameter that is a list of comma delimited
   * strings. For example:
   *
   * <pre>{@code
   *   // list of strings
   *   ConfigValue<List<String>> listParam =
   *       Configuration.getMultiValue("config.stringList", null, Configuration.STRING_PARSER); }
   * </pre>
   *
   * @param configKey configuration file parameter key
   * @param defaultValues list of default key values
   * @param parser specific parser to support the list object's data type
   * @param <T> data type of the list object
   * @return the list type object
   */
  public static <T> ConfigValue<List<T>> getMultiValue(
      String configKey, List<T> defaultValues, Parser<T> parser) {
    return getMultiValue(configKey, defaultValues, parser, ",");
  }

  /**
   * Allows for retrieving a parameter value that consists of a delimited list.
   * For a comma-delimited list, see {@link #getMultiValue(String, List, Parser)
   * getMultiValue}.
   *
   * <p>The most common usage is for a configuration parameter that is a list of delimited
   * strings. For example:
   *
   * <pre>{@code
   *   // list of strings
   *   ConfigValue<List<String>> listParam =
   *       Configuration.getMultiValue(
   *           "config.stringList", null, Configuration.STRING_PARSER, ";"); }
   * </pre>
   *
   * @param configKey configuration file parameter key
   * @param defaultValues list of default key values
   * @param parser specific parser to support the list object's data type
   * @param delimiter the character between multiple values
   * @param <T> data type of the list object
   * @return the list type object
   */
  public static <T> ConfigValue<List<T>> getMultiValue(
      String configKey, List<T> defaultValues, Parser<T> parser, String delimiter) {
    ConfigValue<List<T>> toReturn =
        new ConfigValue.Builder<List<T>>()
            .setConfigKey(configKey)
            .setDefaultValue(defaultValues)
            .setParser(new ListParser<T>(parser, delimiter))
            .build();
    initializeOrRegister(toReturn);
    return toReturn;
  }

  /**
   * Allows for creating custom type {@link ConfigValue} instances.
   *
   * <p>For example:
   * <pre>{@code
   *   // custom parser for URL values
   *   Parser<URL> urlParser =
   *       value -> {
   *         try {
   *           return new URL(value);
   *         } catch (MalformedURLException e) {
   *           throw new InvalidConfigurationException(e);
   *         }
   *       };
   *   ConfigValue<URL> configuredUrl = Configuration.getValue("config.url", null, urlParser);
   * }</pre>
   *
   * @param configKey configuration file parameter key
   * @param defaultValue default key value
   * @param parser custom parser to support the data type
   * @param <T> data type of the configuration key value
   * @return the custom type object
   */
  public static <T> ConfigValue<T> getValue(String configKey, T defaultValue, Parser<T> parser) {
    ConfigValue<T> toReturn =
        new ConfigValue.Builder<T>()
            .setConfigKey(configKey)
            .setDefaultValue(defaultValue)
            .setParser(parser)
            .build();
    initializeOrRegister(toReturn);
    return toReturn;
  }

  @SuppressWarnings("rawtypes")
  private static void initializeOrRegister(ConfigValue toReturn) {
    checkNotNull(toReturn);
    if (initialized.get()) {
      initializeConfigValue(toReturn);
    } else {
      synchronized (configurations) {
        configurations.add(toReturn);
      }
    }
  }

  @SuppressWarnings("rawtypes")
  private static void initializeConfigValue(ConfigValue value) {
    checkState(loadedConfig != null, "loadedConfig not initialized yet");
    String configuredValue =
        Optional.ofNullable(loadedConfig.getProperty(value.getConfigKey()))
            .map(s -> s.trim())
            .orElse(null);
    value.initialize(configuredValue);
    value.freeze();
  }

  /**
   * Checks for a configuration error.
   *
   * <p>This allows the connector to throw a {@link InvalidConfigurationException} instead of a
   * {@code checkArgument()} method's {@code IllegalArgumentException}. This prevents the SDK start
   * up code from performing retries when a quick exit is appropriate.
   *
   * @param condition the valid condition to test
   * @param errorMessage the thrown exception message when the {@code condition} is not met
   */
  public static void checkConfiguration(boolean condition, String errorMessage) {
    if (!condition) {
      throw new InvalidConfigurationException(errorMessage);
    }
  }

  /**
   * Checks for a configuration error.
   *
   * <p>This allows the connector to throw a {@link InvalidConfigurationException} instead of a
   * {@code checkArgument()} method's {@code IllegalArgumentException}. This prevents the SDK start
   * up code from performing retries when a quick exit is appropriate.
   *
   * @param condition the valid condition to test
   * @param errorFormat the format string for the thrown exception message when the
   *     {@code condition} is not met
   * @param errorArgs the arguments for the {@code errorFormat}
   */
  public static void checkConfiguration(boolean condition, String errorFormat,
      Object... errorArgs) {
    if (!condition) {
      throw new InvalidConfigurationException(String.format(errorFormat, errorArgs));
    }
  }

  /**
   * Parses command line arguments.
   *
   * <p>The sdk implemented args: -Dconfig=[config_file_name] // ability to pass a config file to
   * the framework.
   *
   * @param args list of command line arguments
   * @return all the valid key/value pairs of the command line arguments
   */
  private static Properties parseArgs(String[] args) {
    Properties props = new Properties();
    for (String arg : args) {
      if (arg.startsWith(ARGS_KEY)) {
        String[] parts = arg.substring(ARGS_KEY.length()).split("=", 2);
        if (parts.length == 2) {
          props.setProperty(parts[0].trim(), parts[1].trim());
        }
      }
    }
    return props;
  }

  private static synchronized void resetConfiguration() {
    configurations.clear();
    initialized.set(false);
    loadedConfig = null;
  }

  /**
   * {@link TestRule} to reset static {@link Configuration} object for unit tests.
   *
   * @see SetupConfigRule
   */
  public static class ResetConfigRule implements TestRule {
    @Override
    public Statement apply(Statement base, Description description) {
      resetConfiguration();
      return base;
    }
  }

  /**
   * {@link TestRule} to initialize static {@link Configuration} object for unit tests.
   *
   * <p>For example:
   * <pre>
   * {@code @Rule public ResetConfigRule resetConfig = new ResetConfigRule(); }
   * {@code @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized(); }
   *
   * {@code @Test public void testSomething() {
   *     Properties configProperties = new Properties();
   *     configProperties.put("config.key1", "value1");
   *     setupConfig.initConfig(configProperties);
   *     // do test code using initialized configuration
   *     ...
   *   }
   * }</pre>
   */
  public static class SetupConfigRule implements TestRule {
    private SetupConfigRule() {}
    @Override
    public Statement apply(Statement base, Description description) {
      return base;
    }

    /** Factory method to get an instance of {@link SetupConfigRule}. */
    public static SetupConfigRule uninitialized() {
      return new SetupConfigRule();
    }

    /** Initialize {@link Configuration} based on supplied {@link Properties}. */
    public void initConfig(Properties properties) {
      Set<String> names = properties.stringPropertyNames();
      if (properties.size() != names.size()) {
        throw new IllegalArgumentException("Non-string properties found in config: "
            + Sets.difference(properties.keySet(), names));
      }
      Configuration.initConfig(properties);
    }
  }
}
