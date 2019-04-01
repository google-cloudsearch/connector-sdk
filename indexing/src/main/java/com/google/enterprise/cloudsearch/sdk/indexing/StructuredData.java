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
package com.google.enterprise.cloudsearch.sdk.indexing;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.Charset.defaultCharset;

import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.DateTime;
import com.google.api.services.cloudsearch.v1.model.DateValues;
import com.google.api.services.cloudsearch.v1.model.DoubleValues;
import com.google.api.services.cloudsearch.v1.model.EnumPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.EnumValuePair;
import com.google.api.services.cloudsearch.v1.model.EnumValues;
import com.google.api.services.cloudsearch.v1.model.HtmlValues;
import com.google.api.services.cloudsearch.v1.model.IntegerValues;
import com.google.api.services.cloudsearch.v1.model.NamedProperty;
import com.google.api.services.cloudsearch.v1.model.ObjectDefinition;
import com.google.api.services.cloudsearch.v1.model.ObjectValues;
import com.google.api.services.cloudsearch.v1.model.PropertyDefinition;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.StructuredDataObject;
import com.google.api.services.cloudsearch.v1.model.TextValues;
import com.google.api.services.cloudsearch.v1.model.TimestampValues;
import com.google.common.base.Converter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.enterprise.cloudsearch.sdk.Application;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalQueries;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Helper utility to generate a {@link StructuredDataObject}.
 *
 * <p>Structured data initialization occurs when {@link Application#start()} calls
 * {@link StructuredData#initFromConfiguration(IndexingService)}. Once initialized,
 * {@link IndexingItemBuilder} calls {@link StructuredData#getStructuredData} to generate a
 * {@link StructuredDataObject} for each document.
 *
 * <p> Typically the connector code does not access the structured data objects and methods
 * directly. The {@link IndexingItemBuilder#build()} automates the structure data generation using
 * the connector provided data values when creating a document item:
 *
 * <pre>{@code
 * String itemName = ... // set the unique ID for the item
 * String objectType = ... // set the schema object type (possibly from the config file)
 * // build a map that contains structured data values to pass on to the item builder
 * Multimap<String, Object> multiMapValues = ... // populate item values from your data repository
 * // ... add other item metadata values from your data repository
 *
 * // build the item
 * Item item = IndexingItemBuilder.fromConfiguration(itemName)
 *     .setObjectType(objectType)
 *     .setValues(multiMapValues)
 *     // set other metadata such as setAcl, setItemType, setSourceRepositoryUrl, and so on
 *     .build();
 * }</pre>
 */
public class StructuredData {
  private static final Logger logger = Logger.getLogger(StructuredData.class.getName());

  public static final String DATETIME_PATTERNS = "structuredData.dateTimePatterns";
  public static final String LOCAL_SCHEMA = "structuredData.localSchema";

  public static final String IGNORE_CONVERSION_ERRORS = "structuredData.ignoreConversionErrors";

  private static final String DATETIME_PATTERNS_DELIMITER = ";";
  private static final ImmutableList<DateTimeParser> DEFAULT_DATETIME_PARSERS =
      ImmutableList.of(
          new DateTimeParser(getIsoDateTime('T'), "Internal ISO 8601 date-time with T"),
          new DateTimeParser(getIsoDateTime(' '), "Internal ISO 8601 date-time with space"),
          new DateTimeParser(DateTimeFormatter.RFC_1123_DATE_TIME,
              "DateTimeFormatter.RFC_1123_DATE_TIME"));

  private static final JsonObjectParser JSON_PARSER =
      new JsonObjectParser(JacksonFactory.getDefaultInstance());

  private static final AtomicBoolean initialized = new AtomicBoolean();
  private static final List<DateTimeParser> dateTimeParsers = new ArrayList<>();
  private static final AtomicBoolean ignoreConversionErrors = new AtomicBoolean();

  /** A map from object definition names to instances of this class. */
  private static final Map<String, StructuredData> structuredDataMapping =
      new HashMap<String, StructuredData>();

  /** A map from property definition names to a typed ValueExtractor that builds a NamedProperty. */
  private final ImmutableMap<String, ValueExtractor<?>> conversionMap;

  /**
   * Initializes the {@link StructuredData} object as defined by a configured or default
   * {@link Schema}.
   *
   * <p>Build a map of all the structured data objects to their property definitions. This is used
   * during document indexing to populate the document's data values to the appropriate structured
   * data fields. This method must be executed before any other structured data methods can be
   * called.
   *
   * <p>Only one of {@code initFromConfiguration} or {@link #init} can be called.
   *
   * @param indexingService {@link IndexingService} instance used to get the default schema
   */
  public static synchronized void initFromConfiguration(IndexingService indexingService) {
    checkState(!isInitialized(), "StructuredData already initialized.");

    dateTimeParsers.addAll(DEFAULT_DATETIME_PARSERS);
    List<String> dateTimePatterns =
        Configuration.getMultiValue(
            DATETIME_PATTERNS, Collections.emptyList(), Configuration.STRING_PARSER,
            DATETIME_PATTERNS_DELIMITER)
        .get();
    InvalidConfigurationException patternErrors = null;
    for (String pattern : dateTimePatterns) {
      try {
        dateTimeParsers.add(new DateTimeParser(pattern));
      } catch (IllegalArgumentException e) {
        InvalidConfigurationException ice = new InvalidConfigurationException(
            "Invalid date-time pattern \"" + pattern + "\": " + e.getMessage());
        if (patternErrors == null) {
          patternErrors = ice;
        } else {
          patternErrors.addSuppressed(ice);
        }
      }
    }
    if (patternErrors != null) {
      throw patternErrors;
    }

    ignoreConversionErrors.set(Configuration.getBoolean(IGNORE_CONVERSION_ERRORS, Boolean.FALSE).get());

    Schema schema;
    String localSchemaPath = Configuration.getString(LOCAL_SCHEMA, "").get();
    if (!localSchemaPath.isEmpty()) {
      Path localSchema = Paths.get(localSchemaPath);
      if (!Files.exists(localSchema)) {
        throw new InvalidConfigurationException(
            "Local schema file " + localSchemaPath + " does not exist");
      }

      try {
        InputStreamReader inputStreamReader = new InputStreamReader(
            localSchema.toUri().toURL().openStream(), defaultCharset());
        schema = JSON_PARSER.parseAndClose(inputStreamReader, Schema.class);
      } catch (IOException e) {
        throw new StartupException("Failed to parse local schema file " + localSchemaPath, e);
      }
    } else {
      try {
        schema = indexingService.getSchema();
      } catch (IOException e) {
        throw new StartupException("Failed to initialize StructuredData", e);
      }
    }
    init(schema);
  }

  /**
   * Initializes the {@link StructuredData} object as defined by the {@link Schema}.
   *
   * <p>Build a map of all the structured data objects to their property definitions. This is used
   * during document indexing to populate the document's data values to the appropriate structured
   * data fields. This method must be executed before any other structured data methods can be
   * called.
   *
   * <p>Only one of {@link #initFromConfiguration} or {@code init} can be called.
   *
   * @param schema the schema defined in the data source
   */
  // TODO(jlacey): Should we deprecate this method, in favor of initFromConfiguration?
  public static synchronized void init(Schema schema) {
    checkState(!isInitialized(), "StructuredData already initialized.");
    checkNotNull(schema, "schema cannot be null");

    // Handle a direct call to init, rather than initFromConfiguration.
    if (dateTimeParsers.isEmpty()) {
      dateTimeParsers.addAll(DEFAULT_DATETIME_PARSERS);
    }

    List<ObjectDefinition> objectDefinitions =
        schema.getObjectDefinitions() == null
            ? Collections.emptyList()
            : schema.getObjectDefinitions();
    Map<String, StructuredData> mappings =
        objectDefinitions
            .stream()
            .collect(
                Collectors.toMap(
                    objDefinition -> objDefinition.getName(),
                    objDefinition -> new StructuredData(objDefinition)));
    structuredDataMapping.putAll(mappings);
    initialized.set(true);
  }

  /**
   * Determines whether the {@link StructuredData} object has been initialized.
   *
   * <p>The method {@link #init(Schema)} must be called first to initialize structured data.
   *
   * @return {@code true} if the {@link StructuredData} object has been initialized
   */
  public static boolean isInitialized() {
    return initialized.get();
  }

  /**
   * Returns true if object definition is available in {@code Schema} for specified objectType.
   * False if if object definition is not available in {@code Schema}.
   *
   * @param objectType to check if object definition is available in schema.
   * @return true if object definition is available. False otherwise.
   * @throws {@link IllegalStateException} if structured data object is not initialized yet.
   */
  public static boolean hasObjectDefinition(String objectType) {
    checkState(isInitialized(), "StructuredData not initialized");
    return structuredDataMapping.containsKey(objectType);
  }

  /**
   * Generate a {@link StructuredDataObject} for the given object type using the input values.
   *
   * <p>The object type must be present in the current schema. The structured data properties are
   * populated with the matching named values. {@link #init(Schema)} must be executed before calling
   * this method to initialize the structure data mappings.
   *
   * @param objectType object type name as defined in {@link ObjectDefinition#getName()}
   * @param values {@link Multimap} of values to generate the {@link StructuredDataObject}
   * @return {@link StructuredDataObject} structured data
   */
  public static StructuredDataObject getStructuredData(
      String objectType, Multimap<String, Object> values) {
    return getInstance(objectType).getStructuredData(values);
  }

  private static StructuredData getInstance(String objectType) {
    checkState(isInitialized(), "StructuredData not initialized");
    StructuredData structuredData = structuredDataMapping.get(objectType);
    checkArgument(structuredData != null, "invalid object type " + objectType);
    return structuredData;
  }

  private StructuredData(ObjectDefinition objectDefinition) {
    checkNotNull(objectDefinition, "objectDefinition cannot be null");
    checkNotNull(objectDefinition.getPropertyDefinitions(), "property definitions cannot be null");
    conversionMap =
        objectDefinition.getPropertyDefinitions()
            .stream()
            .collect(
                ImmutableMap.toImmutableMap(
                    p -> p.getName(), p -> checkNotNull(getValueExtractor(p))));
  }

  private StructuredDataObject getStructuredData(Multimap<String, Object> values) {
    checkNotNull(values, "values cannot be null");
    List<NamedProperty> properties =
        conversionMap.entrySet()
            .stream()
            .map(entry -> entry.getValue().getProperty(entry.getKey(), values.get(entry.getKey())))
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    return new StructuredDataObject().setProperties(properties);
  }

  private static synchronized void reset() {
    dateTimeParsers.clear();
    structuredDataMapping.clear();
    initialized.set(false);
  }

  /**
   * {@link TestRule} to reset the static {@link StructuredData} object for unit tests.
   */
  public static class ResetStructuredDataRule implements TestRule {
    @Override
    public Statement apply(Statement base, Description description) {
      reset();
      return base;
    }
  }

  private interface NamedPropertyBuilder<T> {
    NamedProperty getNamedProperty(String propertyName, List<T> values);
  }

  /**
   * Wrapper object to convert {@link Object} values to supported property type. TODO(tvartak):
   * Expose ValueExtractor to support custom conversions.
   */
  private static class ValueExtractor<T> {
    private final Converter<Object, T> valueConverter;
    private final NamedPropertyBuilder<T> propertyBuilder;
    private final boolean isRepeated;

    private ValueExtractor(
        Converter<Object, T> valueConverter,
        NamedPropertyBuilder<T> propertyBuilder,
        boolean isRepeated) {
      this.valueConverter = valueConverter;
      this.propertyBuilder = propertyBuilder;
      this.isRepeated = isRepeated;
    }

    private NamedProperty getProperty(String propertyName, Collection<Object> values) {
      List<Object> nonNullValues =
              values.stream().filter(Objects::nonNull).collect(Collectors.toList());
      if (nonNullValues.isEmpty()) {
        return null;
      }
      if (!isRepeated) {
        try {
          return propertyBuilder.getNamedProperty(
                  propertyName, Collections.singletonList(valueConverter.convert(nonNullValues.get(0))));
        } catch (IllegalArgumentException e) {
          if (ignoreConversionErrors.get()) {
            logger.log(Level.FINEST, "Ignoring conversion error: {0}", e.getMessage());
            return null;
          }
          throw e;
        }
      } else {
        List<T> nonNullConvertedValues = nonNullValues
                .stream()
                .map(v -> convert(valueConverter, v))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if(nonNullConvertedValues.isEmpty()) {
          return null;
        }
        return propertyBuilder.getNamedProperty(
                propertyName,
                nonNullConvertedValues);
      }
    }

    private T convert(Converter<Object, T> converter, Object v) {
      try {
        return converter.convert(v);
      } catch (IllegalArgumentException e) {
        if(ignoreConversionErrors.get()) {
          logger.log(Level.FINEST, "Ignoring conversion error: {0}", e.getMessage());
          return null;
        }
        throw e;
      }
    }
  }

  private static ValueExtractor<?> getValueExtractor(PropertyDefinition definition) {
    checkNotNull(definition, "property definition cannot be null");
    if (definition.getBooleanPropertyOptions() != null) {
      return new ValueExtractor<>(BOOLEAN_CONVERTER, BOOLEAN_PROPERTY_BUILDER, false);
    }

    boolean isRepeatable = Optional.ofNullable(definition.getIsRepeatable()).orElse(false);
    if (definition.getTextPropertyOptions() != null) {
      return new ValueExtractor<>(
          STRING_CONVERTER, TEXT_PROPERTY_BUILDER, isRepeatable);
    }
    if (definition.getHtmlPropertyOptions() != null) {
      return new ValueExtractor<>(
          STRING_CONVERTER, HTML_PROPERTY_BUILDER, isRepeatable);
    }
    if (definition.getTimestampPropertyOptions() != null) {
      return new ValueExtractor<>(
          DATETIME_CONVERTER, DATETIME_PROPERTY_BUILDER, isRepeatable);
    }
    if (definition.getDoublePropertyOptions() != null) {
      return new ValueExtractor<>(
          DOUBLE_CONVERTER, DOUBLE_PROPERTY_BUILDER, isRepeatable);
    }
    if (definition.getIntegerPropertyOptions() != null) {
      return new ValueExtractor<>(
          LONG_CONVERTER, INTEGER_PROPERTY_BUILDER, isRepeatable);
    }
    if (definition.getObjectPropertyOptions() != null) {
      ObjectDefinition subObject =
          new ObjectDefinition()
              .setName(definition.getName())
              .setPropertyDefinitions(
                  definition.getObjectPropertyOptions().getSubobjectProperties());
      return new ValueExtractor<>(
          new ObjectConverter(new StructuredData(subObject)),
          OBJECT_PROPERTY_BUILDER,
          isRepeatable);
    }
    if (definition.getDatePropertyOptions() != null) {
      return new ValueExtractor<>(
          DATE_CONVERTER, DATE_PROPERTY_BUILDER, isRepeatable);
    }
    if (definition.getEnumPropertyOptions() != null) {
      return new ValueExtractor<>(
          new EnumConverter(definition.getEnumPropertyOptions()),
          ENUM_PROPERTY_BUILDER,
          isRepeatable);
    }
    throw new IllegalArgumentException("Unknown type: " + definition);
  }

  static final Converter<Object, Boolean> BOOLEAN_CONVERTER =
      new Converter<Object, Boolean>() {

        @Override
        protected Boolean doForward(Object a) {
          if (a instanceof Boolean) {
            return (Boolean) a;
          } else if (a instanceof String) {
            return Boolean.parseBoolean((String) a);
          } else {
            throw new IllegalArgumentException("unable to convert value " + a);
          }
        }

        @Override
        protected Object doBackward(Boolean b) {
          return b;
        }
      };

  static final Converter<Object, String> STRING_CONVERTER =
      new Converter<Object, String>() {

        @Override
        protected String doForward(Object a) {
          return Objects.toString(a);
        }

        @Override
        protected Object doBackward(String b) {
          return b;
        }
      };

  static final Converter<Object, Long> LONG_CONVERTER =
      new Converter<Object, Long>() {

        @Override
        protected Long doForward(Object a) {
          if (a instanceof Number) {
            return ((Number) a).longValue();
          }
          try {
            return Long.valueOf(Objects.toString(a));
          } catch (NumberFormatException e) {
            return Double.valueOf(Objects.toString(a)).longValue();
          }
        }

        @Override
        protected Object doBackward(Long b) {
          return b;
        }
      };

  static final Converter<Object, Double> DOUBLE_CONVERTER =
      new Converter<Object, Double>() {

        @Override
        protected Double doForward(Object a) {
          if (a instanceof Number) {
            return ((Number) a).doubleValue();
          }
          return Double.valueOf(Objects.toString(a));
        }

        @Override
        protected Object doBackward(Double b) {
          return b;
        }
      };

  // TODO(jlacey): Support java.time classes as inputs, here and in DATE_CONVERTER.
  static final Converter<Object, DateTime> DATETIME_CONVERTER =
      new Converter<Object, DateTime>() {

        @Override
        protected DateTime doForward(Object a) {
          if (a instanceof DateTime) {
            return (DateTime) a;
          }
          if (a instanceof Long) {
            return new DateTime((Long) a);
          }
          if (a instanceof String) {
            return toDateTime(parseDateTime((String) a));
          }
          if (a instanceof Date) {
            return new DateTime((Date) a);
          }
          throw new NumberFormatException("Cannot convert \"" + a + "\" to DateTime");
        }

        @Override
        protected Object doBackward(DateTime b) {
          return b;
        }
      };

  static final Converter<Object, com.google.api.services.cloudsearch.v1.model.Date> DATE_CONVERTER =
      new Converter<Object, com.google.api.services.cloudsearch.v1.model.Date>() {

        @Override
        protected com.google.api.services.cloudsearch.v1.model.Date doForward(Object a) {
          if (a instanceof com.google.api.services.cloudsearch.v1.model.Date) {
            return (com.google.api.services.cloudsearch.v1.model.Date) a;
          } else if (a instanceof Date) {
            return getApiDate((Date) a);
          } else if (a instanceof Long) {
            return getApiDate(new Date((long) a));
          } else if (a instanceof DateTime) {
            Date input = new Date(((DateTime) a).getValue());
            return getApiDate(input);
          } else if (a instanceof String) {
            return getApiDate(parseDateTime((String) a));
          }
          throw new NumberFormatException("Cannot convert \"" + a + "\" to Date");
        }

        @Override
        protected Object doBackward(com.google.api.services.cloudsearch.v1.model.Date b) {
          return b;
        }
      };

  private static class ObjectConverter extends Converter<Object, StructuredDataObject> {

    private final StructuredData structuredData;

    private ObjectConverter(StructuredData structuredData) {
      this.structuredData = structuredData;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected StructuredDataObject doForward(Object a) {
      checkArgument((a instanceof Multimap<?, ?>), "invalid object to generate structured data");
      return structuredData.getStructuredData((Multimap<String, Object>) a);
    }

    @Override
    protected Object doBackward(StructuredDataObject b) {
      return b;
    }
  }

  private static class EnumConverter extends Converter<Object, String> {
    final Set<String> possibleValues;
    final Map<Integer, String> valueToNameMap;

    private EnumConverter(EnumPropertyOptions enumPropertyOptions) {
      checkNotNull(enumPropertyOptions, "enum property options cannot be null");
      List<EnumValuePair> enums =
          checkNotNull(
              enumPropertyOptions.getPossibleValues(), "enum possible values cannot be null");

      Set<String> possibleValues = new HashSet<>();
      Map<Integer, String> valueToNameMap = new HashMap<>();
      for (EnumValuePair pair : enums) {
        String enumString = pair.getStringValue();
        checkArgument(!Strings.isNullOrEmpty(enumString));
        possibleValues.add(enumString);
        if (pair.getIntegerValue() != null) {
          valueToNameMap.put(pair.getIntegerValue(), enumString);
        }
      }
      this.possibleValues = Collections.unmodifiableSet(possibleValues);
      this.valueToNameMap = Collections.unmodifiableMap(valueToNameMap);
    }

    @Override
    protected String doForward(Object a) {
      if (a instanceof Enum<?>) {
        String valName = ((Enum<?>) a).name();
        if (possibleValues.contains(valName)) {
          return valName;
        }

        String val = ((Enum<?>) a).toString();
        if (possibleValues.contains(val)) {
          return val;
        }
        throw new IllegalArgumentException("Unknown enum: " + valName);
      } else if (a instanceof String) {
        String val = (String) a;
        if (possibleValues.contains(val)) {
          return val;
        } else {
          try {
            Integer v = Integer.parseInt(val);
            if (valueToNameMap.containsKey(v)) {
              return valueToNameMap.get(v);
            }
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Unknown enum value: " + val);
          }
        }
        throw new IllegalArgumentException("Unknown enum value: " + val);
      } else if (a instanceof Integer) {
        if (valueToNameMap.containsKey(a)) {
          return valueToNameMap.get(a);
        }
        throw new IllegalArgumentException("Unknown enum int value: " + a);
      }
      throw new IllegalArgumentException("Unknown enum object: " + a);
    }

    @Override
    protected Object doBackward(String b) {
      return b;
    }
  }

  /**
   * Parses a {@code ZonedDateTime} object from a string input. Loops over the built-in and
   * configured date-time formats.
   *
   * @param input a date-time string, not null
   * @return a {@code ZonedDateTime} object
   * @throws NumberFormatException if parsing fails
   */
  private static ZonedDateTime parseDateTime(String input) {
    checkState(isInitialized(), "StructuredData not initialized");
    for (DateTimeParser parser : dateTimeParsers) {
      try {
        TemporalAccessor accessor = parser.formatter.parse(input);
        LocalDate localDate = LocalDate.from(accessor);

        // Check for a time zone or use the default zone.
        ZoneId zoneId = accessor.query(TemporalQueries.zone());
        String tzMessage;
        if (zoneId == null) {
          zoneId = ZoneId.systemDefault();
          tzMessage = "time zone";
        } else {
          tzMessage = "no time zone";
        }

        // Check for a time of day or use the earliest time.
        LocalTime localTime = accessor.query(TemporalQueries.localTime());
        if (localTime == null) {
          logger.log(Level.FINEST, "Input string {0} matched {1} as date-only.",
              new Object[] { input, parser.name });
          return localDate.atStartOfDay(zoneId);
        } else {
          logger.log(Level.FINEST, "Input string {0} matched {1} with {2}.",
              new Object[] { input, parser.name, tzMessage });
          return localDate.atTime(localTime).atZone(zoneId);
        }
      } catch (DateTimeParseException e) {
        logger.log(Level.FINEST, "{0} with {1}", new Object[] { e, parser.name });
      }
    }
    throw new NumberFormatException("Cannot convert \"" + input + "\" to DateTime");
  }

  /**
   * Holds a DateTimeFormatter with an associated name for logging.
   */
  private static class DateTimeParser {
    final DateTimeFormatter formatter;
    final String name;

    /**
     * Constructs a DateTimeFormatter from a pattern string, using the
     * pattern itself as the basis for the loggable name.
     */
    DateTimeParser(String pattern) {
      formatter = new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendPattern(pattern)
          .toFormatter();
      name = "Pattern \"" + pattern + "\"";
    }

    /** Uses the given formatter, and the given name for logging. */
    DateTimeParser(DateTimeFormatter formatter, String name) {
      this.formatter = formatter;
      this.name = name;
    }
  }

  /*
   * Produces formatters similar to {@link DateTimeFormatter#ISO_DATE_TIME}, except
   * that the time element and its preceding delimiter are optional, time zone offsets may
   * have only hours (for example, "-07"), with Java 9 or later, and the delimiter between
   * the date and time portion the string is specified by the given delimiter. In practice,
   * only 'T' and ' ' (space) are used.
   *
   * The lenient offset was applied to {@link DateTimeFormatter#ISO_OFFSET_DATE_TIME} in JDK 9,
   * but {@link DateTimeFormatter#ISO_DATE_TIME} was missed. By creating a custom formatter
   * with an optional time element, we can avoid adding {@link DateTimeFormatter#ISO_DATE}.
   *
   * @param delimiter the separator character between the date and time portions of the string
   * @see "https://bugs.openjdk.java.net/browse/JDK-8032051"
   */
  private static DateTimeFormatter getIsoDateTime(char delimiter) {
    return new DateTimeFormatterBuilder()
        .parseCaseInsensitive()
        .append(DateTimeFormatter.ISO_LOCAL_DATE)
        .optionalStart()
        .appendLiteral(delimiter)
        .append(DateTimeFormatter.ISO_LOCAL_TIME)
        .optionalEnd()
        .optionalStart()
        .parseLenient()
        .appendOffsetId()
        .parseStrict()
        .optionalStart()
        .appendLiteral('[')
        .parseCaseSensitive()
        .appendZoneRegionId()
        .appendLiteral(']')
        .parseStrict()
        .toFormatter()
        .withResolverStyle(ResolverStyle.STRICT)
        .withChronology(IsoChronology.INSTANCE);
  }

  private static DateTime toDateTime(ZonedDateTime zonedDateTime) {
    return new DateTime(
        Date.from(zonedDateTime.toInstant()),
        TimeZone.getTimeZone(zonedDateTime.getZone()));
  }

  private static com.google.api.services.cloudsearch.v1.model.Date getApiDate(Date date) {
    Calendar cal = Calendar.getInstance();
    cal.setTime(date);
    return new com.google.api.services.cloudsearch.v1.model.Date()
        .setDay(cal.get(Calendar.DATE))
        /* Calendar.MONTH starts with 0 where as API Date.MONTH starts with 1 */
        .setMonth(cal.get(Calendar.MONTH) + 1)
        .setYear(cal.get(Calendar.YEAR));
  }

  private static com.google.api.services.cloudsearch.v1.model.Date getApiDate(
      ZonedDateTime dateTime) {
    return new com.google.api.services.cloudsearch.v1.model.Date()
        .setDay(dateTime.getDayOfMonth())
        .setMonth(dateTime.getMonthValue())
        .setYear(dateTime.getYear());
  }

  private static final NamedPropertyBuilder<String> TEXT_PROPERTY_BUILDER =
      (propertyName, values) ->
          new NamedProperty()
              .setName(propertyName)
              .setTextValues(new TextValues().setValues(Collections.unmodifiableList(values)));

  private static final NamedPropertyBuilder<String> HTML_PROPERTY_BUILDER =
      (propertyName, values) ->
          new NamedProperty()
              .setName(propertyName)
              .setHtmlValues(new HtmlValues().setValues(Collections.unmodifiableList(values)));

  private static final NamedPropertyBuilder<String> ENUM_PROPERTY_BUILDER =
      (propertyName, values) ->
          new NamedProperty()
              .setName(propertyName)
              .setEnumValues(new EnumValues().setValues(Collections.unmodifiableList(values)));

  private static final NamedPropertyBuilder<Long> INTEGER_PROPERTY_BUILDER =
      (propertyName, values) ->
          new NamedProperty()
              .setName(propertyName)
              .setIntegerValues(
                  new IntegerValues().setValues(Collections.unmodifiableList(values)));

  private static final NamedPropertyBuilder<Boolean> BOOLEAN_PROPERTY_BUILDER =
      (propertyName, values) -> {
        NamedProperty booleanProperty = new NamedProperty().setName(propertyName);
        if (!values.isEmpty()) {
          booleanProperty.setBooleanValue(values.get(0));
        }
        return booleanProperty;
      };

  private static final NamedPropertyBuilder<DateTime> DATETIME_PROPERTY_BUILDER =
      (propertyName, values) ->
          new NamedProperty()
              .setName(propertyName)
              .setTimestampValues(
                  new TimestampValues()
                      .setValues(
                          Collections.unmodifiableList(
                              values
                                  .stream()
                                  .map(v -> v.toStringRfc3339())
                                  .collect(Collectors.toList()))));

  private static final NamedPropertyBuilder<Double> DOUBLE_PROPERTY_BUILDER =
      (propertyName, values) ->
          new NamedProperty()
              .setName(propertyName)
              .setDoubleValues(new DoubleValues().setValues(Collections.unmodifiableList(values)));

  private static final NamedPropertyBuilder<com.google.api.services.cloudsearch.v1.model.Date>
      DATE_PROPERTY_BUILDER =
          (propertyName, values) ->
              new NamedProperty()
                  .setName(propertyName)
                  .setDateValues(new DateValues().setValues(Collections.unmodifiableList(values)));

  private static final NamedPropertyBuilder<StructuredDataObject> OBJECT_PROPERTY_BUILDER =
      (propertyName, values) ->
          new NamedProperty()
              .setName(propertyName)
              .setObjectValues(new ObjectValues().setValues(Collections.unmodifiableList(values)));
}
