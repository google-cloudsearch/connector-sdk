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
package com.google.enterprise.cloudsearch.csvconnector;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.enterprise.cloudsearch.sdk.config.Configuration.checkConfiguration;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.Comparator.comparing;

import com.google.api.client.http.ByteArrayContent;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.enterprise.cloudsearch.sdk.CloseableIterable;
import com.google.enterprise.cloudsearch.sdk.CloseableIterableOnce;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentTemplate;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.FieldOrValue;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingItemBuilder.ItemType;
import com.google.enterprise.cloudsearch.sdk.indexing.UrlBuilder;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/**
 * Manager of configuration file column definitions and csv file.
 *
 * <p>Manages csv file reading, unique ID creation, item and content creation based on csv records.
 *
 * <p>Required configuration parameters:
 *
 * <ul>
 *   <li>{@value #FILEPATH} - Specifies the csv file path.
 * </ul>
 *
 * <p>Optional configuration parameters:
 *
 * <ul>
 *   <li>{@value #UNIQUE_KEY_COLUMNS} - Specifies the csv column names whose values will be used to
 *       generate each record's unique ID. If not specified, the hashcode of the csv record will be
 *       used as its unique key.
 *   <li>{@value #SKIP_HEADER} - Specifies whether to ignore the first (header) line of the csv
 *       file.
 *   <li>{@value #CSVCOLUMNS} - Specifies all the csv column names used in the csv file. This will
 *       be used if there is no header information in the csv file and {@value #SKIP_HEADER} is set
 *       to false, or to redefine the column names in the csv file.
 *   <li>{@value #MULTIVALUE_COLUMNS} - Specifies all of the column names in the csv file that have
 *       multiple values.
 *   <li>{@value #MULTIVALUE_FORMAT_COLUMN} - Specifies the delimiter used for each multivalue field
 *       defined in {@value #MULTIVALUE_COLUMNS}. The default delimiter is a comma.
 *   <li>{@value #FILE_ENCODING} - Specifies the character encoding of the file. The
 *       default value is the default character encoding of the system where the connector
 *       is running. Refer to
 *       {@link "https://docs.oracle.com/javase/8/docs/api/java/nio/charset/Charset.html"}
 *       for details about supported charsets.
 * </ul>
 *
 * <p>Additional configuration parameters:
 *
 * <ul>
 *   <li>{@link UrlBuilder#fromConfiguration} - Specifies the configuration for the record's
 *       view URL.
 *   <li>{@link IndexingItemBuilder#fromConfiguration} - Specifies the configuration for the
 *       record's {@code ItemMetadata} and structured data.
 *   <li>{@code structuredData.dateTimePatterns} - Specifies a semi-colon separated list of
 *       date time patterns used to try parsing strings for any {@code Date} or {@code DateTime}
 *       fields in {@code ItemMetadata} or the configured object definition. Refer to
 *       {@link "https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html"}
 *       for details about date time patterns.
 * </ul>
 */
class CSVFileManager {

  @VisibleForTesting
  static final String FILEPATH = "csv.filePath";
  static final String FILE_ENCODING = "csv.fileEncoding";
  static final String UNIQUE_KEY_COLUMNS = "csv.uniqueKeyColumns";
  static final String SKIP_HEADER = "csv.skipHeaderRecord";
  static final String CSVCOLUMNS = "csv.csvColumns";
  static final String MULTIVALUE_COLUMNS = "csv.multiValueColumns";
  static final String MULTIVALUE_FORMAT_COLUMN = "csv.multiValue.%s";
  static final String CSV_FORMAT = "csv.format";
  static final String CSV_FORMAT_METHOD_VALUE = "csv.format.%s";

  private final CSVFormat csvFormat;
  private final Path csvFilePath;
  private final Charset fileCharset;
  private final ContentTemplate contentTemplate;
  private final LinkedHashSet<String> uniqueKeyColumns;
  private final Map<String, String> columnsToDelimiter;
  private final UrlBuilder urlBuilder;

  private static final Logger logger = Logger.getLogger(CSVFileManager.class.getName());


  /**
   * Creates an instance of CSVFileManager based on the configuration properties. FILEPATH and
   * URL_COLUMNS are required configuration properties which can not be null. The rest are optional.
   *
   * @return {@link CSVFileManager}
   */
  public static CSVFileManager fromConfiguration() {
    checkState(Configuration.isInitialized(), "configuration not initialized");

    // csv file
    String filePath = Configuration.getString(FILEPATH, null).get();
    checkConfiguration(!filePath.isEmpty(), FILEPATH + " is required");
    Path csvFilePath = Paths.get(filePath);
    checkConfiguration(Files.exists(csvFilePath),
        "File does not exist: " + FILEPATH + "=" + filePath);

    // Charset for file
    Charset fileCharset = Configuration.getValue(FILE_ENCODING, defaultCharset(),
        value -> {
          try {
            return Charset.forName(value);
          } catch (IllegalCharsetNameException | UnsupportedCharsetException e) {
            throw new InvalidConfigurationException(
                "Invalid charset: " + FILE_ENCODING + "=" + value);
          }
        }
      ).get();

    Boolean skipHeader = Configuration.getBoolean(SKIP_HEADER, false).get();

    // Column names. If not set, the first row of the file is expected to contain column names.
    List<String> csvColumns =
        Configuration.getMultiValue(
            CSVCOLUMNS, Collections.emptyList(), Configuration.STRING_PARSER).get();

    // csv file format (Excel, etc.)
    String csvFormatName = Configuration.getString(CSV_FORMAT, "").get();
    CSVFormat csvFormat = createCsvFormat(csvFormatName, csvColumns, skipHeader);

    // The CSVFormat will contain the csvColumns, if set, otherwise read the header line
    // from the file.
    Set<String> headers;
    try (CSVParser parser = CSVParser.parse(csvFilePath.toFile(), fileCharset, csvFormat)) {
      headers = parser.getHeaderMap().keySet();
    } catch (IOException e) {
      throw new InvalidConfigurationException("Error reading " + csvFilePath, e);
    }

    // Columns to use to create the unique key, optional.
    // Verify that configured names are known column names.
    List<String> uniqueKeyColumns =
        Configuration.getMultiValue(
            UNIQUE_KEY_COLUMNS, Collections.emptyList(), Configuration.STRING_PARSER).get();
    verifyColumns(UNIQUE_KEY_COLUMNS, new HashSet<>(uniqueKeyColumns), "column headers", headers);

    // Verify that the columns expected for constructing the record URL are known.
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    Set<String> missing = urlBuilder.getMissingColumns(headers);
    checkConfiguration(missing.isEmpty(),
        getMissingMessage(UrlBuilder.CONFIG_COLUMNS, missing, "column headers", headers));

    // Verify that any configured multi-value columns are known.
    List<String> multiValueColumns =
        Configuration.getMultiValue(
            MULTIVALUE_COLUMNS, Collections.emptyList(), Configuration.STRING_PARSER).get();
    verifyColumns(MULTIVALUE_COLUMNS, new HashSet<>(multiValueColumns), "column headers", headers);

    // Check any config properties of the form csv.multiValue.<columnName> and verify
    // the column name(s).
    Set<String> multiValueColumnsWithConfiguredSeparators = Configuration.getConfig()
        .keySet()
        .stream()
        .map(key -> key.toString())
        .filter(key -> key.startsWith("csv.multiValue."))
        .map(key -> key.substring("csv.multiValue.".length()))
        .collect(Collectors.toSet());
    checkConfiguration(
        multiValueColumns.size() > 0 || multiValueColumnsWithConfiguredSeparators.size() == 0,
        "Multi-value separators are configured but no multi-value columns are configured");
    verifyColumns("csv.multiValue.*",
        multiValueColumnsWithConfiguredSeparators, MULTIVALUE_COLUMNS,
        new HashSet<>(multiValueColumns));

    // Build a map from multi-value column name to delimiter.
    Map<String, String> columnsToDelimiter = new HashMap<>();
    for (String column : multiValueColumns) {
      String delimiter = Configuration
          .getString(String.format(MULTIVALUE_FORMAT_COLUMN, column), ",").get();
      columnsToDelimiter.put(column, delimiter);
    }

    return new Builder()
        .setFilePath(csvFilePath)
        .setFileCharset(fileCharset)
        .setUniqueKeyColumns(uniqueKeyColumns)
        .setCsvFormat(csvFormat)
        .setContentTemplate(ContentTemplate.fromConfiguration("csv"))
        .setColumnsToDelimiter(columnsToDelimiter)
        .setUrlBuilder(urlBuilder)
        .verify()
        .build();
  }

  /**
   * Reads the csv file.
   *
   * @return a {@link CloseableIterable} to read the records from
   * @throws IOException If an I/O error occurs when parsing the csv file
   */
  public CloseableIterable<CSVRecord> getCSVFile() throws IOException {
    CSVParser csvParser = csvFormat.parse(getReader());
    return new CSVFile(csvParser);
  }

  /**
   * Creates an {@link Item} for a csvRecord.
   *
   * @param csvRecord a particular row in the csv file
   * @return {@link Item}
   */
  public Item createItem(CSVRecord csvRecord) {
    return IndexingItemBuilder.fromConfiguration(getUniqueId(csvRecord))
        .setValues(generateMultiMap(csvRecord))
        .setSourceRepositoryUrl(FieldOrValue.withValue(getViewUrl(csvRecord)))
        .setItemType(ItemType.CONTENT_ITEM)
        .build();
  }

  /**
   * Creates {@link ByteArrayContent} for a csvRecord based on {@link ContentTemplate}
   *
   * @param csvRecord a particular row in the CSV file
   * @return {@link ByteArrayContent}
   */
  public ByteArrayContent createContent(CSVRecord csvRecord) {
    String htmlContent = contentTemplate.apply(generateMultiMap(csvRecord));
    return ByteArrayContent.fromString("text/html", htmlContent);
  }

  /**
   * Generates a multimap for each column name and corresponding values in the
   * csvRecord. Splits the multivalued field using the configured delimiter or the default
   * delimiter ','.
   *
   * @param csvRecord csvRecord
   * @return a multimap for csv column names and values in csvRecord
   */
  @VisibleForTesting
  Multimap<String, Object> generateMultiMap(CSVRecord csvRecord) {
    Multimap<String, Object> multimap = ArrayListMultimap.create();
    for (Map.Entry<String, String> entry : csvRecord.toMap().entrySet()) {
      List<String> values = new ArrayList<>();
      if (columnsToDelimiter.containsKey(entry.getKey())) {
        values = Splitter.on(columnsToDelimiter.get(entry.getKey())).trimResults()
            .omitEmptyStrings()
          .splitToList(entry.getValue());
      } else {
        String value = entry.getValue().trim();
        if (!value.isEmpty()) {
          values.add(entry.getValue());
        }
      }
      multimap.putAll(entry.getKey(), values);
    }
    return multimap;
  }

  /**
   * Constructs a unique ID based on uniqueKeyColumns. If uniqueKeyColumns is empty, uses a
   * hash of the whole CSVRecord.
   *
   * @param csvRecord a csv record
   * @return uniqueId
   */
  private String getUniqueId(CSVRecord csvRecord) {
    if (uniqueKeyColumns.isEmpty()) {
      // Create a consistent fingerprint of the record values.
      Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
      for (String value : csvRecord) {
        hasher.putUnencodedChars(value);
      }
      return hasher.hash().toString();
    } else {
      List<String> values = new ArrayList<>();
      for (String column : uniqueKeyColumns) {
        values.add(csvRecord.get(column));
      }
      return Joiner.on("||").useForNull("?").join(values);
    }
  }

  private String getViewUrl(CSVRecord record) {
    return urlBuilder.buildUrl(record.toMap());
  }

  private CSVFileManager(Builder builder) {
    this.contentTemplate = builder.contentTemplate;
    this.csvFilePath = builder.csvFilePath;
    this.fileCharset = builder.fileCharset;
    this.columnsToDelimiter = builder.columnsToDelimiter;
    this.uniqueKeyColumns = new LinkedHashSet<>(builder.uniqueKeyColumns);
    this.urlBuilder = builder.urlBuilder;
    this.csvFormat = builder.csvFormat;
  }

  static class Builder {
    private Path csvFilePath;
    private Charset fileCharset;
    private List<String> uniqueKeyColumns;
    private ContentTemplate contentTemplate;
    private Map<String, String> columnsToDelimiter;
    private UrlBuilder urlBuilder;
    private CSVFormat csvFormat;

    Builder() {}

    Builder setFilePath(Path csvFilePath) {
      this.csvFilePath = csvFilePath;
      return this;
    }

    Builder setFileCharset(Charset charset) {
      this.fileCharset = charset;
      return this;
    }

    Builder setUniqueKeyColumns(List<String> uniqueKeyColumns) {
      this.uniqueKeyColumns = uniqueKeyColumns;
      return this;
    }

    Builder setCsvFormat(CSVFormat csvFormat) {
      this.csvFormat = csvFormat;
      return this;
    }

    Builder setContentTemplate(ContentTemplate contentTemplate) {
      this.contentTemplate = contentTemplate;
      return this;
    }

    Builder setColumnsToDelimiter(Map<String, String> columnsToDelimiter) {
      this.columnsToDelimiter = columnsToDelimiter;
      return this;
    }

    Builder setUrlBuilder(UrlBuilder urlBuilder) {
      this.urlBuilder = urlBuilder;
      return this;
    }

    Builder verify() {
      checkNotNull(csvFilePath);
      checkNotNull(fileCharset);
      checkNotNull(uniqueKeyColumns);
      checkNotNull(contentTemplate);
      checkNotNull(columnsToDelimiter);
      checkNotNull(urlBuilder);
      checkNotNull(csvFormat);
      return this;
    }

    CSVFileManager build() {
      return new CSVFileManager(this);
    }
  }

  /**
   * Gets a buffered reader using a larger buffer size than the default in the CSV parser.
   */
  private Reader getReader() throws IOException {
    URI csvUri = csvFilePath.toUri();
    return new BufferedReader(
        new InputStreamReader(csvUri.toURL().openStream(), fileCharset), 16 * 1024 * 1024);
  }

  private static CSVFormat createCsvFormat(String csvFormatName, List<String> csvColumns,
    boolean skipHeader) {
    CSVFormat csvFormat = null;
    if (csvFormatName.isEmpty()) {
      csvFormat = CSVFormat.DEFAULT.withIgnoreSurroundingSpaces();
    } else {
      Set<CSVFormat.Predefined> csvFormats = getPredefinedCsvFormats();
      for (CSVFormat.Predefined format : csvFormats) {
        if (format.toString().equalsIgnoreCase(csvFormatName)) {
          csvFormat = format.getFormat();
          break;
        }
      }
      if (csvFormat == null) {
        throw new InvalidConfigurationException(
            "Invalid CSVFormat " + csvFormatName + ", must be one of " + csvFormats);
      }
    }
    csvFormat = applyCsvFormatMethods(csvFormat);
    if (csvColumns.isEmpty()) {
      checkState(
          !skipHeader,
          "csv.csvColumns property must be specified "
              + "if csv.skipHeaderRecord is true");
      return csvFormat.withHeader();
    } else {
      return csvFormat
          .withHeader(csvColumns.toArray(new String[0]))
          .withSkipHeaderRecord(skipHeader);
    }
  }

  private static Set<CSVFormat.Predefined> getPredefinedCsvFormats() {
    return new TreeSet<>(Arrays.asList(CSVFormat.Predefined.values()));
  }

  /**
   * Gets a map of supported {@code CSVFormat.with}* methods to functions that map
   * a configured string value to an appropriate argument for the method.
   */
  private static Map<Method, Function<String, Object>> getCsvFormatMethods() {
    // We need overloaded methods to be equal, which Method.equals does not do.
    Map<Method, Function<String, Object>> map = new TreeMap<>(comparing(Method::getName));
    for (Method method : CSVFormat.class.getDeclaredMethods()) {
      if (method.getName().startsWith("with")
          && method.getParameterCount() == 1
          && method.getReturnType() == CSVFormat.class) {
        Class<?> param = method.getParameterTypes()[0];
        if (param == char.class) {
          // Use putIfAbsent so that String overloads take precedence.
          map.putIfAbsent(method, value -> {
                checkArgument(!value.isEmpty());
                if (value.length() > 1) {
                  throw new InvalidConfigurationException(
                      String.format(
                          "Unable to configure %s(%s). Value must be a single character.",
                          method.getName(),
                          value));
                }
                return value.charAt(0);
              });
        } else if (param == boolean.class) {
          map.put(method, Configuration.BOOLEAN_PARSER::parse);
        } else if (param == String.class) {
          map.put(method, value -> value);
        }
      }
    }
    return map;
  }

  /**
   * For each CSVFormat class method with a name starting with "with", look for a
   * configuration parameter of the form csv.format.<methodName>. Call that method using
   * the configuration parameter's value.
   */
  private static CSVFormat applyCsvFormatMethods(CSVFormat csvFormat) {
    for (Map.Entry<Method, Function<String, Object>> entry : getCsvFormatMethods().entrySet()) {
      Method method = entry.getKey();
      String name = method.getName();
      String value =
          Configuration.getString(String.format(CSV_FORMAT_METHOD_VALUE, name), "").get();
      if (!value.isEmpty()) {
        Function<String, Object> parser = entry.getValue();
        try {
          csvFormat = (CSVFormat) method.invoke(csvFormat, parser.apply(value));
        } catch (IllegalAccessException | IllegalArgumentException e) {
          throw new InvalidConfigurationException(
              String.format("Unable to configure %s(%s)", name, value), e);
        } catch (InvocationTargetException e) {
          throw new InvalidConfigurationException(
              String.format("Unable to configure %s(%s)", name, value), e.getCause());
        }
      }
    }
    return csvFormat;
  }

  /**
   * Checks if the column names in toCheck can be found in golden.
   *
   * @param toCheck column names to verify
   * @param golden column names get from csv file
   */
  private static void verifyColumns(String configKey, Set<String> toCheck,
      String goldenDescription, Set<String> golden) {
    Set<String> missing = Sets.difference(toCheck, golden);
    if (!missing.isEmpty()) {
      throw new InvalidConfigurationException(
          getMissingMessage(configKey, missing, goldenDescription, golden));
    }
  }

  private static String getMissingMessage(String configKey, Set<String> missing,
      String goldenDescription, Set<String> golden) {
    return String.format("Invalid column names in %s: %s missing from %s (%s)",
        configKey, missing, golden, goldenDescription);
  }

  /**
   * Wraps CSVParser as a {@code CloseableIterable}, to ensure that it is closed,
   * and specifically as a {@code CloseableIterableOnce},
   * because {@code CSVParser.iterator()} always returns the same object.
   */
  private static class CSVFile extends CloseableIterableOnce<CSVRecord> {
    private final CSVParser csvParser;

    CSVFile(CSVParser csvParser) {
      super(checkNotNull(csvParser).iterator());
      this.csvParser = csvParser;
    }

    @Override
    public void close() {
      try {
        csvParser.close();
      } catch (IOException e) {
        logger.log(Level.WARNING, "Error closing the CSV file: " + e);
      } finally {
        super.close();
      }
    }
  }
}
