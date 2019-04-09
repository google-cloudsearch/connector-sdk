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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

import com.google.api.client.util.DateTime;
import com.google.api.services.cloudsearch.v1.model.BooleanPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.Date;
import com.google.api.services.cloudsearch.v1.model.DatePropertyOptions;
import com.google.api.services.cloudsearch.v1.model.DateValues;
import com.google.api.services.cloudsearch.v1.model.DoublePropertyOptions;
import com.google.api.services.cloudsearch.v1.model.DoubleValues;
import com.google.api.services.cloudsearch.v1.model.EnumPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.EnumValuePair;
import com.google.api.services.cloudsearch.v1.model.EnumValues;
import com.google.api.services.cloudsearch.v1.model.GSuitePrincipal;
import com.google.api.services.cloudsearch.v1.model.HtmlPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.HtmlValues;
import com.google.api.services.cloudsearch.v1.model.IntegerPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.IntegerValues;
import com.google.api.services.cloudsearch.v1.model.NamedProperty;
import com.google.api.services.cloudsearch.v1.model.ObjectDefinition;
import com.google.api.services.cloudsearch.v1.model.ObjectPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.ObjectValues;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.api.services.cloudsearch.v1.model.PropertyDefinition;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.StructuredDataObject;
import com.google.api.services.cloudsearch.v1.model.TextPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.TextValues;
import com.google.api.services.cloudsearch.v1.model.TimestampPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.TimestampValues;
import com.google.common.base.Converter;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import java.io.File;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link StructuredData}. */

@RunWith(MockitoJUnitRunner.class)
public class StructuredDataTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ErrorCollector collector = new ErrorCollector();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock IndexingService mockIndexingService;

  private static enum MyEnum {
    P0(0),
    P1(1),
    P2(2),
    P3(3),
    P4(4),
    P5(5);

    final int intVal;

    MyEnum(int intVal) {
      this.intVal = intVal;
    }
  }

  interface OrdinalExtractor<T extends Enum<T>>{
    int getIntegerValue(T element);
  }

  private static final OrdinalExtractor<MyEnum> MY_ENUM_ORD_EXT = (t) -> t.intVal;

  @Test
  public void testInitNullSchema() {
    thrown.expect(NullPointerException.class);
    StructuredData.init(null);
  }

  @Test
  public void testInit() {
    StructuredData.init(new Schema());
    assertTrue(StructuredData.isInitialized());
  }

  @Test
  public void testReInit() {
    StructuredData.init(new Schema());
    assertTrue(StructuredData.isInitialized());
    StructuredData.init(new Schema());
    assertTrue(StructuredData.isInitialized());
  }

  @Test
  public void testInit_thenInitFromConfiguration() throws Exception {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());

    StructuredData.init(new Schema());
    assertTrue(StructuredData.isInitialized());
    StructuredData.initFromConfiguration(mockIndexingService);
    assertTrue(StructuredData.isInitialized());
  }

  @Test
  public void testInitFromConfiguration_thenInit() throws Exception {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());

    StructuredData.initFromConfiguration(mockIndexingService);
    assertTrue(StructuredData.isInitialized());
    StructuredData.init(new Schema());
    assertTrue(StructuredData.isInitialized());
  }

  @Test
  public void initFromConfiguration_multipleCalls_succeeds() throws Exception {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());

    StructuredData.initFromConfiguration(mockIndexingService);
    assertTrue(StructuredData.isInitialized());
    StructuredData.initFromConfiguration(mockIndexingService);
    assertTrue(StructuredData.isInitialized());
  }

  @Test
  public void init_multipleCallsDifferentSchemas_lastWins() throws Exception {
    Schema schema1 = new Schema().setObjectDefinitions(
        Collections.singletonList(getObjectDefinition("myObject1", Collections.emptyList())));
    Schema schema2 = new Schema().setObjectDefinitions(
        Collections.singletonList(getObjectDefinition("myObject2", Collections.emptyList())));

    StructuredData.init(schema1);
    assertTrue(StructuredData.isInitialized());
    assertTrue(StructuredData.hasObjectDefinition("myObject1"));
    assertFalse(StructuredData.hasObjectDefinition("myObject2"));

    StructuredData.init(schema2);
    assertTrue(StructuredData.isInitialized());
    assertTrue(StructuredData.hasObjectDefinition("myObject2"));
    assertFalse(StructuredData.hasObjectDefinition("myObject1"));
  }

  @Test
  public void initFromConfiguration_multipleCallsDifferentSchemas_lastWins() throws Exception {
    Schema schema1 = new Schema().setObjectDefinitions(
        Collections.singletonList(getObjectDefinition("myObject1", Collections.emptyList())));
    Schema schema2 = new Schema().setObjectDefinitions(
        Collections.singletonList(getObjectDefinition("myObject2", Collections.emptyList())));
    when(mockIndexingService.getSchema())
        .thenReturn(schema1)
        .thenReturn(schema2);

    setupConfig.initConfig(new Properties());
    StructuredData.initFromConfiguration(mockIndexingService);
    assertTrue(StructuredData.isInitialized());
    assertTrue(StructuredData.hasObjectDefinition("myObject1"));
    assertFalse(StructuredData.hasObjectDefinition("myObject2"));

    StructuredData.initFromConfiguration(mockIndexingService);
    assertTrue(StructuredData.isInitialized());
    assertTrue(StructuredData.hasObjectDefinition("myObject2"));
    assertFalse(StructuredData.hasObjectDefinition("myObject1"));
  }

  @Test
  public void testInitFromConfiguration_getSchemaException() throws Exception {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenThrow(new IOException("failed to get schema"));

    thrown.expect(StartupException.class);
    thrown.expectMessage("Failed to initialize StructuredData");
    StructuredData.initFromConfiguration(mockIndexingService);
  }

  @Test
  public void testNotInitialized() {
    assertFalse(StructuredData.isInitialized());
    thrown.expect(IllegalStateException.class);
    StructuredData.getStructuredData("myObject", ArrayListMultimap.create());
  }

  @Test
  public void testMissingObjectDefinitions() {
    StructuredData.init(new Schema());
    assertTrue(StructuredData.isInitialized());
    thrown.expect(IllegalArgumentException.class);
    StructuredData.getStructuredData("myObject", ArrayListMultimap.create());
  }

  @Test
  public void testInitFromConfiguration_localSchema() throws IOException {
    String schema =
        "{ \"objectDefinitions\" : [\n"
            + "{ \"name\" : \"myObject\", \"propertyDefinitions\" : "
            + "[{\"name\" :\"textProperty\", \"textPropertyOptions\" : {}}] } ] }";
    File schemaFile = temporaryFolder.newFile("schema.json");
    Files.asCharSink(schemaFile, UTF_8).write(schema);

    Properties config = new Properties();
    config.put(StructuredData.LOCAL_SCHEMA, schemaFile.getAbsolutePath());
    setupConfig.initConfig(config);
    StructuredData.initFromConfiguration(mockIndexingService);
    assertTrue(StructuredData.hasObjectDefinition("myObject"));
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("textProperty", "v1");
    // textProperty defined in schema will be initialized with isRepeatable = false. So value v2
    // will be ignored.
    values.put("textProperty", "v2");
    assertEquals(
        new StructuredDataObject()
            .setProperties(
                Collections.singletonList(
                    new NamedProperty()
                        .setName("textProperty")
                        .setTextValues(
                            new TextValues().setValues(Collections.singletonList("v1"))))),
        StructuredData.getStructuredData("myObject", values));
  }

  @Test
  public void testWithEmptyObjectDefinition() {
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Collections.singletonList(getObjectDefinition("myObject", Collections.emptyList())));
    StructuredData.init(schema);
    assertTrue(StructuredData.isInitialized());
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("textProperty", "v1");
    values.put("textProperty", "v2");
    assertEquals(
        new StructuredDataObject().setProperties(Collections.emptyList()),
        StructuredData.getStructuredData("myObject", values));
    assertTrue(StructuredData.hasObjectDefinition("myObject"));
  }

  @Test
  public void testWithEmptyValues() {
    setupSchema();
    assertTrue(StructuredData.isInitialized());
    assertEquals(
        new StructuredDataObject().setProperties(Collections.emptyList()),
        StructuredData.getStructuredData("myObject", ArrayListMultimap.create()));
    assertFalse(StructuredData.hasObjectDefinition("undefinedObject"));
  }

  @Test
  public void testGetStructuredData() {
    setupSchema();
    StructuredDataObject expected =
        new StructuredDataObject()
            .setProperties(
                Arrays.asList(
                    new NamedProperty()
                        .setName("textProperty")
                        .setTextValues(new TextValues().setValues(Arrays.asList("v1", "v2"))),
                    new NamedProperty()
                        .setName("htmlProperty")
                        .setHtmlValues(new HtmlValues().setValues(Arrays.asList("f1", "f2"))),
                    new NamedProperty().setName("booleanProperty").setBooleanValue(false)));

    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("textProperty", "v1");
    values.put("textProperty", "v2");
    values.put("htmlProperty", "f1");
    values.put("htmlProperty", "f2");
    values.put("htmlProperty", null);
    values.put("booleanProperty", "false");
    values.put("booleanProperty", true); // this should be ignored
    assertEquals(expected, StructuredData.getStructuredData("myObject", values));
  }

  @Test
  public void testGetStructuredDataAllTypes() {
    setupSchema();
    long currentMillis = System.currentTimeMillis();
    DateTime currentTime = new DateTime(currentMillis);
    String currentString = currentTime.toStringRfc3339();
    Calendar currentCal = Calendar.getInstance();
    currentCal.setTimeInMillis(currentMillis);
    Principal userPrincipal =
        new Principal().setGsuitePrincipal(new GSuitePrincipal().setGsuiteDomain(true));

    Date dateValue1 =
        new com.google.api.services.cloudsearch.v1.model.Date()
            .setDay(25)
            .setMonth(12)
            .setYear(2017);

    Date dateValue2 =
        new com.google.api.services.cloudsearch.v1.model.Date()
            .setDay(currentCal.get(Calendar.DATE))
            .setMonth(currentCal.get(Calendar.MONTH) + 1)
            .setYear(currentCal.get(Calendar.YEAR));

    StructuredDataObject nestedObject =
        new StructuredDataObject()
            .setProperties(
                Arrays.asList(
                    new NamedProperty()
                        .setName("nestedStringProperty")
                        .setTextValues(new TextValues().setValues(Arrays.asList("nested")))));

    StructuredDataObject expectedSubObject =
        new StructuredDataObject()
            .setProperties(
                Arrays.asList(
                    new NamedProperty()
                        .setName("subTextProperty")
                        .setTextValues(
                            new TextValues().setValues(Arrays.asList("sub-v1", "sub-v2"))),
                    new NamedProperty()
                        .setName("subIntegerProperty")
                        .setIntegerValues(new IntegerValues().setValues(Arrays.asList(123L, 456L))),
                    new NamedProperty()
                        .setName("nestedObject")
                        .setObjectValues(
                            new ObjectValues()
                                .setValues(Collections.singletonList(nestedObject)))));
    StructuredDataObject expected =
        new StructuredDataObject()
            .setProperties(
                Arrays.asList(
                    new NamedProperty()
                        .setName("textProperty")
                        .setTextValues(new TextValues().setValues(Arrays.asList("v1", "v2"))),
                    new NamedProperty()
                        .setName("integerProperty")
                        .setIntegerValues(
                            new IntegerValues().setValues(Arrays.asList(10L, 105L, 200L, 300L))),
                    new NamedProperty().setName("booleanProperty").setBooleanValue(false),
                    new NamedProperty()
                        .setName("dateTimeProperty")
                        .setTimestampValues(
                            new TimestampValues()
                                .setValues(
                                    Arrays.asList(
                                        currentTime.toStringRfc3339(),
                                        currentString,
                                        new DateTime(currentMillis + 10000).toStringRfc3339()))),
                    new NamedProperty()
                        .setName("dateProperty")
                        .setDateValues(
                            new DateValues()
                                .setValues(Arrays.asList(dateValue1, dateValue2, dateValue2))),
                    new NamedProperty()
                        .setName("doubleProperty")
                        .setDoubleValues(new DoubleValues().setValues(Arrays.asList(0d, 2.5, 50d))),
                    new NamedProperty()
                        .setName("enumProperty")
                        .setEnumValues(
                            new EnumValues()
                                .setValues(Arrays.asList("P0", "P1", "P3", "P4", "P5"))),
                    new NamedProperty()
                        .setName("objectProperty")
                        .setObjectValues(
                            new ObjectValues()
                                .setValues(Collections.singletonList(expectedSubObject)))));

    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("textProperty", "v1");
    values.put("textProperty", "v2");
    values.put("booleanProperty", "false");
    values.put("booleanProperty", true); // this should be ignored
    values.put("integerProperty", 10);
    values.put("integerProperty", "105");
    values.put("integerProperty", 200.0);
    values.put("integerProperty", "300.50");
    values.put("dateTimeProperty", currentTime);
    values.put("dateTimeProperty", currentString);
    values.put("dateTimeProperty", currentMillis + 10000);

    values.put("doubleProperty", 0);
    values.put("doubleProperty", "2.5");
    values.put("doubleProperty", "50");

    values.put("dateProperty", dateValue1);
    values.put("dateProperty", currentCal.getTime());
    values.put("dateProperty", currentMillis);

    values.put("enumProperty", MyEnum.P0);
    values.put("enumProperty", MyEnum.P1.intVal);
    values.put("enumProperty", "3");
    values.put("enumProperty", 4);
    values.put("enumProperty", "P5");
    values.put("userProperty", userPrincipal);
    Multimap<String, Object> subObject = ArrayListMultimap.create();
    subObject.put("subTextProperty", "sub-v1");
    subObject.put("subTextProperty", "sub-v2");
    subObject.put("subIntegerProperty", 123);
    subObject.put("subIntegerProperty", "456");
    Multimap<String, Object> nested = ArrayListMultimap.create();
    nested.put("nestedStringProperty", "nested");
    subObject.put("nestedObject", nested);
    values.put("objectProperty", subObject);
    assertEquals(expected, StructuredData.getStructuredData("myObject", values));
  }

  @Test
  public void testInvalidNestedObjectValues() {
    setupSchema();
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("objectProperty", "invalid");
    thrown.expect(IllegalArgumentException.class);
    StructuredData.getStructuredData("myObject", values);
  }

  @Test
  public void testConversionError() {
    setupSchema();
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("integerProperty", "v1");
    thrown.expect(NumberFormatException.class);
    StructuredData.getStructuredData("myObject", values);
  }

  @Test
  public void testConversionErrorBoolean() {
    setupSchema();
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("booleanProperty", new DateTime(System.currentTimeMillis()));
    thrown.expect(IllegalArgumentException.class);
    StructuredData.getStructuredData("myObject", values);
  }

  @Test
  public void testConversionErrorDateTime() {
    setupSchema();
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("dateTimeProperty", "v1");
    thrown.expect(NumberFormatException.class);
    StructuredData.getStructuredData("myObject", values);
  }

  @Test
  public void testConversionErrorEnum_string() {
    setupSchema();
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("enumProperty", "P99");
    thrown.expect(IllegalArgumentException.class);
    StructuredData.getStructuredData("myObject", values);
  }

  @Test
  public void testConversionErrorEnum_intString() {
    setupSchema();
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("enumProperty", "99");
    thrown.expect(IllegalArgumentException.class);
    StructuredData.getStructuredData("myObject", values);
  }

  @Test
  public void testConversionErrorEnum_integer() {
    setupSchema();
    Multimap<String, Object> values = ArrayListMultimap.create();
    values.put("enumProperty", Integer.valueOf(99));
    thrown.expect(IllegalArgumentException.class);
    StructuredData.getStructuredData("myObject", values);
  }

  @Test
  public void testDateTimeConverter_nonZeroOffset() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    String rfc3339String = "2018-08-08T15:48:17.000-07:00";
    DateTime expected = new DateTime(rfc3339String);
    // Baseline so we don't chase unexpected errors below.
    assertEquals("Baseline failure", rfc3339String, expected.toString());

    Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
    for (String dateString : new String[] {
          "2018-08-08T15:48:17.000-07:00",
          "2018-08-08t15:48:17-07:00", // Lowercase t
          // "2018-08-08T15:48:17-07", TODO(jlacey): Restore these if running Java 9.
          "2018-08-08 15:48:17.000-07:00",
          "2018-08-08 15:48:17-07:00",
          // "2018-08-08 15:48:17-07", TODO(jlacey): ibid.
          "Wed, 8 Aug 2018 15:48:17 -0700",
          "08 AUG 2018 15:48:17 -0700", // Uppercase month name
        }) {
      try {
        collector.checkThat(dateString, converter.convert(dateString), equalTo(expected));
      } catch (NumberFormatException e) {
        collector.addError(e);
      }
    }
  }

  @Test
  public void testDateTimeConverter_zeroOffset() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    String rfc3339String = "2018-08-08T15:48:17.000Z";
    DateTime expected = new DateTime(rfc3339String);
    // Baseline so we don't chase unexpected errors below.
    assertEquals("Baseline failure", rfc3339String, expected.toString());

    Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
    for (String dateString : new String[] {
          "2018-08-08T15:48:17.000-00:00",
          "2018-08-08T15:48:17+00:00",
          // "2018-08-08T15:48:17+00", TODO(jlacey): ibid.
          "2018-08-08T15:48:17Z",
          "2018-08-08 15:48:17.000-00:00",
          "2018-08-08 15:48:17+00:00",
          // "2018-08-08 15:48:17+00", TODO(jlacey): ibid.
          "2018-08-08 15:48:17Z",
          "Wed, 8 Aug 2018 15:48:17 +0000",
          "08 Aug 2018 15:48:17 GMT",
        }) {
      try {
        collector.checkThat(dateString, converter.convert(dateString), equalTo(expected));
      } catch (NumberFormatException e) {
        collector.addError(e);
      }
    }
  }

  @Test
  public void testDateTimeConverter_noOffset() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT-04:00"));
    try {
      String rfc3339String = "2018-08-08T15:48:17.000-04:00";
      DateTime expected = new DateTime(rfc3339String);
      // Baseline so we don't chase unexpected errors below.
      assertEquals("Baseline failure", rfc3339String, expected.toString());

      Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
      for (String dateString : new String[] {
            "2018-08-08T15:48:17.000",
            "2018-08-08T15:48:17",
            "2018-08-08 15:48:17.000",
            "2018-08-08 15:48:17",
          }) {
        try {
          collector.checkThat(dateString, converter.convert(dateString), equalTo(expected));
        } catch (NumberFormatException e) {
          collector.addError(e);
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  public void testDateTimeConverter_dateOnly() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT-04:00"));
    try {
      String rfc3339String = "2018-08-08T00:00:00.000-04:00";
      DateTime expected = new DateTime(rfc3339String);
      // Baseline so we don't chase unexpected errors below.
      assertEquals("Baseline failure", rfc3339String, expected.toString());

      Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
      for (String dateString : new String[] {
            "2018-08-08-04:00",
            "2018-08-08",
          }) {
        try {
          collector.checkThat(dateString, converter.convert(dateString), equalTo(expected));
        } catch (NumberFormatException e) {
          collector.addError(e);
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  public void testDateTimeConverter_dateOnly_mismatch() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT-04:00"));
    try {
      String rfc3339String = "2018-08-08T00:00:00.000-04:00";
      DateTime expected = new DateTime(rfc3339String);
      // Baseline so we don't chase unexpected errors below.
      assertEquals("Baseline failure", rfc3339String, expected.toString());

      Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
      for (String dateString : new String[] {
            // Time zones are still parsed from strings with no time.
            "2018-08-08-07:00",
          }) {
        try {
          collector.checkThat(dateString, converter.convert(dateString), not(equalTo(expected)));
        } catch (NumberFormatException e) {
          collector.addError(e);
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  public void testDateTimeConverter_noDate() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
    thrown.expect(NumberFormatException.class);
    converter.convert("15:48:17-04:00");
  }

  @Test
  public void testDateTimeConverter_configuredPatterns() throws IOException {
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    Properties config = new Properties();
    config.put(StructuredData.DATETIME_PATTERNS, "M/d/yy h:mm:ss a zzz; dd MMM yyyy HH:mm[:ss]xx");
    setupConfig.initConfig(config);
    StructuredData.initFromConfiguration(mockIndexingService);

    String rfc3339String = "2018-08-08T15:48:17.000-07:00";
    DateTime expected = new DateTime(rfc3339String);
    // Baseline so we don't chase unexpected errors below.
    assertEquals("Baseline failure", rfc3339String, expected.toString());

    Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
    for (String dateString : new String[] {
          "2018-08-08T15:48:17-07:00",
          "Wed, 8 Aug 2018 15:48:17 -0700",
          "8/8/18 3:48:17 PM GMT-07:00",
          "08 AuG 2018 15:48:17-0700", // Mixed-case month name
        }) {
      try {
        collector.checkThat(dateString, converter.convert(dateString), equalTo(expected));
      } catch (NumberFormatException e) {
        collector.addError(e);
      }
    }
  }

  @Test
  public void testDateTimeConverter_configuredPatterns_dateOnly() throws IOException {
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    Properties config = new Properties();
    config.put(StructuredData.DATETIME_PATTERNS, "M/d/yy ;dd MMM uuuu");
    setupConfig.initConfig(config);
    StructuredData.initFromConfiguration(mockIndexingService);

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT-04:00"));
    try {
      String rfc3339String = "2018-08-08T00:00:00.000-04:00";
      DateTime expected = new DateTime(rfc3339String);
      // Baseline so we don't chase unexpected errors below.
      assertEquals("Baseline failure", rfc3339String, expected.toString());

      Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
      for (String dateString : new String[] {
            "2018-08-08-04:00",
            "Wed, 8 Aug 2018 00:00:00 -0400",
            "8/8/18",
            "08 aUg 2018",
          }) {
        try {
          collector.checkThat(dateString, converter.convert(dateString), equalTo(expected));
        } catch (NumberFormatException e) {
          collector.addError(e);
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  public void testDateTimeConverter_configuredPatterns_dateOnly_mismatch() throws IOException {
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    Properties config = new Properties();
    config.put(StructuredData.DATETIME_PATTERNS, "M/d/yy ;dd MMM uuuu");
    setupConfig.initConfig(config);
    StructuredData.initFromConfiguration(mockIndexingService);

    TimeZone original = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT-04:00"));
    try {
      String rfc3339String = "2018-08-08T00:00:00.000-04:00";
      DateTime expected = new DateTime(rfc3339String);
      // Baseline so we don't chase unexpected errors below.
      assertEquals("Baseline failure", rfc3339String, expected.toString());

      Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
      for (String dateString : new String[] {
            // Time zones are still parsed from strings with no time.
            "2018-08-08-07:00",
          }) {
        try {
          collector.checkThat(dateString, converter.convert(dateString), not(equalTo(expected)));
        } catch (NumberFormatException e) {
          collector.addError(e);
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  /**
   * This tests both explicit empty config (handled by Configuration.getMultiValue)
   * and the ResetStructuredDataRule used by the tests.
   */
  @Test
  public void testDateTimeConverter_configuredEmptyPatterns() throws IOException {
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    Properties config = new Properties();
    config.put(StructuredData.DATETIME_PATTERNS, "");
    setupConfig.initConfig(config);
    StructuredData.initFromConfiguration(mockIndexingService);

    thrown.expect(NumberFormatException.class);
    StructuredData.DATETIME_CONVERTER.convert("8/8/18 3:48:17 PM GMT-07:00");
  }

  /** Tests the parsing of multiple strings, and capture of all pattern syntax errors. */
  @Test
  public void testDateTimeConverter_configuredInvalidPatterns() throws IOException {
    Properties config = new Properties();
    config.put(StructuredData.DATETIME_PATTERNS, "foo, bar; baz");
    setupConfig.initConfig(config);

    try {
      StructuredData.initFromConfiguration(mockIndexingService);
      fail("Expected a NumberFormatException");
    } catch (InvalidConfigurationException e) {
      assertThat(e.getMessage(), containsString("foo, bar"));
      List<Throwable> suppressed = Arrays.asList(e.getSuppressed());
      assertEquals("Expected 1 exception: " + suppressed, 1, suppressed.size());
      assertThat(suppressed.get(0), instanceOf(InvalidConfigurationException.class));
      assertThat(suppressed.get(0).getMessage(), containsString("baz"));
    }
  }

  @Test
  public void dateTimeConverter_fromDate() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    String rfc3339String = "2018-08-08T15:48:17.000-04:00";
    // DATETIME_CONVERTER assumes the current time zone, so we need to
    // construct an expected value in the local time zone.
    DateTime expected = new DateTime(new DateTime(rfc3339String).getValue());

    Converter<Object, DateTime> converter = StructuredData.DATETIME_CONVERTER;
    java.util.Date input = java.util.Date.from(ZonedDateTime.parse(rfc3339String).toInstant());
    assertThat(converter.convert(input), equalTo(expected));
  }

  @Test
  public void dateTimeConverter_fromUnsupportedObject_throwsException() throws IOException {
    thrown.expect(NumberFormatException.class);
    thrown.expectMessage("Object");
    StructuredData.DATETIME_CONVERTER.convert(new Object());
  }

  @Test
  public void dateConverter_fromString() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    Date expected = new Date().setYear(2018).setMonth(8).setDay(8);

    Converter<Object, Date> converter = StructuredData.DATE_CONVERTER;
    for (String dateString : new String[] {
          // API Date class doesn't have a time zone, so anything will match.
          // TODO(jlacey): We could construct offsets relative to the actual local time zone,
          // rather than assuming that +14:00 and -11:00 will always test rollover.
          "2018-08-08T15:48:17.000-07:00",
          "2018-08-08 15:48:17-04:00",
          "Wed, 8 Aug 2018 15:48:17 -0700",
          "08 Aug 2018 15:48:17 +0100",
          "2018-08-08T23:48:17-11:00", // 2018-08-09 in most local time zones.
          "2018-08-08+14:00", // 2018-08-07 in most local time zones.
          "2018-08-08",
        }) {
      try {
        collector.checkThat(dateString, converter.convert(dateString), equalTo(expected));
      } catch (NumberFormatException e) {
        collector.addError(e);
      }
    }
  }

  @Test
  public void dateConverter_fromString_noDate() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    Converter<Object, Date> converter = StructuredData.DATE_CONVERTER;
    thrown.expect(NumberFormatException.class);
    converter.convert("15:48:17-04:00");
  }

  @Test
  public void dateConverter_fromString_unparsedCharacters() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    Converter<Object, Date> converter = StructuredData.DATE_CONVERTER;
    thrown.expect(NumberFormatException.class);
    converter.convert("2018-08-08T15:48:17.000-07:00 and so on");
  }

  @Test
  public void dateConverter_fromDateTime() throws IOException {
    setupConfig.initConfig(new Properties());
    when(mockIndexingService.getSchema()).thenReturn(new Schema());
    StructuredData.initFromConfiguration(mockIndexingService);

    Date expected = new Date().setYear(2018).setMonth(8).setDay(8);

    Converter<Object, Date> converter = StructuredData.DATE_CONVERTER;
    for (String dateString : new String[] {
          // API Date class doesn't have a time zone, so anything will match.
          "2018-08-08T23:48:17-11:00", // 2018-08-09 in most local time zones.
          "2018-08-08T00:01:00+14:00", // 2018-08-07 in most local time zones.
          "2018-08-08",
        }) {
      try {
        collector.checkThat(dateString, converter.convert(new DateTime(dateString)),
            equalTo(expected));
      } catch (NumberFormatException e) {
        collector.addError(e);
      }
    }
  }

  @Test
  public void dateConverter_fromUnsupportedObject_throwsException() throws IOException {
    thrown.expect(NumberFormatException.class);
    thrown.expectMessage("Object");
    StructuredData.DATE_CONVERTER.convert(new Object());
  }

  private void setupSchema() {
    Schema schema = new Schema();
    schema.setObjectDefinitions(
        Collections.singletonList(getObjectDefinition("myObject", getAllKinds())));
    StructuredData.init(schema);
  }

  private ObjectDefinition getObjectDefinition(String name, List<PropertyDefinition> properties) {
    return new ObjectDefinition().setName(name).setPropertyDefinitions(properties);
  }

  private List<PropertyDefinition> getAllKinds() {
    List<PropertyDefinition> properties = new ArrayList<PropertyDefinition>();
    properties.add(
        new PropertyDefinition()
            .setName("textProperty")
            .setIsRepeatable(true)
            .setTextPropertyOptions(new TextPropertyOptions()));
    properties.add(
        new PropertyDefinition()
            .setName("htmlProperty")
            .setIsRepeatable(true)
            .setHtmlPropertyOptions(new HtmlPropertyOptions()));
    properties.add(
        new PropertyDefinition()
            .setName("integerProperty")
            .setIsRepeatable(true)
            .setIntegerPropertyOptions(new IntegerPropertyOptions()));
    properties.add(
        new PropertyDefinition()
            .setName("booleanProperty")
            .setIsRepeatable(false)
            .setBooleanPropertyOptions(new BooleanPropertyOptions()));
    properties.add(
        new PropertyDefinition()
            .setName("dateTimeProperty")
            .setIsRepeatable(true)
            .setTimestampPropertyOptions(new TimestampPropertyOptions()));
    properties.add(
        new PropertyDefinition()
            .setName("dateProperty")
            .setIsRepeatable(true)
            .setDatePropertyOptions(new DatePropertyOptions()));
    properties.add(
        new PropertyDefinition()
            .setName("doubleProperty")
            .setIsRepeatable(true)
            .setDoublePropertyOptions(new DoublePropertyOptions()));
    properties.add(
        new PropertyDefinition()
            .setName("enumProperty")
            .setIsRepeatable(true)
            .setEnumPropertyOptions(
                new EnumPropertyOptions()
                    .setPossibleValues(getEnumValuePairs(MyEnum.class, MY_ENUM_ORD_EXT))));
    PropertyDefinition nestedProperty =
        new PropertyDefinition()
            .setName("nestedObject")
            .setIsRepeatable(true)
            .setObjectPropertyOptions(
                new ObjectPropertyOptions()
                    .setSubobjectProperties(
                        Arrays.asList(
                            new PropertyDefinition()
                                .setName("nestedStringProperty")
                                .setIsRepeatable(true)
                                .setTextPropertyOptions(new TextPropertyOptions()))));
    properties.add(
        new PropertyDefinition()
            .setName("objectProperty")
            .setIsRepeatable(true)
            .setObjectPropertyOptions(
                new ObjectPropertyOptions()
                    .setSubobjectProperties(
                        Arrays.asList(
                            new PropertyDefinition()
                                .setName("subTextProperty")
                                .setIsRepeatable(true)
                                .setTextPropertyOptions(new TextPropertyOptions()),
                            new PropertyDefinition()
                                .setName("subIntegerProperty")
                                .setIsRepeatable(true)
                                .setIntegerPropertyOptions(new IntegerPropertyOptions()),
                            nestedProperty))));
    return properties;
  }

  private static <T extends Enum<T>> List<EnumValuePair> getEnumValuePairs(
      Class<T> enumClass, OrdinalExtractor<T> ordinalExtractor) {
    return Arrays.asList(enumClass.getEnumConstants())
        .stream()
        .map(
            e ->
                new EnumValuePair()
                    .setStringValue(e.name())
                    .setIntegerValue(ordinalExtractor.getIntegerValue(e)))
        .collect(Collectors.toList());
  }
}
