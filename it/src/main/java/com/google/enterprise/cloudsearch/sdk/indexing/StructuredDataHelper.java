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

import static com.google.common.truth.Truth.assertThat;
import static java.util.Arrays.asList;

import com.google.api.services.cloudsearch.v1.model.BooleanPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.DatePropertyOptions;
import com.google.api.services.cloudsearch.v1.model.DoublePropertyOptions;
import com.google.api.services.cloudsearch.v1.model.EnumPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.EnumValuePair;
import com.google.api.services.cloudsearch.v1.model.HtmlPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.IntegerPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.NamedProperty;
import com.google.api.services.cloudsearch.v1.model.ObjectDefinition;
import com.google.api.services.cloudsearch.v1.model.PropertyDefinition;
import com.google.api.services.cloudsearch.v1.model.RetrievalImportance;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.api.services.cloudsearch.v1.model.TextPropertyOptions;
import com.google.api.services.cloudsearch.v1.model.TimestampPropertyOptions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Helper class for Mock Connector Structured Data
 */
public class StructuredDataHelper {

  private static final Comparator<PropertyDefinition> PROPERTY_DEFINITION_COMPARATOR =
      (p1, p2) -> p1.getName().compareTo(p2.getName());

  private static final Comparator<NamedProperty> NAMED_PROPERTY_COMPARATOR =
      (c1, c2) -> c1.getName().compareTo(c2.getName());

  private static PropertyDefinition getDatePropertyDefinition() {
    return new PropertyDefinition()
        .setDatePropertyOptions(new DatePropertyOptions())
        .setName("date")
        .setIsRepeatable(true)
        .setIsReturnable(true);
  }

  private static PropertyDefinition getIntegerPropertyDefinition() {
    return new PropertyDefinition()
        .setIntegerPropertyOptions(new IntegerPropertyOptions())
        .setName("integer")
        .setIsRepeatable(true)
        .setIsReturnable(true);
  }

  private static PropertyDefinition getHtmlPropertyDefinition() {
    return new PropertyDefinition()
        .setHtmlPropertyOptions(new HtmlPropertyOptions()
            .setRetrievalImportance(new RetrievalImportance()))
        .setName("html")
        .setIsRepeatable(true)
        .setIsReturnable(true);
  }

  private static PropertyDefinition getTextPropertyDefinition() {
    return new PropertyDefinition()
        .setTextPropertyOptions(new TextPropertyOptions()
            .setRetrievalImportance(new RetrievalImportance()))
        .setName("text")
        .setIsRepeatable(true)
        .setIsReturnable(true);
  }

  private static PropertyDefinition getBooleanPropertyDefinition() {
    return new PropertyDefinition()
        .setBooleanPropertyOptions(new BooleanPropertyOptions())
        .setName("boolean")
        .setIsReturnable(true);
  }

  private static PropertyDefinition getEnumPropertyDefinition() {
    EnumPropertyOptions options = new EnumPropertyOptions();
    options.setPossibleValues(ImmutableList.of(
        new EnumValuePair().setStringValue("one").setIntegerValue(1),
        new EnumValuePair().setStringValue("two").setIntegerValue(2)));
    return new PropertyDefinition()
        .setEnumPropertyOptions(options)
        .setName("enum")
        .setIsRepeatable(true)
        .setIsReturnable(true);
  }

  private static PropertyDefinition getTimestampPropertyDefinition() {
    return new PropertyDefinition()
        .setTimestampPropertyOptions(new TimestampPropertyOptions())
        .setName("timestamp")
        .setIsRepeatable(true)
        .setIsReturnable(true);
  }

  private static PropertyDefinition getDoublePropertyDefinition() {
    return new PropertyDefinition()
        .setDoublePropertyOptions(new DoublePropertyOptions())
        .setName("double")
        .setIsRepeatable(true)
        .setIsReturnable(true);
  }

  public static void assertStructuredData(Item actualItem, Item expectedItem,
      String schemaObjectType) throws IOException {
    assertThat(schemaObjectType).isEqualTo(actualItem.getMetadata().getObjectType());
    Collections.sort(getItemProperties(expectedItem), NAMED_PROPERTY_COMPARATOR);
    Collections.sort(getItemProperties(actualItem), NAMED_PROPERTY_COMPARATOR);
    assertThat(getItemProperties(expectedItem)).isEqualTo(getItemProperties(actualItem));
  }

  private static List<NamedProperty> getItemProperties(Item itemResponse) {
    return itemResponse
        .getStructuredData()
        .getObject()
        .getProperties();
  }

  /**
   * Verify data source is set up with correct schema.
   */
  public static void verifyMockContentDatasourceSchema(Schema actualSchema)
      throws IOException {
    ObjectDefinition objectDefinition = new ObjectDefinition()
        .setName("myMockDataObject");
    objectDefinition.setPropertyDefinitions(asList(
        getTextPropertyDefinition(),
        getDatePropertyDefinition(),
        getBooleanPropertyDefinition(),
        getIntegerPropertyDefinition(),
        getHtmlPropertyDefinition(),
        getEnumPropertyDefinition(),
        getTimestampPropertyDefinition(),
        getDoublePropertyDefinition()));

    Schema expectedSchema = new Schema()
        .setObjectDefinitions(asList(objectDefinition));
    // Sort Schema ObjectDefinitions
    actualSchema
        .getObjectDefinitions()
        .stream()
        .forEach(o-> o.getPropertyDefinitions().sort(PROPERTY_DEFINITION_COMPARATOR));
    expectedSchema
        .getObjectDefinitions()
        .stream()
        .forEach(o-> o.getPropertyDefinitions().sort(PROPERTY_DEFINITION_COMPARATOR));
    assertThat(expectedSchema).isEqualTo(actualSchema);
  }
}