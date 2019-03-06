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

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.UrlBuilder.Builder;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for UrlBuilder. */
public class UrlBuilderTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Test
  public void testUrl() {
    Properties config = new Properties();
    String url = "http://anysite/{0}/category/{1}";
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_FORMAT, url);
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "1#2#3");
    allColumnValues.put("name", "product 1");
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    String golden = "http://anysite/1#2#3/category/product 1";
    assertEquals(golden, urlBuilder.buildUrl(allColumnValues));
  }

  @Test
  public void fromConfiguration_invalidPattern_throwsException() {
    Properties config = new Properties();
    String url = "http://anysite/{}";
    config.setProperty(UrlBuilder.CONFIG_COLUMNS, "id");
    config.setProperty(UrlBuilder.CONFIG_FORMAT, url);
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "42");
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    thrown.expect(IllegalArgumentException.class);
    thrown.expectCause(isA(NumberFormatException.class));
    urlBuilder.buildUrl(allColumnValues);
  }

  @Test
  public void fromConfiguration_extraPatternParameter_remainsUnsubstituted() {
    Properties config = new Properties();
    String url = "http://anysite/{0}/category/{1}";
    config.setProperty(UrlBuilder.CONFIG_COLUMNS, "id");
    config.setProperty(UrlBuilder.CONFIG_FORMAT, url);
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "42");
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    String golden = "http://anysite/42/category/{1}";
    assertEquals(golden, urlBuilder.buildUrl(allColumnValues));
  }

  @Test
  public void fromConfiguration_nonEnglishHostname_preservesCharacters() {
    Properties config = new Properties();
    String url = "http://她今天看起来.com/terms?q={0}";
    config.setProperty(UrlBuilder.CONFIG_COLUMNS, "id");
    config.setProperty(UrlBuilder.CONFIG_FORMAT, url);
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "42");
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    String golden = "http://她今天看起来.com/terms?q=42";
    assertEquals(golden, urlBuilder.buildUrl(allColumnValues));
  }

  @Test
  public void testEscapeUrl() {
    Properties config = new Properties();
    String url = "http://anysite/{0}/category/{1}";
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "name");
    config.put(UrlBuilder.CONFIG_FORMAT, url);
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "1#2#3");
    allColumnValues.put("name", "product 1");
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    String golden = "http://anysite/1#2#3/category/product%201";
    assertEquals(golden, urlBuilder.buildUrl(allColumnValues));
  }

  @Test
  public void testEscapeUrl_repeatCols() {
    Properties config = new Properties();
    String url = "http://anysite/{0}/category/{1}/id={0}";
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "name");
    config.put(UrlBuilder.CONFIG_FORMAT, url);
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "1#2#3");
    allColumnValues.put("name", "product 1");
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    String golden = "http://anysite/1#2#3/category/product%201/id=1#2#3";
    assertEquals(golden, urlBuilder.buildUrl(allColumnValues));
  }

  @Test
  public void testEscapeUrl_sameValues() {
    Properties config = new Properties();
    String url = "http://anysite/{0}/category/{1}";
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "id, name");
    config.put(UrlBuilder.CONFIG_FORMAT, url);
    setupConfig.initConfig(config);
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "product 1");
    allColumnValues.put("name", "product 1");
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    String golden = "http://anysite/product%201/category/product%201";
    assertEquals(golden, urlBuilder.buildUrl(allColumnValues));
  }

  @Test
  public void testInvalidEscapeColumns() {
    Properties config = new Properties();
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name");
    config.put(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE, "name, zipcode");
    setupConfig.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    thrown.expectMessage(UrlBuilder.CONFIG_COLUMNS_TO_ESCAPE);
    thrown.expectMessage("invalid");
    thrown.expectMessage("zipcode");
    UrlBuilder.fromConfiguration();
  }

  @Test
  public void testInvalidColumns() {
    Properties config = new Properties();
    ImmutableSet<String> allColumns = ImmutableSet.of("id", "name", "address", "phone");
    config.put(UrlBuilder.CONFIG_COLUMNS, "id, name, zipcode");
    setupConfig.initConfig(config);
    UrlBuilder urlBuilder = UrlBuilder.fromConfiguration();
    Set<String> missing = urlBuilder.getMissingColumns(allColumns);
    assertEquals(ImmutableSet.of("zipcode"), missing);
  }

  @Test
  public void testBuilder() {
    UrlBuilder urlBuilder = new Builder()
        .setFormat("http://anysite/{0}/category/{1}")
        .setColumns(ImmutableSet.of("id", "name"))
        .setColumnsToEscape(ImmutableSet.of("name"))
        .build();
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "1#2#3");
    allColumnValues.put("name", "product 1");
    String golden = "http://anysite/1#2#3/category/product%201";
    assertEquals(golden, urlBuilder.buildUrl(allColumnValues));
  }

  @Test
  public void testBuilder_emptyFormat() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("URL format must not be empty.");
    new Builder()
        .setFormat("")
        .setColumns(ImmutableSet.of("id", "name"))
        .setColumnsToEscape(ImmutableSet.of("name"))
        .build();
  }

  @Test
  public void testBuilder_emptyColumns() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("URL columns must not be empty.");
    new Builder()
        .setFormat("http://anysite/{0}/category/{1}")
        .setColumns(ImmutableSet.of())
        .setColumnsToEscape(ImmutableSet.of("name"))
        .build();
  }

  @Test
  public void testBuilder_emptyColumnsToEscape() {
    UrlBuilder urlBuilder = new Builder()
        .setFormat("http://anysite/{0}/category/{1}")
        .setColumns(ImmutableSet.of("id", "name"))
        .setColumnsToEscape(ImmutableSet.of())
        .build();
    Map<String, Object> allColumnValues = new HashMap<String, Object>();
    allColumnValues.put("id", "1#2#3");
    allColumnValues.put("name", "product 1");
    String golden = "http://anysite/1#2#3/category/product 1";
    assertEquals(golden, urlBuilder.buildUrl(allColumnValues));
  }
}
