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

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentTemplate.Builder;
import com.google.enterprise.cloudsearch.sdk.indexing.ContentTemplate.UnmappedColumnsMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link ContentTemplate}. */

public class ContentTemplateTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Test
  public void testGetDivTag() {
    assertEquals(
        "<div id='theKey'>\n  <p>theKey:</p>\n  <h2>%s</h2>\n</div>\n",
        ContentTemplate.getDiv("theKey", "<h2>", "</h2>", true));
  }

  @Test
  public void testGetDivEmptyTag() {
    assertEquals(
        "<div id='theKey'>\n  <p>theKey:</p>\n  <p>%s</p>\n</div>\n",
        ContentTemplate.getDiv("theKey", "<p>", "</p>", true));
  }

  @Test
  public void testEmptyKey() {
    thrown.expect(IllegalArgumentException.class);
    ContentTemplate.getDiv("", "<p>", "</p>", true);
  }

  @Test
  public void testEmptyTag() {
    thrown.expect(IllegalArgumentException.class);
    ContentTemplate.getDiv("theKey", "", "</p>", true);
  }

  @Test
  public void testBuildTemplate() {
    String title = "TField";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    ContentTemplate template =
        new Builder()
            .setTitle(title)
            .setHighContent(high)
            .setMediumContent(med)
            .setLowContent(low)
            .setUnmappedColumnMode(UnmappedColumnsMode.IGNORE)
            .build();
    assertEquals(title, template.getTitle());
    assertEquals(new LinkedHashSet<>(high), template.getHighContent());
    assertEquals(new LinkedHashSet<>(med), template.getMediumContent());
    assertEquals(new LinkedHashSet<>(low), template.getLowContent());
    StringBuilder target = new StringBuilder();
    target.append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>%s</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>MField1:</p>\n  <p>%s</p>\n</div>\n")
        .append("<div id='LField3'>\n  <p>LField3:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.getTemplate());
  }

  @Test
  public void testBuildTemplateNoIncludeFieldNames() {
    String title = "TField";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    ContentTemplate template =
        new Builder()
            .setTitle(title)
            .setIncludeFieldName(false)
            .setHighContent(high)
            .setMediumContent(med)
            .setLowContent(low)
            .setUnmappedColumnMode(UnmappedColumnsMode.IGNORE)
            .build();
    StringBuilder target = new StringBuilder();
    target.append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>%s</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>%s</p>\n</div>\n")
        .append("<div id='LField3'>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p><small>%s</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.getTemplate());
  }

  @Test
  public void testBuildTemplateNoMediumsDupHigh() {
    String title = "TField ";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2", " HField1 "));
    List<String> low = new ArrayList<>(Arrays.asList(" LField3", "LField2 ", "  LField1  "));
    ContentTemplate template = new Builder().setTitle(title).setHighContent(high)
        .setLowContent(low).build();
    StringBuilder target = new StringBuilder();
    target
        .append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>%s</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='LField3'>\n  <p>LField3:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("%s</body>\n</html>\n");
    assertEquals(target.toString(), template.getTemplate());
  }

  @Test
  public void testBuildTemplateDupMedLow() {
    String title = "TField ";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("HField2", "MField1"));
    List<String> low = new ArrayList<>(
        Arrays.asList(" LField3", " HField2", "MField1", "LField2 ", "  LField1  "));
    ContentTemplate template = new Builder().setTitle(title).setHighContent(high)
        .setMediumContent(med).setLowContent(low).build();
    StringBuilder target = new StringBuilder();
    target
        .append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>%s</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>MField1:</p>\n  <p>%s</p>\n</div>\n")
        .append("<div id='LField3'>\n  <p>LField3:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("%s</body>\n</html>\n");
    assertEquals(target.toString(), template.getTemplate());
  }

  @Test
  public void testSetHighWithNull() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be null"));
    new Builder().setHighContent(null);
  }

  @Test
  public void testSetHighWithNullContent() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be empty"));
    new Builder().setHighContent(Arrays.asList("Field1", "", "Field3"));
  }

  @Test
  public void testSetMedWithNull() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be null"));
    new Builder().setMediumContent(null);
  }

  @Test
  public void testSetMedWithNullContent() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be empty"));
    new Builder().setMediumContent(Arrays.asList("Field1", "", "Field3"));
  }

  @Test
  public void testSetLowWithNull() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be null"));
    new Builder().setLowContent(null);
  }

  @Test
  public void testSetLowWithNullContent() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be empty"));
    new Builder().setLowContent(Arrays.asList("Field1", "", "Field3"));
  }

  @Test
  public void testBuildTemplateNoTitle() {
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("title cannot be empty"));
    new Builder()
        .setHighContent(high).setMediumContent(med).setLowContent(low).build();
  }

  @Test
  public void testBuildTemplateNullTitle() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("title cannot be null"));
    new Builder().setTitle(null).build();
  }

  @Test
  public void testBuildTemplateNoContent() {
    String title = "TField";
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("without any content"));
    new Builder().setTitle(title).setUnmappedColumnMode(UnmappedColumnsMode.IGNORE).build();
  }

  @Test
  public void testBuildTemplateEmptyField() {
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "", "HField2"));
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be empty"));
    new Builder().setHighContent(high);
  }

  @Test
  public void testBuildTemplateUsingConfig() {
    Properties config = new Properties();
    config.put("contentTemplate.myTemplate.title", "TField");
    config.put("contentTemplate.myTemplate.quality.high", " HField1 , HField2");
    config.put("contentTemplate.myTemplate.quality.medium", "MField1 ");
    config.put("contentTemplate.myTemplate.quality.low", "LField3,   LField2, LField1");
    setupConfig.initConfig(config);
    ContentTemplate template = ContentTemplate.fromConfiguration("myTemplate");
    StringBuilder target = new StringBuilder();
    target
        .append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>%s</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>MField1:</p>\n  <p>%s</p>\n</div>\n")
        .append("<div id='LField3'>\n  <p>LField3:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>%s</small></p>\n</div>\n")
        .append("%s</body>\n</html>\n");
    assertEquals(target.toString(), template.getTemplate());
  }

  @Test
  public void getTemplate_fromConfiguration_includeFieldNameTrue() {
    Properties config = new Properties();
    config.setProperty("contentTemplate.myTemplate.title", "TField");
    config.setProperty("contentTemplate.myTemplate.includeFieldName", "true");
    config.setProperty("contentTemplate.myTemplate.quality.high", "HField1");
    setupConfig.initConfig(config);
    ContentTemplate template = ContentTemplate.fromConfiguration("myTemplate");
    StringBuilder target = new StringBuilder();
    target
        .append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>%s</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>%s</h1>\n</div>\n")
        .append("%s</body>\n</html>\n");
    assertEquals(target.toString(), template.getTemplate());
  }

  @Test
  public void getTemplate_fromConfiguration_includeFieldNameFalse() {
    Properties config = new Properties();
    config.setProperty("contentTemplate.myTemplate.title", "TField");
    config.setProperty("contentTemplate.myTemplate.includeFieldName", "false");
    config.setProperty("contentTemplate.myTemplate.quality.high", "HField1");
    setupConfig.initConfig(config);
    ContentTemplate template = ContentTemplate.fromConfiguration("myTemplate");
    StringBuilder target = new StringBuilder();
    target
        .append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>%s</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <h1>%s</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <h1>%s</h1>\n</div>\n")
        .append("%s</body>\n</html>\n");
    assertEquals(target.toString(), template.getTemplate());
  }

  @Test
  public void apply_fromConfiguration_unmappedColumnsAppend() {
    Properties config = new Properties();
    config.setProperty("contentTemplate.myTemplate.title", "TField");
    config.setProperty("contentTemplate.myTemplate.unmappedColumnsMode", "APPEND");
    setupConfig.initConfig(config);
    ContentTemplate template = ContentTemplate.fromConfiguration("myTemplate");
    Multimap<String, Object> keyVals = LinkedListMultimap.create();
    keyVals.put("TField", "My Title");
    keyVals.put("AField1", "A1Value");
    StringBuilder target = new StringBuilder();
    target.append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>My Title</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>My Title</h1>\n</div>\n")
        .append("<div id='AField1'>\n  <p>AField1:</p>\n  <p><small>A1Value</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.apply(keyVals));
  }

  @Test
  public void apply_fromConfiguration_unmappedColumnsIgnore() {
    Properties config = new Properties();
    config.setProperty("contentTemplate.myTemplate.title", "TField");
    config.setProperty("contentTemplate.myTemplate.quality.high", "HField1");
    config.setProperty("contentTemplate.myTemplate.unmappedColumnsMode", "IGNORE");
    setupConfig.initConfig(config);
    ContentTemplate template = ContentTemplate.fromConfiguration("myTemplate");
    Multimap<String, Object> keyVals = LinkedListMultimap.create();
    keyVals.put("TField", "My Title");
    keyVals.put("HField1", "H1Value");
    keyVals.put("AField1", "A1Value");
    StringBuilder target = new StringBuilder();
    target.append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>My Title</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>My Title</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>H1Value</h1>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.apply(keyVals));
  }

  @Test
  public void testBuildTemplateUsingConfigWithEmpty() {
    Properties config = new Properties();
    config.put("contentTemplate.myTemplate.title", "TField");
    config.put("contentTemplate.myTemplate.quality.high", " HField1 , HField2");
    config.put("contentTemplate.myTemplate.quality.medium", "MField1 ");
    config.put("contentTemplate.myTemplate.quality.low", "LField3,  , LField2, LField1");
    setupConfig.initConfig(config);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage(containsString("content fields cannot be empty"));
    ContentTemplate.fromConfiguration("myTemplate");
  }

  @Test
  public void testTemplateApply() {
    // build template
    String title = "TField";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    ContentTemplate template = new Builder().setTitle(title)
        .setHighContent(high).setMediumContent(med).setLowContent(low).build();
    // apply map key/values
    Map<String, Object> keyVals = new HashMap<>();
    keyVals.put("TField", "My Title");
    keyVals.put("HField1", "H1Value");
    keyVals.put("HField2", "H2Value");
    keyVals.put("MField1", "M1Value");
    keyVals.put("LField1", "L1Value");
    keyVals.put("LField2", "L2Value");
    keyVals.put("LField3", "L3Value");
    // build target
    StringBuilder target = new StringBuilder();
    target.append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>My Title</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>My Title</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>H1Value</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1>H2Value</h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>MField1:</p>\n  <p>M1Value</p>\n</div>\n")
        .append("<div id='LField3'>\n  <p>LField3:</p>\n  <p><small>L3Value</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>L2Value</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>L1Value</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.apply(keyVals));
  }

  @Test
  public void testTemplateApplyWithAppends() {
    // build template
    String title = "TField";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    ContentTemplate template = new Builder().setTitle(title)
        .setUnmappedColumnMode(UnmappedColumnsMode.APPEND)
        .setHighContent(high).setMediumContent(med).setLowContent(low).build();
    // apply map key/values
    Multimap<String, Object> keyVals = LinkedListMultimap.create();
    keyVals.put("TField", "My Title");
    keyVals.put("HField1", "H1Value");
    keyVals.put("HField2", "H2Value");
    keyVals.put("MField1", "M1Value");
    keyVals.put("LField1", "L1Value");
    keyVals.put("LField2", "L2Value");
    keyVals.put("LField3", "L3Value");
    keyVals.put("AField1", "A1Value");
    keyVals.put("AField2", "A2Value");
    // build target
    StringBuilder target = new StringBuilder();
    target.append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>My Title</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>My Title</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>H1Value</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1>H2Value</h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>MField1:</p>\n  <p>M1Value</p>\n</div>\n")
        .append("<div id='LField3'>\n  <p>LField3:</p>\n  <p><small>L3Value</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>L2Value</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>L1Value</small></p>\n</div>\n")
        .append("<div id='AField1'>\n  <p>AField1:</p>\n  <p><small>A1Value</small></p>\n</div>\n")
        .append("<div id='AField2'>\n  <p>AField2:</p>\n  <p><small>A2Value</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.apply(keyVals));
  }

  @Test
  public void testTemplateApplyWithAppendsNullValues() {
    // build template
    String title = "TField";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    ContentTemplate template =
        new Builder()
            .setTitle(title)
            .setUnmappedColumnMode(UnmappedColumnsMode.APPEND)
            .setHighContent(high)
            .setMediumContent(med)
            .setLowContent(low)
            .build();
    // apply map key/values
    Multimap<String, Object> keyVals = LinkedListMultimap.create();
    keyVals.put("TField", "My Title");
    keyVals.put("HField1", "H1Value");
    keyVals.put("HField2", "H2Value");
    keyVals.put("MField1", "M1Value");
    keyVals.put("LField1", "L1Value");
    keyVals.put("LField2", "L2Value");
    keyVals.put("LField3", "L3Value");
    keyVals.put("AField1", null);
    keyVals.put("AField2", null);
    keyVals.put("AField2", "A2Value");
    // build target
    StringBuilder target = new StringBuilder();
    target
        .append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>My Title</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>My Title</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>H1Value</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1>H2Value</h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>MField1:</p>\n  <p>M1Value</p>\n</div>\n")
        .append("<div id='LField3'>\n  <p>LField3:</p>\n  <p><small>L3Value</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>L2Value</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>L1Value</small></p>\n</div>\n")
        .append("<div id='AField1'>\n  <p>AField1:</p>\n  <p><small></small></p>\n</div>\n")
        .append("<div id='AField2'>\n  <p>AField2:</p>\n  <p><small>A2Value</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.apply(keyVals));
  }

  @Test
  public void testTemplateApplyMultimap() {
    // build template
    String title = "TField";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    ContentTemplate template =
        new Builder()
            .setTitle(title)
            .setHighContent(high)
            .setMediumContent(med)
            .setLowContent(low)
            .build();
    // apply map key/values
    Multimap<String, Object> keyVals = ArrayListMultimap.create();
    keyVals.put("TField", "My Title");
    keyVals.put("HField1", null);
    keyVals.put("HField1", "H1Value");
    keyVals.put("HField1", "H2Value");
    keyVals.put("HField2", "H2Value");
    keyVals.put("MField1", "M1Value");
    keyVals.put("LField1", "L1Value");
    keyVals.put("LField2", 10);
    keyVals.put("LField3", "L3Value");
    keyVals.put("LField3", true);
    // build target
    StringBuilder target = new StringBuilder();
    target
        .append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>My Title</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>My Title</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>H1Value, H2Value</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1>H2Value</h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>MField1:</p>\n  <p>M1Value</p>\n</div>\n")
        .append(
            "<div id='LField3'>\n  <p>LField3:</p>\n"
                + "  <p><small>L3Value, true</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>10</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>L1Value</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.apply(keyVals));
  }

  @Test
  public void testTemplateApplyWithMissingValues() {
    // build template
    String title = "TField";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    ContentTemplate template = new Builder().setTitle(title)
        .setHighContent(high).setMediumContent(med).setLowContent(low).build();
    // apply map key/values
    Map<String, Object> keyVals = new HashMap<>();
    keyVals.put("TField", "My Title");
    keyVals.put("HField1", "H1Value");
    keyVals.put("LField1", "L1Value");
    keyVals.put("LField2", "L2Value");
    keyVals.put("LField3", "L3Value");
    // build target
    StringBuilder target = new StringBuilder();
    target.append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>My Title</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <p>TField:</p>\n  <h1>My Title</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <p>HField1:</p>\n  <h1>H1Value</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <p>HField2:</p>\n  <h1></h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p>MField1:</p>\n  <p></p>\n</div>\n")
        .append("<div id='LField3'>\n  <p>LField3:</p>\n  <p><small>L3Value</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p>LField2:</p>\n  <p><small>L2Value</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p>LField1:</p>\n  <p><small>L1Value</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.apply(keyVals));
  }

  @Test
  public void testTemplateApplyWithMissingValuesAndNoIncludeFieldNames() {
    // build template
    String title = "TField";
    List<String> high = new ArrayList<>(Arrays.asList("HField1", "HField2"));
    List<String> med = new ArrayList<>(Arrays.asList("MField1"));
    List<String> low = new ArrayList<>(Arrays.asList("LField3", "LField2", "LField1"));
    ContentTemplate template =
        new Builder()
            .setTitle(title)
            .setIncludeFieldName(false)
            .setHighContent(high)
            .setMediumContent(med)
            .setLowContent(low)
            .setUnmappedColumnMode(UnmappedColumnsMode.IGNORE)
            .build();
    // apply map key/values
    Map<String, Object> keyVals = new HashMap<>();
    keyVals.put("TField", "My Title");
    keyVals.put("HField1", "H1Value");
    keyVals.put("LField1", "L1Value");
    keyVals.put("LField2", "L2Value");
    keyVals.put("LField3", "L3Value");
    // build target
    StringBuilder target = new StringBuilder();
    target.append("<!DOCTYPE html>\n<html lang='en'>\n<head>\n")
        .append("<meta http-equiv='Content-Type' content='text/html; charset=utf-8'/>\n")
        .append("<title>My Title</title>\n</head>\n<body>\n")
        .append("<div id='TField'>\n  <h1>My Title</h1>\n</div>\n")
        .append("<div id='HField1'>\n  <h1>H1Value</h1>\n</div>\n")
        .append("<div id='HField2'>\n  <h1></h1>\n</div>\n")
        .append("<div id='MField1'>\n  <p></p>\n</div>\n")
        .append("<div id='LField3'>\n  <p><small>L3Value</small></p>\n</div>\n")
        .append("<div id='LField2'>\n  <p><small>L2Value</small></p>\n</div>\n")
        .append("<div id='LField1'>\n  <p><small>L1Value</small></p>\n</div>\n")
        .append("</body>\n</html>\n");
    assertEquals(target.toString(), template.apply(keyVals));
  }
}
