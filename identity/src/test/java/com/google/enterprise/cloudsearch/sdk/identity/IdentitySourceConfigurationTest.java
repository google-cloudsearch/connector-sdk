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
package com.google.enterprise.cloudsearch.sdk.identity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class IdentitySourceConfigurationTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Test
  public void testBuilder() {
    IdentitySourceConfiguration idSourceConfig =
        new IdentitySourceConfiguration.Builder("idSource1").build();
    assertNotNull(idSourceConfig);
    assertEquals("idSource1", idSourceConfig.getIdentitySourceId());
    assertEquals("idSource1", idSourceConfig.getIdentitySourceSchema());
    assertEquals("idSource1_identifier", idSourceConfig.getIdentitySourceSchemaAttribute());
    assertEquals("identitysources/idSource1", idSourceConfig.getGroupNamespace());
  }

  @Test
  public void testNullIdentitySourceIdBuilder() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Identity Source ID can not be null or empty");
    new IdentitySourceConfiguration.Builder(null).build();
  }

  @Test
  public void testNullIdentitySourceSchemaBuilder() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Identity Source schema can not be null or empty");
    new IdentitySourceConfiguration.Builder("id1").setIdentitySourceSchema(null).build();
  }

  @Test
  public void testNullIdentitySourceSchemaAttributeBuilder() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Identity Source schema attribute can not be null or empty");
    new IdentitySourceConfiguration.Builder("id1").setIdentitySourceSchemaAttribute(null).build();
  }

  @Test
  public void testFromConfiguration() {
    Properties properties = new Properties();
    properties.put("api.identitySourceId", "idSource1");
    setupConfig.initConfig(properties);

    IdentitySourceConfiguration idSourceConfig = IdentitySourceConfiguration.fromConfiguration();
    assertNotNull(idSourceConfig);
    assertEquals("idSource1", idSourceConfig.getIdentitySourceId());
    assertEquals("idSource1", idSourceConfig.getIdentitySourceSchema());
    assertEquals("idSource1_identifier", idSourceConfig.getIdentitySourceSchemaAttribute());
    assertEquals("identitysources/idSource1", idSourceConfig.getGroupNamespace());
  }

  @Test
  public void testFromConfigurationMissingConfig() {
    setupConfig.initConfig(new Properties() /* empty configuration */);

    thrown.expect(InvalidConfigurationException.class);
    IdentitySourceConfiguration.fromConfiguration();

  }

  @Test
  public void testFromConfigurationOverrideSchameAndAttribute() {
    Properties properties = new Properties();
    properties.put("api.identitySourceId", "idSource1");
    properties.put("api.identitySourceSchema", "idSource1_schema");
    properties.put("api.identitySourceSchemaAttribute", "idSource1_attrib");
    setupConfig.initConfig(properties);

    IdentitySourceConfiguration idSourceConfig = IdentitySourceConfiguration.fromConfiguration();
    assertNotNull(idSourceConfig);
    assertEquals("idSource1", idSourceConfig.getIdentitySourceId());
    assertEquals("idSource1_schema", idSourceConfig.getIdentitySourceSchema());
    assertEquals("idSource1_attrib", idSourceConfig.getIdentitySourceSchemaAttribute());
    assertEquals("identitysources/idSource1", idSourceConfig.getGroupNamespace());
  }

  @Test
  public void testReferenceIdentitySourcesFromConfiguration() {
    Properties properties = new Properties();
    properties.put("api.referenceIdentitySources", "idSource1,idSource2");
    properties.put("api.referenceIdentitySource.idSource1.id", "id1");
    properties.put("api.referenceIdentitySource.idSource2.id", "id2");
    setupConfig.initConfig(properties);

    IdentitySourceConfiguration config1 = new IdentitySourceConfiguration.Builder("id1").build();
    IdentitySourceConfiguration config2 = new IdentitySourceConfiguration.Builder("id2").build();
    ImmutableMap<String, IdentitySourceConfiguration> expected =
        ImmutableMap.of("idSource1", config1, "idSource2", config2);

    ImmutableMap<String, IdentitySourceConfiguration> actual =
        IdentitySourceConfiguration.getReferenceIdentitySourcesFromConfiguration();

    assertEquals(expected, actual);
  }
}
