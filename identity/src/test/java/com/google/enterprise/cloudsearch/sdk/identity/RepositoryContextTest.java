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

import static org.hamcrest.CoreMatchers.anything;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.util.Optional;
import java.util.Properties;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

// TODO(tvartak) : Add more tests
public class RepositoryContextTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Test
  public void testBuilder() {
    IdentitySourceConfiguration sourceConfiguration =
        new IdentitySourceConfiguration.Builder("idsource1")
            .setIdentitySourceSchema("idsource1")
            .setIdentitySourceSchemaAttribute("idsource1_identifier")
            .build();
    RepositoryContext context =
        new RepositoryContext.Builder().setIdentitySourceConfiguration(sourceConfiguration).build();
    assertThat(context.getIdentitySourceConfiguration(), equalTo(sourceConfiguration));
    assertThat(context.getReferenceIdentitySourceConfiguration().keySet(), isEmpty());
  }

  @Test
  public void testWithReferenceConfiguration() {
    IdentitySourceConfiguration sourceConfiguration =
        new IdentitySourceConfiguration.Builder("idsource1")
            .setIdentitySourceSchema("idsource1")
            .setIdentitySourceSchemaAttribute("idsource1_identifier")
            .build();
    IdentitySourceConfiguration referenceConfiguration =
        new IdentitySourceConfiguration.Builder("ref_idsource1")
            .setIdentitySourceSchema("ref_idsource1")
            .setIdentitySourceSchemaAttribute("ref_idsource1_identifier")
            .build();
    RepositoryContext context =
        new RepositoryContext.Builder()
            .setIdentitySourceConfiguration(sourceConfiguration)
            .setReferenceIdentitySourceConfiguration(
                ImmutableMap.of("reference", referenceConfiguration))
            .build();

    RepositoryContext referenceContext =
        new RepositoryContext.Builder()
            .setIdentitySourceConfiguration(referenceConfiguration)
            .build();

    assertThat(context.getIdentitySourceConfiguration(), equalTo(sourceConfiguration));
    assertThat(
        context.getReferenceIdentitySourceConfiguration().keySet(),
        equalTo(ImmutableSet.of("reference")));
    assertThat(
        context.getRepositoryContextForReferenceIdentitySource("reference"),
        equalTo(Optional.of(referenceContext)));
  }

  @Test
  public void testFromConfiguration() {
    Properties properties = new Properties();
    properties.put("api.identitySourceId", "idSource1");
    properties.put("api.referenceIdentitySources", "idSource1,idSource2");
    properties.put("api.referenceIdentitySource.idSource1.id", "refId1");
    properties.put("api.referenceIdentitySource.idSource2.id", "refId2");
    setupConfig.initConfig(properties);
    IdentitySourceConfiguration sourceConfiguration =
        new IdentitySourceConfiguration.Builder("idSource1").build();
    IdentitySourceConfiguration refConfiguration1 =
        new IdentitySourceConfiguration.Builder("refId1").build();
    IdentitySourceConfiguration refConfiguration2 =
        new IdentitySourceConfiguration.Builder("refId2").build();

    RepositoryContext context = RepositoryContext.fromConfiguration();
    assertThat(context.getIdentitySourceConfiguration(), equalTo(sourceConfiguration));
    assertThat(
        context.getReferenceIdentitySourceConfiguration(),
        equalTo(ImmutableMap.of("idSource1", refConfiguration1, "idSource2", refConfiguration2)));
  }

  @Test
  public void testBuildGroupKey() {
    IdentitySourceConfiguration sourceConfiguration =
        new IdentitySourceConfiguration.Builder("idsource1")
            .setIdentitySourceSchema("idsource1")
            .setIdentitySourceSchemaAttribute("idsource1_identifier")
            .build();
    RepositoryContext context =
        new RepositoryContext.Builder().setIdentitySourceConfiguration(sourceConfiguration).build();
    assertThat(
        context.buildEntityKeyForGroup("group1"),
        equalTo(new EntityKey().setId("group1").setNamespace("identitysources/idsource1")));
  }

  @Test
  public void testBuildGroupKeyWithEncoding() {
    IdentitySourceConfiguration sourceConfiguration =
        new IdentitySourceConfiguration.Builder("idsource1")
            .setIdentitySourceSchema("idsource1")
            .setIdentitySourceSchemaAttribute("idsource1_identifier")
            .build();
    RepositoryContext context =
        new RepositoryContext.Builder().setIdentitySourceConfiguration(sourceConfiguration).build();
    assertThat(
        context.buildEntityKeyForGroup("group 1"),
        equalTo(new EntityKey().setId("group%201").setNamespace("identitysources/idsource1")));
  }

  private static <T> Matcher<Iterable<? super T>> isEmpty() {
    return not(hasItem(anything()));
  }
}
