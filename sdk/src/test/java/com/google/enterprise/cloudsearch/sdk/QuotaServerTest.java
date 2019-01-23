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
package com.google.enterprise.cloudsearch.sdk;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;

import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests for {@link QuotaServer}. */

public class QuotaServerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  private static final double DELTA_FOR_EQUALS = 0.01;

  private enum Operations {
    OP1,
    OP2,
    OP3
  }

  private enum Empty {}

  @Test
  public void testDefaultBuilder() {
    QuotaServer<Operations> qs = new QuotaServer.Builder<>(Operations.class).build();
    assertEquals(QuotaServer.DEFAULT_QPS, qs.getRate(Operations.OP1), DELTA_FOR_EQUALS);
    assertEquals(QuotaServer.DEFAULT_QPS, qs.getRate(Operations.OP2), DELTA_FOR_EQUALS);
    assertEquals(QuotaServer.DEFAULT_QPS, qs.getRate(Operations.OP3), DELTA_FOR_EQUALS);
  }

  @Test
  public void testNonDefaultQpsBuilder() {
    QuotaServer<Operations> qs =
        new QuotaServer.Builder<>(Operations.class).setDefaultQps(4).build();
    assertEquals(4, qs.getRate(Operations.OP1), DELTA_FOR_EQUALS);
    assertEquals(4, qs.getRate(Operations.OP2), DELTA_FOR_EQUALS);
    assertEquals(4, qs.getRate(Operations.OP3), DELTA_FOR_EQUALS);
  }

  @Test
  public void testOverrideDefaultQpsBuilder() {
    QuotaServer<Operations> qs =
        new QuotaServer.Builder<>(Operations.class)
            .addQuota(Operations.OP3, 8)
            .setDefaultQps(4)
            .build();
    assertEquals(4, qs.getRate(Operations.OP1), DELTA_FOR_EQUALS);
    assertEquals(4, qs.getRate(Operations.OP2), DELTA_FOR_EQUALS);
    assertEquals(8, qs.getRate(Operations.OP3), DELTA_FOR_EQUALS);
  }

  @Test
  public void testEmptyEnum() {
    new QuotaServer.Builder<>(Empty.class).build();
  }

  @Test
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testNonEnum() {
    thrown.expect(IllegalArgumentException.class);
    new QuotaServer.Builder(Object.class);
  }

  @Test
  public void testInvalidOperationAcquire() {
    QuotaServer<Operations> qs = new QuotaServer.Builder<>(Operations.class).build();
    thrown.expect(IllegalArgumentException.class);
    qs.acquire(null);
  }

  @Test
  public void testInvalidOperationBuilder() {
    QuotaServer.Builder<Operations, QuotaServer<Operations>> qs =
        new QuotaServer.Builder<>(Operations.class);
    thrown.expect(IllegalArgumentException.class);
    qs.addQuota(null, 5);
  }

  @Test
  public void testInvalidOperationBuilderZeroQps() {
    QuotaServer.Builder<Operations, QuotaServer<Operations>> qs =
        new QuotaServer.Builder<>(Operations.class);
    thrown.expect(IllegalArgumentException.class);
    qs.addQuota(Operations.OP1, 0);
  }

  @Test
  public void testInvalidOperationBuilderNegQps() {
    QuotaServer.Builder<Operations, QuotaServer<Operations>> qs =
        new QuotaServer.Builder<>(Operations.class);
    thrown.expect(IllegalArgumentException.class);
    qs.addQuota(Operations.OP1, -5);
  }

  @Test
  public void testDefaultConfig() {
    setupConfig.initConfig(new Properties());
    QuotaServer<Operations> qs =
        QuotaServer.<Operations>createFromConfiguration("ops", Operations.class);
    checkNotNull(qs);
    assertEquals(QuotaServer.DEFAULT_QPS, qs.getRate(Operations.OP1), DELTA_FOR_EQUALS);
    assertEquals(QuotaServer.DEFAULT_QPS, qs.getRate(Operations.OP2), DELTA_FOR_EQUALS);
    assertEquals(QuotaServer.DEFAULT_QPS, qs.getRate(Operations.OP3), DELTA_FOR_EQUALS);
  }

  @Test
  public void testDefaultQpsFromConfig() {
    Properties properties = new Properties();
    properties.put("quotaServer.ops.defaultQps", "5");
    setupConfig.initConfig(properties);
    QuotaServer<Operations> qs =
        QuotaServer.<Operations>createFromConfiguration("ops", Operations.class);
    checkNotNull(qs);
    assertEquals(5, qs.getRate(Operations.OP1), DELTA_FOR_EQUALS);
    assertEquals(5, qs.getRate(Operations.OP2), DELTA_FOR_EQUALS);
    assertEquals(5, qs.getRate(Operations.OP3), DELTA_FOR_EQUALS);
  }

  @Test
  public void testQpsFromConfig() {
    Properties properties = new Properties();
    properties.put("quotaServer.ops.defaultQps", "5");
    properties.put("quotaServer.ops.OP1", "2");
    properties.put("quotaServer.ops.OP2", "7");
    properties.put("quotaServer.ops.IGNORE", "3");
    setupConfig.initConfig(properties);
    QuotaServer<Operations> qs =
        QuotaServer.<Operations>createFromConfiguration("ops", Operations.class);
    checkNotNull(qs);
    assertEquals(2, qs.getRate(Operations.OP1), DELTA_FOR_EQUALS);
    assertEquals(7, qs.getRate(Operations.OP2), DELTA_FOR_EQUALS);
    assertEquals(5, qs.getRate(Operations.OP3), DELTA_FOR_EQUALS);

    qs.acquire(Operations.OP1);
    qs.acquire(Operations.OP2);
    qs.acquire(Operations.OP3);
  }

  @Test
  public void testInvalidDefaultQpsFromConfig() {
    Properties properties = new Properties();
    properties.put("quotaServer.ops.defaultQps", "5ABCD");
    setupConfig.initConfig(properties);
    thrown.expect(InvalidConfigurationException.class);
    QuotaServer.<Operations>createFromConfiguration("ops", Operations.class);

  }

  @Test
  public void testInvalidQpsFromConfig() {
    Properties properties = new Properties();
    properties.put("quotaServer.ops.OP1", "2");
    properties.put("quotaServer.ops.OP2", "XYZ");
    setupConfig.initConfig(properties);
    thrown.expect(InvalidConfigurationException.class);
    QuotaServer.<Operations>createFromConfiguration("ops", Operations.class);
  }

  @Test
  public void testNegQpsFromConfig() {
    Properties properties = new Properties();
    properties.put("quotaServer.ops.OP1", "-2");
    setupConfig.initConfig(properties);
    thrown.expect(InvalidConfigurationException.class);
    QuotaServer.<Operations>createFromConfiguration("ops", Operations.class);
  }
}
