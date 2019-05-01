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

import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;

import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.Parser;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.SensitiveDataCodec.SecurityLevel;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link Configuration}. */
@RunWith(MockitoJUnitRunner.class)
public class ConfigurationTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testUninitializedGetConfig() {
    thrown.expect(IllegalStateException.class);
    Configuration.getConfig();
  }

  @Test
  public void testGetConfig() {
    Properties config = new Properties();
    config.put("config.key", "configured value");
    Configuration.initConfig(config);
    assertEquals(config, Configuration.getConfig());
  }

  @Test
  public void initConfig_callTwiceWithDifferentProperties_throwsException() {
    Properties config = new Properties();
    config.setProperty("name", "value");
    Properties config2 = new Properties();
    config2.setProperty("name", "value2");
    config2.setProperty("foo", "bar");
    thrown.expect(IllegalStateException.class);
    Configuration.initConfig(config);
    Configuration.initConfig(config2);
  }

  @Test
  public void initConfig_callTwiceWithSameProperties_succeeds() {
    Properties config1 = new Properties();
    config1.setProperty("foo", "bar");

    Configuration.initConfig(config1);
    Configuration.initConfig(config1);
  }

  @Test
  public void initConfig_callTwiceWithEqualProperties_succeeds() {
    Properties config1 = new Properties();
    config1.setProperty("foo", "bar");
    Properties config2 = new Properties();
    config2.setProperty("foo", "bar");

    Configuration.initConfig(config1);
    Configuration.initConfig(config2);
  }

  @Test
  public void initConfig_callTwiceWithEqualPropertiesWithDefaults_succeeds() {
    Properties defaults = new Properties();
    defaults.setProperty("foo", "bar");
    Properties config1 = new Properties(defaults);
    Properties config2 = new Properties();
    config2.setProperty("foo", "bar");

    Configuration.initConfig(config1);
    Configuration.initConfig(config2);
  }

  @Test
  public void testBooleanConfigFailesWithoutInitConfig() {
    ConfigValue<Boolean> booleanParam = Configuration.getBoolean("config.key", true);
    thrown.expect(IllegalStateException.class);
    booleanParam.get();
  }

  @Test
  public void testBooleanConfigDefaultValue() {
    ConfigValue<Boolean> booleanParam = Configuration.getBoolean("config.key", true);
    Configuration.initConfig(new Properties());
    assertTrue(booleanParam.get());
  }

  @Test
  public void testBooleanConfigOverrideDefaultValue() {
    ConfigValue<Boolean> booleanParam = Configuration.getBoolean("config.key", true);
    assertFalse(booleanParam.isInitialized());
    assertEquals(true, booleanParam.getDefault());
    Properties config = new Properties();
    config.put("config.key", "false");
    Configuration.initConfig(config);
    assertFalse(booleanParam.get());
  }

  @Test
  public void testBooleanConfigIgnoreCase() {
    ConfigValue<Boolean> booleanParam = Configuration.getBoolean("config.False", true);
    assertFalse(booleanParam.isInitialized());
    assertEquals(true, booleanParam.getDefault());
    Properties config = new Properties();
    config.put("config.False", "False");
    config.put("config.True", "TRUE");
    Configuration.initConfig(config);
    assertFalse(booleanParam.get());
    assertTrue(Configuration.getBoolean("config.True", false).get());
  }

  @Test
  public void testBooleanConfigParseError() {
    ConfigValue<Boolean> booleanParam = Configuration.getBoolean("config.key", true);
    assertFalse(booleanParam.isInitialized());
    assertEquals(true, booleanParam.getDefault());
    Properties config = new Properties();
    config.put("config.key", "other");
    thrown.expect(InvalidConfigurationException.class);
    Configuration.initConfig(config);
  }

  @Test
  public void testBooleanConfigParseErrorAfterInit() {
    Properties config = new Properties();
    config.put("config.key", "other");
    Configuration.initConfig(config);
    assertTrue(Configuration.isInitialized());
    thrown.expect(InvalidConfigurationException.class);
    Configuration.getBoolean("config.key", true);
  }

  @Test
  public void testStringConfigOverridenDefaultValue() {
    String expected = "non default value";
    Properties config = new Properties();
    config.put("config.key", "non default value");
    Configuration.initConfig(config);
    ConfigValue<String> stringParam = Configuration.getString("config.key", "default value");
    assertEquals(expected, stringParam.get());
  }

  @Test
  public void testStringConfigOverridenDefaultValueWithFallback() {
    String expected = "overriden default value";
    Properties config = new Properties();
    config.put("config.key", "non default value");
    config.put("overriden.config.key", "overriden default value");
    Configuration.initConfig(config);
    ConfigValue<String> defaultValue = Configuration.getString("config.key", "default value");
    ConfigValue<String> stringParam =
        Configuration.getOverriden("overriden.config.key", defaultValue);
    assertEquals(expected, stringParam.get());
  }

  @Test
  public void testIntegerConfigValue() {
    Integer expected = 100;
    Properties config = new Properties();
    config.put("config.key", "100");
    Configuration.initConfig(config);
    ConfigValue<Integer> intParam = Configuration.getInteger("config.key", 10);
    assertEquals(expected, intParam.get());
  }

  @Test
  public void testStringConfigValueDefault() {
    String expected = "default value";
    Configuration.initConfig(new Properties());
    ConfigValue<String> stringParam = Configuration.getString("config.key", "default value");
    assertEquals(expected, stringParam.get());
  }

  @Test
  public void testMissingRequiredValue() {
    Configuration.initConfig(new Properties());
    thrown.expect(InvalidConfigurationException.class);
    Configuration.getString("config.key", null);
  }

  @Test
  public void testMissingRequiredWithIncompletePropertyValue() {
    Properties config = new Properties();
    config.put("config.key", "");
    Configuration.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    Configuration.getString("config.key", null);
  }

  @Test
  public void testIntegerConfigValueParseException() {
    Properties config = new Properties();
    config.put("config.key", "100ABC");
    Configuration.initConfig(config);
    thrown.expect(InvalidConfigurationException.class);
    Configuration.getInteger("config.key", 10);
  }

  @Test
  public void testMultiValue() {
    List<String> expected = Arrays.asList("ABC", "", "PQR");
    Properties config = new Properties();
    config.put("config.key", "ABC,,PQR");
    Configuration.initConfig(config);
    ConfigValue<List<String>> multiValue =
        Configuration.getMultiValue(
            "config.key", Collections.emptyList(), Configuration.STRING_PARSER);
    assertEquals(expected, multiValue.get());
  }

  @Test
  public void testMultiValue_emptyString() {
    List<String> expected = Collections.singletonList("default");
    Properties config = new Properties();
    config.put("config.key", "");
    Configuration.initConfig(config);
    ConfigValue<List<String>> multiValue =
        Configuration.getMultiValue(
            "config.key", Collections.singletonList("default"), Configuration.STRING_PARSER);
    assertEquals(expected, multiValue.get());
  }

  @Test
  public void testMultiValue_delimiter() {
    List<String> expected = Arrays.asList("ABC", "", "PQR");
    Properties config = new Properties();
    config.put("config.key", "ABC//PQR");
    Configuration.initConfig(config);
    ConfigValue<List<String>> multiValue =
        Configuration.getMultiValue(
            "config.key", Collections.emptyList(), Configuration.STRING_PARSER, "/");
    assertEquals(expected, multiValue.get());
  }

  @Test
  public void testCustomParser() throws MalformedURLException {
    Properties config = new Properties();
    config.put("config.key", "http://www.google.com");
    Configuration.initConfig(config);
    Parser<URL> urlParser =
        value -> {
          try {
            return new URL(value);
          } catch (MalformedURLException e) {
            throw new InvalidConfigurationException(e);
          }
        };
    ConfigValue<URL> configuredUrl = Configuration.getValue("config.key", null, urlParser);
    URL expected = new URL("http://www.google.com");
    assertEquals(expected, configuredUrl.get());
  }

  @Test
  public void testParseError() {
    Properties config = new Properties();
    config.put("config.key", "abc");
    config.put("config.valid.key", "valid");
    @SuppressWarnings("unchecked")
    Parser<String> mockParser = Mockito.mock(Parser.class);
    ConfigValue<String> stringParam =
        Configuration.getValue("config.valid.key", "some", mockParser);
    ConfigValue<Integer> intParam = Configuration.getInteger("config.key", 10);
    try {
      Configuration.initConfig(config);
      fail("Missing InvalidConfigurationException");
    } catch (InvalidConfigurationException expected) {
    }
    assertFalse(Configuration.isInitialized());
    assertFalse(stringParam.isInitialized());
    assertFalse(intParam.isInitialized());
    verify(mockParser).parse("valid");
  }

  @Test
  public void testInitConfigWithStringNoKey() throws IOException {
    File tmpfile = temporaryFolder.newFile("test.properties");
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {"-Dconfig=test.properties", tmpfilePath};
    Configuration.initConfig(args);
  }

  @Test
  public void testInitConfigWithStringNoPrefix() throws IOException {
    File tmpfile = temporaryFolder.newFile("testplain.properties");
    createFile(tmpfile, ISO_8859_1, "test.name=testencoding/4245!");
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {"-Dconfig=test.properties", tmpfilePath};
    Configuration.initConfig(args);
    String decodeValue = Configuration.getString("test.name", null).get();
    assertEquals("testencoding/4245!", decodeValue);
  }

  @Test
  public void testInitConfigWithStringPlainText() throws IOException {
    File tmpfile = temporaryFolder.newFile("testplain.properties");
    createFile(tmpfile, ISO_8859_1, "test.name=pl:testencoding/4245!");
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {"-Dconfig=test.properties", tmpfilePath};
    Configuration.initConfig(args);
    String decodeValue = Configuration.getString("test.name", null).get();
    assertEquals("testencoding/4245!", decodeValue);
  }

  @Test
  public void initConfig_utf8File_readsUtf8BytesAsLatin1() throws IOException {
    File tmpfile = temporaryFolder.newFile("testplain.properties");
    createFile(tmpfile, UTF_8, "test.name=\u20AC3.00");
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {"-Dconfig=test.properties", tmpfilePath};
    Configuration.initConfig(args);
    String value = Configuration.getString("test.name", null).get();
    assertNotEquals("\u20AC3.00", value);
    assertEquals(new String("\u20AC3.00".getBytes(UTF_8), ISO_8859_1), value);
  }

  @Test
  public void testTrimPropertiesFile() throws IOException {
    File tmpfile = temporaryFolder.newFile("testplain.properties");
    createFile(tmpfile, ISO_8859_1, "test.name=\\ other string \\");
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {"-Dconfig=test.properties", tmpfilePath};
    Configuration.initConfig(args);
    String decodeValue = Configuration.getString("test.name", null).get();
    assertEquals("other string", decodeValue);
  }

  @Test
  public void testTrimValues() {
    Properties config = new Properties();
    config.put("config.integer", "1 ");
    config.put("config.string", "string ");
    config.put("config.stringother", " other string ");
    config.put("config.boolean", "True ");
    Configuration.initConfig(config);
    assertEquals((Integer) 1, Configuration.getInteger("config.integer", null).get());
    assertEquals("string", Configuration.getString("config.string", null).get());
    assertEquals("other string", Configuration.getString("config.stringother", null).get());
    assertTrue(Configuration.getBoolean("config.boolean", null).get());
  }

  @Test
  public void testInitConfigWithStringObf() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    String readable = "testencoding/4245!";
    String encoded = sensitiveDataCodec.encodeData(readable, SecurityLevel.OBFUSCATED);
    File tmpfile = temporaryFolder.newFile("testObf.properties");
    createFile(tmpfile, ISO_8859_1, "test.name=" + encoded);
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {"-Dconfig=test.properties", tmpfilePath};
    Configuration.initConfig(args);
    String decodeValue = Configuration.getString("test.name", null).get();
    assertEquals(readable, decodeValue);
  }

  @Test
  public void testInitConfigWithStringObfTrim() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    String readable = "testencoding/4245!";
    String encoded = sensitiveDataCodec.encodeData(readable, SecurityLevel.OBFUSCATED);
    File tmpfile = temporaryFolder.newFile("testObf.properties");
    createFile(tmpfile, ISO_8859_1, "test.name=" + encoded + " ");
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {"-Dconfig=test.properties", tmpfilePath};
    Configuration.initConfig(args);
    String decodeValue = Configuration.getString("test.name", null).get();
    assertEquals(readable, decodeValue);
  }

  @Test
  public void testSetupConfigRule() {
    Properties config = new Properties();
    config.put("config.key", "42");
    setupConfig.initConfig(config);
    assertEquals(config, Configuration.getConfig());
    assertEquals(Integer.valueOf(42), Configuration.getInteger("config.key", null).get());
  }

  @Test
  public void testSetupConfigRule_nonStrings() {
    Properties config = new Properties();
    config.put("config.key", Integer.valueOf(42));
    thrown.expect(IllegalArgumentException.class);
    setupConfig.initConfig(config);
  }

  @Test
  public void testInitConfigWithStringOverride() throws IOException {
    File tmpfile = temporaryFolder.newFile("testconfig.properties");
    createFile(tmpfile, ISO_8859_1, "test.param = fileValue");
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {tmpfilePath, "-Dtest.param=commandLineValue"};
    Configuration.initConfig(args);
    String value = Configuration.getString("test.param", null).get();
    assertEquals("commandLineValue", value);
  }

  @Test
  public void testInitConfigWithStringEncoded() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    String readable = "testencoding/4245!";
    String encoded = sensitiveDataCodec.encodeData(readable, SecurityLevel.OBFUSCATED);
    String[] args = {"-Dtest.param=" + encoded};
    Configuration.initConfig(args);
    String value = Configuration.getString("test.param", null).get();
    assertEquals(readable, value);
  }

  @Test
  public void testInitConfigWithStringNoDash() throws IOException {
    String[] args = {"test.param=foo"};
    Configuration.initConfig(args);
    thrown.expect(InvalidConfigurationException.class);
    Configuration.getString("test.param", null).get();
  }

  @Test
  public void initConfig_commandLineCalledTwice_succeeds() throws IOException {
    File tmpfile = temporaryFolder.newFile("testconfig.properties");
    createFile(tmpfile, UTF_8, "test.param = fileValue");
    String tmpfilePath = "-Dconfig=" + tmpfile.getAbsolutePath();
    String[] args = {tmpfilePath, "-Dtest.param=commandLineValue"};
    Configuration.initConfig(args);
    Configuration.initConfig(args);
    String value = Configuration.getString("test.param", null).get();
    assertEquals("commandLineValue", value);
  }

  private void createFile(File file, Charset charset, String content) throws IOException {
    try (OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(file), charset)) {
      out.write(content);
    }
  }
}
