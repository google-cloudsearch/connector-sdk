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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.google.enterprise.cloudsearch.sdk.config.SensitiveDataCodec.SecurityLevel;
import com.google.enterprise.cloudsearch.sdk.config.SensitiveDataCodec.SensitiveDataCodecHelper;
import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link SensitiveDataCodec}. */
@RunWith(MockitoJUnitRunner.class)
public class SensitiveDataCodecTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock SensitiveDataCodecHelper sensitiveDataCodecHelperMock;

  @Test
  public void testConstructorWithHelper() {
    MockitoAnnotations.initMocks(this);
    new SensitiveDataCodec(sensitiveDataCodecHelperMock);
  }

  @Test
  public void testPlainTextEncode() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    String readable = "obf:testPlainText";
    String encrypt = sensitiveDataCodec.encodeData(readable, SecurityLevel.PLAIN_TEXT);
    assertEquals("pl:obf:testPlainText", encrypt);
  }

  @Test
  public void testEncodeWithNullInputString() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("input message cannot be null");
    sensitiveDataCodec.encodeData(null, SecurityLevel.PLAIN_TEXT);
  }

  @Test
  public void testEncodeWithNullSecurityLevel() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("security level cannot be null");
    sensitiveDataCodec.encodeData("testtest", null);
  }

  @Test
  public void testDecodeWithNullString() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("encrypted message cannot be null");
    sensitiveDataCodec.decodeData(null);
  }

  @Test
  public void testPlainTextDecode() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    String unreadable = "pl:obf:testPlainText";
    String decrypt = sensitiveDataCodec.decodeData(unreadable);
    assertEquals("obf:testPlainText", decrypt);
  }

  @Test
  public void testPlainTextDecodeWithNoPrefix() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    String unreadable = "testPlainText";
    String decrypt = sensitiveDataCodec.decodeData(unreadable);
    assertEquals(unreadable, decrypt);
  }

  @Test
  public void testObfEncodeDecode() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    String readable = "testObfEn&c$o#de";
    String encrypt = sensitiveDataCodec.encodeData(readable, SecurityLevel.OBFUSCATED);
    String decrypt = sensitiveDataCodec.decodeData(encrypt);
    assertEquals(readable, decrypt);
    assertThat(encrypt, startsWith("obf:"));
  }

  @Test
  public void testLongValueEncodeDecode() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    byte[] bytes = new byte[128];
    Arrays.fill(bytes, (byte) 'a');
    String readable = new String(bytes, UTF_8);
    String encrypt = sensitiveDataCodec.encodeData(readable, SecurityLevel.OBFUSCATED);
    assertThat(encrypt, not(containsString("\r\n")));
    String decrypt = sensitiveDataCodec.decodeData(encrypt);
    assertEquals(readable, decrypt);
    assertThat(encrypt, startsWith("obf:"));
  }

  @Test
  public void testKeyEncodeDecodeNullKey() throws IOException {
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    String readable = "testObfEn&c$o#de";
    thrown.expect(NullPointerException.class);
    sensitiveDataCodec.encodeData(readable, SecurityLevel.ENCRYPTED);
  }

  @Test
  public void testKeyEncodeDecode() throws IOException {
    MockitoAnnotations.initMocks(this);
    KeyPair keyPair;
    try {
      KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
      SecureRandom random = SecureRandom.getInstance("SHA1PRNG", "SUN");
      keyPairGenerator.initialize(2048, random);
      keyPair = keyPairGenerator.generateKeyPair();
    } catch (NoSuchAlgorithmException | NoSuchProviderException e) {
      throw new IOException(e);
    }

    when(sensitiveDataCodecHelperMock.getKeyPair()).thenReturn(keyPair);
    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec(sensitiveDataCodecHelperMock);
    String readable = "testObfEn&c$o#de";
    String encrypt = sensitiveDataCodec.encodeData(readable, SecurityLevel.ENCRYPTED);
    String decrypt = sensitiveDataCodec.decodeData(encrypt);
    assertEquals(readable, decrypt);
    assertThat(encrypt, startsWith("pkc:"));
  }
}
