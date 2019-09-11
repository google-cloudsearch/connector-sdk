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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import java.io.BufferedReader;
import java.io.Console;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.function.Function;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Helper object used to encode and decode sensitive data.
 *
 * <p>There are three levels of {@link SecurityLevel}, PLAIN_TEXT, OBFUSCATED and ENCRYPTED. To
 * facilitate decoding a message, first use <code>getSecurityLevel(String encrypted_message)</code>
 * to get the securityLevel. If the prefix of the encrypted_message doesn't match a valid security
 * level, the decoder uses PLAIN_TEXT by default.
 *
 * <p>To decrypt the message, use <code>decodeData(String encrypted)</code>.
 */
public class SensitiveDataCodec {

  private final SensitiveDataCodecHelper sensitiveDataCodecHelper;
  private static final Charset CHARSET = Charset.forName("UTF-8");
  private static final SecretKey OBFUSCATING_KEY =
      new SecretKeySpec(
          new byte[] {
            (byte) 0x7d, (byte) 0xec, (byte) 0xbd, (byte) 0x31, (byte) 0x4e,
            (byte) 0xf3, (byte) 0x68, (byte) 0x69, (byte) 0x69, (byte) 0x78,
            (byte) 0x7a, (byte) 0xc9, (byte) 0xfc, (byte) 0x99, (byte) 0x07,
            (byte) 0x9c
          },
          "AES");

  public SensitiveDataCodec() {
    this(new SensitiveDataCodecHelper());
  }

  @VisibleForTesting
  SensitiveDataCodec(SensitiveDataCodecHelper sensitiveDataCodecHelper) {
    this.sensitiveDataCodecHelper = checkNotNull(sensitiveDataCodecHelper);
  }

  /**
   * Values that specify the security level, used typically for a user parameter.
   */
  public enum SecurityLevel {
    /**
     * The value is prefixed with "pl:" and is not encrypted.
     */
    PLAIN_TEXT("pl:"),
    /**
     * The value is prefixed with "obf:" and is obfuscated, but no real security is added. AES is
     * used to encrypt the value.
     */
    OBFUSCATED("obf:"),
    /**
     * The value is prefixed with "pkc:" and is encrypted using the publock key crytography provided
     * to the constructor.
     */
    ENCRYPTED("pkc:");

    private final String prefix;

    SecurityLevel(String prefix) {
      this.prefix = prefix;
    }

    String getPrefix() {
      return prefix;
    }
  }

  /**
   * Encodes the text based on {@code securityLevel}.
   *
   * @param readable original text
   * @param securityLevel one of three different security levels: PLAIN_TEXT, OBFUSCATED and
   *     ENCRYPTED using public/private key pair
   * @return encrypted text
   */
  public String encodeData(String readable, SecurityLevel securityLevel) throws IOException {
    checkNotNull(readable, "input message cannot be null");
    checkNotNull(securityLevel, "security level cannot be null");
    StringBuilder res = new StringBuilder();
    res.append(securityLevel.getPrefix());
    switch (securityLevel) {
      case PLAIN_TEXT:
        res.append(readable);
        break;
      case OBFUSCATED:
        res.append(encryptAndBase64(readable, createObfuscatingCipher(), OBFUSCATING_KEY));
        break;
      case ENCRYPTED:
        //use public key to encrypt data
        KeyPair keyPair = sensitiveDataCodecHelper.getKeyPair();
        res.append(
            encryptAndBase64(readable, createEncryptingCipher(keyPair), keyPair.getPublic()));
        break;
      default:
        throw new IOException("Invalid securityLevel " + securityLevel.getPrefix());
    }
    return res.toString();
  }

  /**
   * Decrypts the encoded text.
   *
   * @param encrypted the encrypted string
   * @return the decrypted string
   */
  public String decodeData(String encrypted) throws InvalidConfigurationException, IOException {
    checkNotNull(encrypted, "encrypted message cannot be null");
    SecurityLevel securityLevel = getSecurityLevel(encrypted);
    if (encrypted.startsWith(securityLevel.getPrefix())) {
      encrypted = encrypted.substring(securityLevel.getPrefix().length());
    }
    switch (securityLevel) {
      case PLAIN_TEXT:
        return encrypted;
      case OBFUSCATED:
        return decryptAndBase64(encrypted, createObfuscatingCipher(), OBFUSCATING_KEY);
      case ENCRYPTED:
        KeyPair keyPair = sensitiveDataCodecHelper.getKeyPair();
        return decryptAndBase64(
            encrypted, createEncryptingCipher(keyPair), keyPair.getPrivate());
      default:
        throw new InvalidConfigurationException("Invalid SecurityLevel " + securityLevel);
    }
  }

  private String decryptAndBase64(String unreadable, Cipher cipher, Key key)
      throws InvalidConfigurationException {
    try {
      cipher.init(Cipher.DECRYPT_MODE, key);
    } catch (InvalidKeyException e) {
      throw new InvalidConfigurationException(e);
    }

    try {
      byte[] outputBytes = cipher.doFinal(Base64.getDecoder().decode(unreadable));
      return new String(outputBytes, CHARSET);
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new InvalidConfigurationException(e);
    }
  }

  /**
   * Determines the security level based on {@code unreadable}.
   *
   * <p>If {@code unreadable} does not start with a prefix, treat it as plain text.
   *
   * @param unreadable the encrypted string
   * @return security level
   */
  private SecurityLevel getSecurityLevel(String unreadable) {
    checkNotNull(unreadable, "input encrypted message cannot be null");
    for (SecurityLevel testLevel : SecurityLevel.values()) {
      if (unreadable.startsWith(testLevel.getPrefix())) {
        return testLevel;
      }
    }
    return SecurityLevel.PLAIN_TEXT;
  }

  private String encryptAndBase64(String readable, Cipher cipher, Key key)
      throws InvalidConfigurationException {
    byte[] bytes = readable.getBytes(CHARSET);

    try {
      cipher.init(Cipher.ENCRYPT_MODE, key);
    } catch (InvalidKeyException e) {
      throw new InvalidConfigurationException(e);
    }

    try {
      bytes = cipher.doFinal(bytes);
    } catch (IllegalBlockSizeException | BadPaddingException e) {
      throw new InvalidConfigurationException(e);
    }

    return Base64.getEncoder().encodeToString(bytes);
  }

  private Cipher createObfuscatingCipher() throws IOException {
    try {
      return Cipher.getInstance(OBFUSCATING_KEY.getAlgorithm());
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new IOException(e);
    }
  }

  private Cipher createEncryptingCipher(KeyPair keyPair) throws IOException {
    try {
      return Cipher.getInstance(keyPair.getPublic().getAlgorithm());
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new IOException(e);
    }
  }

  private static boolean isParameterSet(String[] args, String parameterName) {
    for (String arg : args) {
      if (arg.equals(parameterName)) {
        return true;
      }
    }
    return false;
  }

  // this handles keypair loading
  static class SensitiveDataCodecHelper {

    private KeyPair keyPair = null;

    KeyPair getKeyPair() throws IOException {
      if (keyPair == null) {
        keyPair = KeyPairManager.getKeyPair(System.getProperty("alias"));
      }
      return keyPair;
    }
  }

  /**
   * Encodes and decodes sensitive data.
   *
   * <p>Example usage to get encoded sensitive data value from the command line:
   *
   * <pre>
   * java -DsecurityLevel=ENCRYPTED \
   * -Djavax.net.ssl.keyStore=encryptKeyStore.jks \
   * -Djavax.net.ssl.keyStorePassword=testtest \
   * -Djavax.net.ssl.keyStoreType=JKS \
   * -Dalias=testkeypair \
   * -classpath 'google-cloudsearch-connector-sdk-{version}.jar' \
   * com.google.enterprise.cloudsearch.sdk.config.SensitiveDataCodec
   * </pre>
   *
   * <p>To enable reading the sensitive value from standard input, add a quiet parameter
   * ("--quiet"). If the quiet parameter is present, the program outputs the encoded sensitive value
   * without any additional text.
   */
  public static void main(String[] args) throws IOException {
    Function<String, char[]> readPassword;
    Console console = System.console();
    if (console == null) {
      readPassword = null;
    } else {
      readPassword = console::readPassword;
    }
    String result = mainHelper(args, System.in, readPassword);
    if (result != null) {
      System.out.println(result);
    }
  }

  @VisibleForTesting
  static String mainHelper(String[] args, InputStream in, Function<String, char[]> readPassword)
      throws IOException {
    String securityLevelStr = System.getProperty("securityLevel");
    SecurityLevel securityLevel;
    try {
      securityLevel = SecurityLevel.valueOf(securityLevelStr);
    } catch (IllegalArgumentException e) {
      throw new IOException("Security level input is not valid: " + securityLevelStr
          + ". Please use PLAIN_TEXT, OBFUSCATED or ENCRYPTED");
    } catch (NullPointerException e) {
      throw new IOException(
          "Security level is not specified. Please use PLAIN_TEXT, OBFUSCATED or ENCRYPTED");
    }

    SensitiveDataCodec sensitiveDataCodec = new SensitiveDataCodec();
    boolean isQuiet = isParameterSet(args, "--quiet");
    if (isQuiet) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(in));
      return sensitiveDataCodec.encodeData(reader.readLine(), securityLevel);
    } else {
      if (readPassword == null) {
        return null;
      }
      char[] valueToEncode = readPassword.apply("Sensitive Value: ");
      String encodedValue = sensitiveDataCodec.encodeData(new String(valueToEncode), securityLevel);
      return "Encoded value is: " + encodedValue;
    }
  }
}
