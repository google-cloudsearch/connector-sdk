/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.enterprise.cloudsearch.sdk.config;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link KeyPairManager}. */
@RunWith(MockitoJUnitRunner.class)
public class KeyPairManagerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void nullAlias_throwsException() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("alias cannot be null");
    KeyPairManager.getKeyPair(null);
  }

  @Test
  public void missingKeyStoreFileProperty_throwsException() throws Exception {
    thrown.expect(IOException.class);
    thrown.expectMessage(KeyPairManager.KEY_STORE_KEY);
    KeyPairManager.getKeyPair("alias");
  }

  @Test
  public void missingKeyStorePasswordProperty_throwsException() throws Exception {
    try {
      System.setProperty(KeyPairManager.KEY_STORE_KEY, "non-null-value");
      thrown.expect(IOException.class);
      thrown.expectMessage(KeyPairManager.KEY_STORE_PASSWORD_KEY);
      KeyPairManager.getKeyPair("alias");
    } finally {
      System.clearProperty(KeyPairManager.KEY_STORE_KEY);
    }
  }

  @Test
  public void missingKey_throwsException() throws Exception {
    String password = "password";
    char[] passwordChars = password.toCharArray();
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, passwordChars);
    File keyStoreFile = tempFolder.newFile("keyStoreFile");
    try (FileOutputStream fos = new FileOutputStream(keyStoreFile)) {
        keyStore.store(fos, passwordChars);
    }

    try {
      System.setProperty(KeyPairManager.KEY_STORE_KEY, keyStoreFile.getAbsolutePath());
      System.setProperty(KeyPairManager.KEY_STORE_PASSWORD_KEY, password);

      thrown.expect(IOException.class);
      thrown.expectMessage("No key for alias");
      KeyPairManager.getKeyPair("alias");
    } finally {
      System.clearProperty(KeyPairManager.KEY_STORE_KEY);
      System.clearProperty(KeyPairManager.KEY_STORE_PASSWORD_KEY);
    }
  }

  @Test
  public void badKeyStoreType_throwsException() throws Exception {
    String password = "password";
    char[] passwordChars = password.toCharArray();
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null, passwordChars);
    File keyStoreFile = tempFolder.newFile("keyStoreFile");
    try (FileOutputStream fos = new FileOutputStream(keyStoreFile)) {
        keyStore.store(fos, passwordChars);
    }

    try {
      System.setProperty(KeyPairManager.KEY_STORE_KEY, keyStoreFile.getAbsolutePath());
      System.setProperty(KeyPairManager.KEY_STORE_KEY, keyStoreFile.getAbsolutePath());
      System.setProperty(KeyPairManager.KEY_STORE_PASSWORD_KEY, password);
      System.setProperty(KeyPairManager.KEY_STORE_TYPE, "badType");

      thrown.expect(IOException.class);
      thrown.expectMessage("java.security.KeyStoreException");
      KeyPairManager.getKeyPair("alias");
    } finally {
      System.clearProperty(KeyPairManager.KEY_STORE_KEY);
      System.clearProperty(KeyPairManager.KEY_STORE_PASSWORD_KEY);
      System.clearProperty(KeyPairManager.KEY_STORE_TYPE);
    }
  }

  @Test
  public void validKeystore_succeeds() throws Exception {
    String password = "changeit";
    char[] passwordChars = password.toCharArray();

    try {
      System.setProperty(KeyPairManager.KEY_STORE_KEY,
          KeyPairManagerTest.class.getResource("/test-keys.jks").getPath());
      System.setProperty(KeyPairManager.KEY_STORE_PASSWORD_KEY, password);
      assertNotNull(KeyPairManager.getKeyPair("adaptor"));
    } finally {
      System.clearProperty(KeyPairManager.KEY_STORE_KEY);
      System.clearProperty(KeyPairManager.KEY_STORE_PASSWORD_KEY);
    }
  }
}
