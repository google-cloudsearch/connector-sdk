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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

/**
 * This class load keyPair from user specified keyStore
 * javax.net.ssl.keyStore gives the file path to key store file
 * javax.net.ssl.keyStorePassword gives the password for key store file
 * javax.net.ssl.keyStoreType sepecifies the type of key store. Eg JKS
 */
class KeyPairManager {
  static final String KEY_STORE_KEY = "javax.net.ssl.keyStore";
  static final String KEY_STORE_PASSWORD_KEY = "javax.net.ssl.keyStorePassword";
  static final String KEY_STORE_TYPE = "javax.net.ssl.keyStoreType";

  static KeyPair getKeyPair(String alias) throws IOException {
    checkNotNull(alias, "alias cannot be null");
    String keyStoreFile = System.getProperty(KEY_STORE_KEY);
    if (keyStoreFile == null) {
      throw new IOException(KEY_STORE_KEY + " is not set");
    }
    String keyStoreType = System.getProperty(KEY_STORE_TYPE, KeyStore.getDefaultType());
    String keyStorePassword = System.getProperty(KEY_STORE_PASSWORD_KEY);
    if (keyStorePassword == null) {
      throw new IOException(KEY_STORE_PASSWORD_KEY + " is not set");
    }

    try (InputStream inputStream = new FileInputStream(keyStoreFile)) {
      KeyStore keyStore = KeyStore.getInstance(keyStoreType);
      keyStore.load(inputStream, keyStorePassword.toCharArray());
      Key key = keyStore.getKey(alias, keyStorePassword.toCharArray());

      if (key == null) {
        throw new IOException("No key for alias " + alias);
      }

      PrivateKey privateKey = (PrivateKey) key;
      PublicKey publicKey = keyStore.getCertificate(alias).getPublicKey();
      return new KeyPair(publicKey, privateKey);
    } catch (KeyStoreException
        | CertificateException
        | NoSuchAlgorithmException
        | UnrecoverableKeyException e) {
      throw new IOException(e);
    }
  }
}
