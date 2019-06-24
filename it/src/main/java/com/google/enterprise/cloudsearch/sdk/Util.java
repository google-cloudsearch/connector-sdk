/*
 * Copyright © 2018 Google Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.indexing.Acl;
import java.util.Random;

/**
 * Common utility methods for integration tests.
 */
public class Util {
  public static final Acl PUBLIC_ACL = new Acl.Builder()
      .setReaders(ImmutableList.of(Acl.getCustomerPrincipal()))
      .build();

  private static final Random RANDOM_ID = new Random();

  public static String getRandomId() {
    return Integer.toHexString(RANDOM_ID.nextInt());
  }

  public static String getItemId(String sourceId, String name) {
    return "datasources/" + sourceId + "/items/" + BaseApiService.escapeResourceName(name);
  }

  public static String unescapeItemName(String name) {
    return BaseApiService.decodeResourceName(name);
  }
}
