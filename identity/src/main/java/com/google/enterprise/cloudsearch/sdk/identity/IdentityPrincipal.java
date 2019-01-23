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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.base.Strings;

/** Represents third-party identity such as user or group. */
public abstract class IdentityPrincipal<T> implements Sync<T> {

  /** Kind of {@link IdentityPrincipal} */
  public enum Kind {
    /** Represents third-party user. */
    USER,
    /** Represents third-party group. */
    GROUP
  }

  protected final String identity;

  /** Constructs {@link IdentityPrincipal} for given {@code identity}. */
  public IdentityPrincipal(String identity) {
    this.identity = identity;
  }

  /** Gets kind for principal. */
  public abstract Kind getKind();

  /** Gets identity for third-party principal. */
  public String getIdentity() {
    return identity;
  }

  static String checkNotNullOrEmpty(String input, String errorMessage) {
    checkArgument(!Strings.isNullOrEmpty(input), errorMessage);
    return input;
  }
}
