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

import static com.google.common.base.Preconditions.checkArgument;

import com.google.api.client.util.escape.PercentEscaper;
import com.google.common.base.Strings;

/**
 * Helper utility to encode Group identifier in a consistent way between identity and indexing
 * connectors. CloudIdentity API supports only subset of characters in group identifiers. This
 * utility escapes unsupported characters using percent encoding.
 */
public class GroupIdEncoder {
  private static final PercentEscaper GROUP_ID_ESCAPER = new PercentEscaper("_.@-\\/", false);

  /**
   * Encode group identifier by escaping unsupported characters.
   *
   * @param groupId to encode
   * @return encoded group identifier
   */
  public static String encodeGroupId(String groupId) {
    checkArgument(!Strings.isNullOrEmpty(groupId), "groupId can not be null or empty");
    return GROUP_ID_ESCAPER.escape(groupId);
  }
}
