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
package com.google.enterprise.cloudsearch.sdk.indexing.template;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.cloudsearch.v1.model.PushItem;
import com.google.common.base.Strings;
import java.util.Objects;

/**
 * Wrapper to pair the item id and PushItem.
 */
public class PushItemResource {
  private final String id;
  private final PushItem item;

  public PushItemResource(String id, PushItem item) {
    checkArgument(!Strings.isNullOrEmpty(id), "id can not be null or empty");
    this.id = id;
    this.item = checkNotNull(item, "item can not be null");
  }

  public String getId() {
    return id;
  }

  public PushItem getItem() {
    return item;
  }

  @Override
  public String toString() {
    return "[itemId=" + id + ", pushItem=" + item + "]";
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, item);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof PushItemResource)) {
      return false;
    }
    PushItemResource other = (PushItemResource) obj;
    return Objects.equals(id, other.id) && Objects.equals(item, other.item);
  }
}
