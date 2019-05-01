/*
 * Copyright 2019 Google LLC
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

package com.google.enterprise.cloudsearch.sdk.indexing.acl;

import com.google.api.services.cloudsearch.v1.model.Item;

/**
 * An interface to retrieve items.
 *
 * This allows to abstract away the specific mechanism by which ItemReadersResolver obtains the ACL
 * object for a given item. This is necessary because an @code{ItemAcl} object provides the *name*
 * of the item it inherits from, but not a reference to that item (see
 * https://developers.google.com/cloud-search/docs/reference/rest/v1/indexing.datasources.items#itemacl).
 *
 * A basic implementation may just call @code{v1.indexing.datasources.items.get}. A more complex
 * implementation may implement some kind of caching mechanism to speed-up walking through the
 * hierarchies of several items.
 */
public interface ItemFetcher {

  /**
   * Gets the ACL for the given item.
   */
  Item getItem(String itemName) throws ItemRetrievalException;
}
