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

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class to resolve the allowed readers for an item.
 *
 * The functionality in this class is helpful to verify that item permissions
 * are correctly indexed without needing to verify items are serving (with the
 * additional complexities that requires).
 */
public class ItemReadersResolver {
  private static final Logger logger = Logger.getLogger(ItemReadersResolver.class.getName());

  static final String PARENT_OVERRIDE = "PARENT_OVERRIDE";
  static final String BOTH_PERMIT = "BOTH_PERMIT";
  static final String CHILD_OVERRIDE = "CHILD_OVERRIDE";

  private final ItemFetcher itemFetcher;

  public ItemReadersResolver(ItemFetcher itemFetcher) {
    checkNotNull(itemFetcher, "itemFetcher may not be null");
    this.itemFetcher = itemFetcher;
  }

  public Collection<String> resolveReaders(String itemName)
      throws ItemReadersResolverException, ItemRetrievalException {
    checkNotNull(itemName, "itemName may not be null");
    return resolveReaders(itemFetcher.getItem(itemName));
  }

  /**
   * Obtains a list of users with read access to the item.
   *
   * Notice that no group of any kind is expanded. So, for example, if
   * {@code user1} is part of @code{groupA} and at some point {@code user1} is individually
   * given read permission but at some upper level {@code groupA} is denied access,
   * then this method can't tell that {@code user1} is ultimately denied access to the item
   * because it's a member of {@code groupA}.
   *
   * @return a list with the names of the principals with access to the item.
   *   The names have the format "u:userName" and "g:groupName" to make it
   *   simple to determine which principals are groups and which are users.
   */
  public List<String> resolveReaders(Item item)
      throws ItemReadersResolverException, ItemRetrievalException {
    // Summary of Cloud Search's ACL evaluation rules given in
    // https://developers.google.com/cloud-search/docs/guides/acls:
    // - ACLs are evaluated leaf-to-root.
    // - Parent override: parent_readers U (child_readers - parent_denied)
    // - Child override: child_readers U (parent_readers - child_denied)
    // - Both permit: child_readers ^ parent_readers
    checkNotNull(item, "item may not be null");
    ItemAcl itemAcl = getAcl(item);
    Set<String> readers = new HashSet<>(formatPrincipalNames(itemAcl.getReaders()));
    readers.removeAll(formatPrincipalNames(itemAcl.getDeniedReaders()));
    logger.log(Level.FINE, "{0} <= child_readers = {1}, child_denied = {2}",
        new Object[]{
            readers,
            formatPrincipalNames(itemAcl.getReaders()),
            formatPrincipalNames(itemAcl.getDeniedReaders())
    });
    String parentName = itemAcl.getInheritAclFrom();
    while (parentName != null) {
      Item parentItem = itemFetcher.getItem(parentName);
      ItemAcl parentAcl = getAcl(parentItem);
      Set<String> parentReaders = formatPrincipalNames(parentAcl.getReaders());
      String inheritanceType = itemAcl.getAclInheritanceType();
      switch (inheritanceType) {
        case PARENT_OVERRIDE:
          Set<String> parentDenied = formatPrincipalNames(parentAcl.getDeniedReaders());
          readers.removeAll(parentDenied);
          readers.addAll(parentReaders);
          logger.log(
              Level.FINE, "{0} <= parent={1}, parent_denied={2}, parent_readers={3} with {4}",
              new Object[]{ readers, parentName, parentDenied, parentReaders, inheritanceType });
          break;
        case CHILD_OVERRIDE:
          Set<String> childDenied = formatPrincipalNames(itemAcl.getDeniedReaders());
          parentReaders.removeAll(childDenied);
          readers.addAll(parentReaders);
          logger.log(Level.FINE, "{0} <= parent={1}, child_denied={2}, parent_readers={3} with {4}",
              new Object[]{ readers, parentName, childDenied, parentReaders, inheritanceType });
          break;
        case BOTH_PERMIT:
          readers.retainAll(parentReaders);
          logger.log(Level.FINE, "{0} <= parent={1}, parent_readers={2} with {3}",
            new Object[]{ readers, parentName, inheritanceType, parentReaders });
          break;
        default:
          throw new ItemReadersResolverException(
              "Unrecognized inheritance type " + inheritanceType + " for item " + item.getName());
      }
      // Move up.
      parentName = parentAcl.getInheritAclFrom();
      item = parentItem;
      itemAcl = parentAcl;
    }
    ArrayList<String> sortedReaders = new ArrayList<>(readers);
    Collections.sort(sortedReaders);
    return sortedReaders;
  }

  @VisibleForTesting
  static Set<String> formatPrincipalNames(@Nullable List<Principal> principalList) {
    if (principalList == null) {
      return Collections.emptySet();
    }
    return principalList.stream()
        .filter(Objects::nonNull)
        .map(ItemReadersResolver::getSimplePrincipalName)
        .collect(Collectors.toSet());
  }

  @VisibleForTesting
  static String getSimplePrincipalName(Principal principal) {
    checkNotNull(principal, "principal may not be null");
    if (!Strings.isNullOrEmpty(principal.getGroupResourceName())) {
      return "g:" + getSimpleResourceName(principal.getGroupResourceName());
    } else if (!Strings.isNullOrEmpty(principal.getUserResourceName())) {
      return "u:" + getSimpleResourceName(principal.getUserResourceName());
    } else if (principal.getGsuitePrincipal() != null) {
      if (!Strings.isNullOrEmpty(principal.getGsuitePrincipal().getGsuiteUserEmail())) {
        return "u:" + principal.getGsuitePrincipal().getGsuiteUserEmail();
      } else {
        return "g:" + principal.getGsuitePrincipal().getGsuiteGroupEmail();
      }
    } else {
      return null;
    }
  }

  @VisibleForTesting
  static String getSimpleResourceName(String resourceName) {
    checkNotNull(resourceName, "resourceName may not be null");
    // The format for principal.getXxxResourceName() is
    // identitysources/<IDENTITY SOURCE ID>/{user|groups}/<RESOURCE NAME>. Notice that
    // the resource name may contain slashes (e.g., SharePoint local groups the resource
    // name is "%5Bhttp%3A//<HOST>%3A13202/sites/testsitecollection%5D<GROUP NAME>".
    String[] parts = resourceName.split("/");
    if (parts.length > 3) {
      return String.join("/", Arrays.copyOfRange(parts, 3, parts.length));
    } else {
      return resourceName;
    }
  }

  private ItemAcl getAcl(Item item) throws ItemReadersResolverException {
    checkNotNull(item, "item may not be null");
    ItemAcl itemAcl = item.getAcl();
    if (itemAcl == null) {
      throw new ItemReadersResolverException("No ACL defined for item " + item.getName());
    }
    return itemAcl;
  }

  /**
   * Generic exception for errors found when resolving readers for an item.
   */
  public class ItemReadersResolverException extends Exception {
    ItemReadersResolverException(String message) {
      super(message);
    }
  }
}
