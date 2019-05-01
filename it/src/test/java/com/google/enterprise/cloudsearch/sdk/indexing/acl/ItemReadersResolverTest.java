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

import static junit.framework.TestCase.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudsearch.v1.model.GSuitePrincipal;
import com.google.api.services.cloudsearch.v1.model.Item;
import com.google.api.services.cloudsearch.v1.model.ItemAcl;
import com.google.api.services.cloudsearch.v1.model.Principal;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.enterprise.cloudsearch.sdk.indexing.acl.ItemReadersResolver.ItemReadersResolverException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import junitparams.converters.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Unit tests for ItemReadersResolver.
 */
@RunWith(JUnitParamsRunner.class)
public class ItemReadersResolverTest {

  @Test(expected = NullPointerException.class)
  public void constructor_nullRetrieverArg_throwsNullPointerException() {
    new ItemReadersResolver(null);
  }

  @Test(expected = ItemRetrievalException.class)
  public void resolveReaders_parentAclNotRetrieved_throwsItemRetrievalException()
      throws ItemReadersResolverException, ItemRetrievalException {
    Item item = makeItem("NONEXISTENT", "MOCK_TYPE", null, null);
    ItemFetcher mockRetriever = mock(ItemFetcher.class);
    when(mockRetriever.getItem(anyString())).thenThrow(ItemRetrievalException.class);
    ItemReadersResolver walker = new ItemReadersResolver(mockRetriever);
    walker.resolveReaders(item);
  }

  @Test(expected = ItemReadersResolverException.class)
  public void resolveReaders_itemWithoutAcl_throwsItemReadersResolverException()
      throws ItemReadersResolverException, ItemRetrievalException {
    Item item = new Item();
    ItemReadersResolver walker = new ItemReadersResolver(itemName -> item);
    walker.resolveReaders(item);
  }

  @Test(expected = ItemReadersResolverException.class)
  public void resolveReaders_invalidInheritanceType_throwsItemReadersResolverException()
      throws ItemReadersResolverException, ItemRetrievalException {
    Item item = makeItem("MOCK_PARENT_NAME", "INVALID_TYPE", null, null);
    ItemReadersResolver walker = new ItemReadersResolver(itemName -> item);
    walker.resolveReaders(item);
  }

  @Test
  @Parameters
  public void resolveReaders(List<String> expectedReaders, Map<String, Item> itemMap)
      throws ItemReadersResolverException, ItemRetrievalException {
    ItemFetcher retriever = itemName -> itemMap.getOrDefault(itemName, null);
    ItemReadersResolver walker = new ItemReadersResolver(retriever);
    assertEquals(expectedReaders, walker.resolveReaders("child"));
  }

  private Object parametersForResolveReaders() {
    String po = ItemReadersResolver.PARENT_OVERRIDE;
    String co = ItemReadersResolver.CHILD_OVERRIDE;
    String bp = ItemReadersResolver.BOTH_PERMIT;
    return new Object[][] {
        {
          ImmutableList.of("u:u1", "u:u3"),
            ImmutableMap.of(
                "parent", makeItem(null, null, ImmutableList.of("u1"), ImmutableList.of("u2")),
                "child", makeItem(
                    "parent", po, ImmutableList.of("u2", "u3"), ImmutableList.of("u1")))
        },
        {
          ImmutableList.of("u:u1"),
            ImmutableMap.of(
                "parent", makeItem(null, null, ImmutableList.of("u1"), null),
                "child", makeItem("parent", po, null, null)
            )
        },
        {
            ImmutableList.of("u:u2", "u:u3"),
            ImmutableMap.of(
                "parent", makeItem(null, null, ImmutableList.of("u1"), ImmutableList.of("u2")),
                "child", makeItem(
                    "parent", co, ImmutableList.of("u2", "u3"), ImmutableList.of("u1"))
            )
        },
        {
            ImmutableList.of("u:u1"),
            ImmutableMap.of(
                "parent", makeItem(null, null, ImmutableList.of("u1"), null),
                "child", makeItem("parent", co, null, null)
            )
        },
        {
            ImmutableList.of("u:u2"),
            ImmutableMap.of(
                "parent", makeItem(null, null, ImmutableList.of("u1", "u2"), null),
                "child", makeItem("parent", bp, ImmutableList.of("u2", "u3"), null)
            )
        },
        {
            ImmutableList.of(),
            ImmutableMap.of(
                "parent", makeItem(null, null, ImmutableList.of("u1"), null),
                "child", makeItem("parent", bp, null, null)
            )
        }
    };
  }

  private Item makeItem(
      String parent, String inheritanceType, List<String> readers, List<String> deniedReaders) {
    ItemAcl acl = new ItemAcl()
        .setAclInheritanceType(inheritanceType)
        .setInheritAclFrom(parent);
    if (readers != null) {
      acl.setReaders(
          readers.stream()
              .map(name -> new Principal().setUserResourceName(name))
              .collect(Collectors.toList()));
    }
    if (deniedReaders != null) {
      acl.setDeniedReaders(
          deniedReaders.stream()
              .map(name -> new Principal().setUserResourceName(name))
              .collect(Collectors.toList()));
    }
    return new Item().setAcl(acl);
  }

  @Test
  @Parameters
  public void formatPrincipalNames(Set<String> expectedNames, List<Principal> principals) {
    assertEquals(expectedNames, ItemReadersResolver.formatPrincipalNames(principals));
  }
  private Object parametersForFormatPrincipalNames() {
    return new Object[][] {
        {Collections.emptySet(), null},
        {Collections.emptySet(), Collections.singletonList(null)},
        {Collections.emptySet(), ImmutableList.of()},
        {
          ImmutableSet.of("u:a", "u:b"),
            ImmutableList.of(
                new Principal().setUserResourceName("a"),
                new Principal().setUserResourceName("b"))
        }
    };
  }

  @Test
  @Parameters
  public void getSimplePrincipalName(String expectedName, Principal principal) {
    assertEquals(expectedName, ItemReadersResolver.getSimplePrincipalName(principal));
  }
  private Object parametersForGetSimplePrincipalName() {
    return new Object[][] {
        {"g:group", new Principal().setGroupResourceName("identitysources/X/groups/group")},
        {"u:user", new Principal().setUserResourceName("identitysources/X/users/user")},
        {
          "g:g@d",
            new Principal().setGsuitePrincipal(new GSuitePrincipal().setGsuiteGroupEmail("g@d"))
        },
        {
          "u:u@d",
            new Principal().setGsuitePrincipal(new GSuitePrincipal().setGsuiteUserEmail("u@d"))
        }
    };
  }

  @Test(expected = NullPointerException.class)
  public void getSimplePrincipalName_nullPrincipal_throwsNullPointerException() {
    ItemReadersResolver.getSimplePrincipalName(null);
  }

  @Test
  @Parameters({
      "a//h.d.com/b/GroupName, identitysources/X/groups/a//h.d.com/b/GroupName",
      "1/2/3, 1/2/3",
      "1, 1"
  })
  public void getSimpleResourceName(String expectedName, @Nullable String resourceName) {
    assertEquals(expectedName, ItemReadersResolver.getSimpleResourceName(resourceName));
  }

  @Test(expected = NullPointerException.class)
  public void getSimpleResourceName_nullResourceName_throwsNullPointerException() {
    ItemReadersResolver.getSimpleResourceName(null);
  }
}
