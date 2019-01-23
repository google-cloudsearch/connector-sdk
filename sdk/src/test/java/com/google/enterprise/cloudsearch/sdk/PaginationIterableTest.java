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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Unit tests for {@link PaginationIterable} */
public class PaginationIterableTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  private abstract static class TestPaginationIterable extends PaginationIterable<String, String> {
    public TestPaginationIterable() {
      super(Optional.empty());
    }
  }

  @Test
  public void testEmptyIterable() {
    TestPaginationIterable iterable =
        new TestPaginationIterable() {
          @Override
          public Page<String, String> getPage(Optional<String> nextPage) throws IOException {
            return new Page<>(ImmutableList.of(), Optional.empty());
          }
        };

    validate(ImmutableList.of(), iterable);
  }

  @Test
  public void testPaginationError() {
    TestPaginationIterable iterable =
        new TestPaginationIterable() {
          @Override
          public Page<String, String> getPage(Optional<String> nextPage) throws IOException {
            throw new IOException("error");
          }
        };

    // Try iterating over all items
    thrown.expect(RuntimeException.class);
    Iterables.size(iterable);
  }

  @Test
  public void testSinglePageIterable() {
    TestPaginationIterable iterable =
        new TestPaginationIterable() {
          @Override
          public Page<String, String> getPage(Optional<String> nextPage) throws IOException {
            return new Page<>(ImmutableList.of("item1", "item2"), Optional.empty());
          }
        };

    validate(ImmutableList.of("item1", "item2"), iterable);
  }

  @Test
  public void testFirstpageEmptyNextPageAvailableIterable() {
    TestPaginationIterable iterable =
        new TestPaginationIterable() {
          @Override
          public Page<String, String> getPage(Optional<String> nextPage) throws IOException {
            if (!nextPage.isPresent()) {
              return new Page<>(ImmutableList.of(), Optional.of("nextPage"));
            }
            if ("nextPage".equals(nextPage.get())) {
              return new Page<>(ImmutableList.of("item1", "item2"), Optional.empty());
            }
            throw new UnsupportedOperationException("Unexpected pagination token " + nextPage);
          }
        };

    validate(ImmutableList.of("item1", "item2"), iterable);
  }

  @Test
  public void testMultiPage() {
    TestPaginationIterable iterable =
        new TestPaginationIterable() {
          @Override
          public Page<String, String> getPage(Optional<String> nextPage) throws IOException {
            if (!nextPage.isPresent()) {
              return new Page<>(ImmutableList.of("item1", "item2"), Optional.of("nextPage"));
            }
            if ("nextPage".equals(nextPage.get())) {
              return new Page<>(ImmutableList.of("item3", "item4"), Optional.empty());
            }
            throw new UnsupportedOperationException("Unexpected pagination token " + nextPage);
          }
        };

    validate(ImmutableList.of("item1", "item2", "item3", "item4"), iterable);
  }

  private void validate(ImmutableList<String> expected, Iterable<String> iterable) {
    ImmutableList<String> actual = ImmutableList.copyOf(iterable);
    assertEquals(expected, actual);
  }
}
