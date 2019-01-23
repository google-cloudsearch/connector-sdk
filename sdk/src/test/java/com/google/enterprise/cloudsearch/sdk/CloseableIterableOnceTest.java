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
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/** Tests CloseableIterableOnce. */
public class CloseableIterableOnceTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testIterator() {
    List<String> list = ImmutableList.of("foo");
    CloseableIterable<String> iterable  = new CloseableIterableOnce<>(list.iterator());
    assertEquals(list, ImmutableList.copyOf(iterable.iterator()));
    thrown.expect(IllegalStateException.class);
    iterable.iterator();
  }

  @Test
  public void testClose() {
    List<String> list = ImmutableList.of("foo");
    CloseableIterable<String> iterable = new CloseableIterableOnce<>(list.iterator());
    iterable.close();
    thrown.expect(IllegalStateException.class);
    iterable.iterator();
  }
}
