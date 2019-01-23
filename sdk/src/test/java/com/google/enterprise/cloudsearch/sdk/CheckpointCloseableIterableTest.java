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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.CheckpointCloseableIterableImpl.CompareCheckpointCloseableIterableRule;
import java.util.Collections;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for CheckpointCloseableIterable. */
@RunWith(MockitoJUnitRunner.class)
public class CheckpointCloseableIterableTest {

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Rule
  public CompareCheckpointCloseableIterableRule<String> changeComparer =
      CompareCheckpointCloseableIterableRule.getCompareRule();

  private byte[] defaultCheckPoint = "checkpoint".getBytes();

  private byte[] checkPoint1 = "checkpoint1".getBytes();

  private byte[] checkPoint2 = "checkpoint2".getBytes();

  @Test
  public void testNullChanges() {

    CloseableIterable<String> delegate = null;
    thrown.expect(NullPointerException.class);
    new CheckpointCloseableIterableImpl.Builder<>(delegate).build();
  }

  @Test
  public void testFactoryMethod() {
    List<String> changes = Collections.emptyList();
    CheckpointCloseableIterableImpl<String> incremental =
        new CheckpointCloseableIterableImpl.Builder<>(changes)
            .setCheckpoint(defaultCheckPoint)
            .build();
    checkNotNull(incremental);
    assertEquals(defaultCheckPoint, incremental.getCheckpoint());
    assertFalse(incremental.hasMore());
  }

  @Test
  public void testIterator() {
    List<String> changes = ImmutableList.of("id1");
    CheckpointCloseableIterable<String> incremental =
        new CheckpointCloseableIterableImpl.Builder<>(changes.iterator()).build();
    assertEquals(changes, ImmutableList.copyOf(incremental.iterator()));
    thrown.expect(IllegalStateException.class);
    incremental.iterator();
  }

  @Test
  public void testCollection() {
    List<String> changes = ImmutableList.of("id1");
    CheckpointCloseableIterable<String> incremental =
        new CheckpointCloseableIterableImpl.Builder<>(changes).build();
    assertEquals(changes, ImmutableList.copyOf(incremental.iterator()));
    thrown.expect(IllegalStateException.class);
    incremental.iterator();
  }

  @Test
  public void testEquals() {
    List<String> changes1 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental1 =
        new CheckpointCloseableIterableImpl.Builder<>(changes1)
            .setCheckpoint(defaultCheckPoint)
            .build();

    List<String> changes2 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental2 =
        new CheckpointCloseableIterableImpl.Builder<>(changes2)
            .setCheckpoint(defaultCheckPoint)
            .build();
    assertTrue(changeComparer.compare(incremental1, incremental2));
  }

  @Test
  public void testNotEqualsDifferentCheckpoint() {
    List<String> changes1 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental1 =
        new CheckpointCloseableIterableImpl.Builder<>(changes1).setCheckpoint(checkPoint1).build();

    List<String> changes2 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental2 =
        new CheckpointCloseableIterableImpl.Builder<>(changes2).setCheckpoint(checkPoint2).build();
    assertFalse(changeComparer.compare(incremental1, incremental2));
  }

  @Test
  public void testNotEqualsDifferentChanges() {
    List<String> changes1 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental1 =
        new CheckpointCloseableIterableImpl.Builder<>(changes1).setCheckpoint(checkPoint1).build();

    List<String> changes2 = ImmutableList.of("id2");
    CheckpointCloseableIterableImpl<String> incremental2 =
        new CheckpointCloseableIterableImpl.Builder<>(changes2).setCheckpoint(checkPoint1).build();
    assertFalse(changeComparer.compare(incremental1, incremental2));
  }

  @Test
  public void testNotEqualsDifferentChangesExtraOperation() {
    List<String> changes1 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental1 =
        new CheckpointCloseableIterableImpl.Builder<>(changes1).setCheckpoint(checkPoint1).build();

    List<String> changes2 = ImmutableList.of("id1", "id2");
    CheckpointCloseableIterableImpl<String> incremental2 =
        new CheckpointCloseableIterableImpl.Builder<>(changes2).setCheckpoint(checkPoint1).build();
    assertFalse(changeComparer.compare(incremental1, incremental2));
  }

  @Test
  public void testChangesIdentical() {
    List<String> changes1 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental1 =
        new CheckpointCloseableIterableImpl.Builder<>(changes1).setCheckpoint(checkPoint1).build();
    assertTrue(changeComparer.compare(incremental1, incremental1));
  }

  @Test
  public void testChangesOneNull() {
    List<String> changes1 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental1 =
        new CheckpointCloseableIterableImpl.Builder<>(changes1).setCheckpoint(checkPoint1).build();
    assertFalse(changeComparer.compare(incremental1, null));
  }

  @Test
  public void testChangesBothNull() {
    assertTrue(changeComparer.compare(null, null));
  }

  @Test
  public void testCheckpointException() {
    List<String> changes1 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental1 =
        new CheckpointCloseableIterableImpl.Builder<>(changes1)
            .setCheckpoint(defaultCheckPoint)
            .build();

    List<String> changes2 = ImmutableList.of("id1");
    CheckpointCloseableIterableImpl<String> incremental2 =
        new CheckpointCloseableIterableImpl.Builder<>(changes2)
            .setCheckpoint(
                () -> {
                  throw new IllegalArgumentException();
                })
            .build();
    thrown.expect(IllegalArgumentException.class);
    changeComparer.compare(incremental1, incremental2);
  }

}
