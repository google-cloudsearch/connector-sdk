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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.api.client.json.GenericJson;
import com.google.api.services.cloudsearch.v1.model.Operation;
import com.google.common.collect.ImmutableList;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests BatchApiOperation. */
@RunWith(MockitoJUnitRunner.class)
public class BatchApiOperationTest {
  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Mock ApiOperation mockApiOperation;

  @Test
  public void testExecute() throws Exception {
    List<ApiOperation> list = ImmutableList.of(mockApiOperation);
    BatchApiOperation batch  = new BatchApiOperation(list.iterator());
    batch.execute(null);
    thrown.expect(IllegalStateException.class);
    batch.iterator();
  }

  @Test
  public void testExecuteWithConsumer() throws Exception {
    class ModifiableOperation implements ApiOperation {
      String name = "original name";

      public List<GenericJson> execute(IndexingService service) {
        return ImmutableList.of(new Operation().setDone(true));
      }
    }
    Consumer<ApiOperation> consumer = (operation) -> {
      if (operation instanceof ModifiableOperation) {
        ((ModifiableOperation) operation).name = "modified name";
      }
    };
    ModifiableOperation operation = new ModifiableOperation();
    List<ApiOperation> list = ImmutableList.of(operation);
    BatchApiOperation batch = new BatchApiOperation(list.iterator());
    batch.execute(null, Optional.of(consumer));
    assertEquals("modified name", operation.name);
  }

  @Test
  public void testIterator() {
    List<ApiOperation> list = ImmutableList.of(mockApiOperation);
    BatchApiOperation batch  = new BatchApiOperation(list.iterator());
    assertEquals(list, ImmutableList.copyOf(batch.iterator()));
    thrown.expect(IllegalStateException.class);
    batch.equals(new BatchApiOperation(list.iterator()));
  }

  @Test
  public void testEquals() throws Exception {
    List<ApiOperation> list = ImmutableList.of(mockApiOperation);
    BatchApiOperation batch  = new BatchApiOperation(list.iterator());
    assertEquals(new BatchApiOperation(list.iterator()), batch);
    thrown.expect(IllegalStateException.class);
    batch.execute(null);
  }

  @Test
  public void testEquals_same() {
    List<ApiOperation> list = ImmutableList.of(mockApiOperation);
    BatchApiOperation batch  = new BatchApiOperation(list.iterator());
    assertTrue(batch.equals(batch));
  }

  @Test
  public void testEquals_different() {
    List<ApiOperation> list = ImmutableList.of(mockApiOperation);
    List<ApiOperation> other = ImmutableList.of();
    BatchApiOperation batch  = new BatchApiOperation(list.iterator());
    assertFalse(batch.equals(new BatchApiOperation(other.iterator())));
  }

  @Test
  public void testEquals_null() {
    List<ApiOperation> list = ImmutableList.of(mockApiOperation);
    BatchApiOperation batch  = new BatchApiOperation(list.iterator());
    assertFalse(batch.equals(null));
  }

  @Test
  public void testEquals_object() {
    List<ApiOperation> list = ImmutableList.of(mockApiOperation);
    BatchApiOperation batch  = new BatchApiOperation(list.iterator());
    assertFalse(batch.equals(new Object()));
  }
}
