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
package com.google.enterprise.cloudsearch.sdk.indexing.traverser;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.enterprise.cloudsearch.sdk.indexing.IndexingService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Tests for {@link AbstractTraverserWorker}. */

public class AbstractTraverserWorkerTest {

  private class AbstractTraverserWorkerMock extends AbstractTraverserWorker {

    public AbstractTraverserWorkerMock(
        TraverserConfiguration conf, IndexingService indexingService) {
      super(conf, indexingService);
    }

    @Override public void poll() {}

    @Override
    void shutdownWorker() {}

  }

  @Mock TraverserConfiguration conf;
  @Mock IndexingService indexingService;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testGetName(){
    TraverserConfiguration conf = mock(TraverserConfiguration.class);
    when(conf.getName()).thenReturn("TestName");
    AbstractTraverserWorker my = spy(new AbstractTraverserWorkerMock(conf , indexingService));
    my.getName();
    assertEquals(my.getName(), "TestName");
  }

}
