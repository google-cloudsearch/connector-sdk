/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.enterprise.cloudsearch.sdk;

import static org.junit.Assert.assertTrue;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

/** Tests for {@link IgnoreExceptionHandler}. */
@RunWith(JUnitParamsRunner.class)
public class IgnoreExceptionHandlerTest {

  @Test
  @Parameters(method = "handleExceptionValues")
  public void handleException_anyParameters_returnsTrue(Exception e, int numTries)
      throws Exception {
    IgnoreExceptionHandler handler = new IgnoreExceptionHandler();
    assertTrue(handler.handleException(e, numTries));
  }

  private Object[] handleExceptionValues() {
    return new Object[][] {
      { null, Integer.valueOf(0) },
      { null, Integer.valueOf(3) },
      { new Exception(), Integer.valueOf(0) },
      { new Exception(), Integer.valueOf(3) }
    };
  }
}
