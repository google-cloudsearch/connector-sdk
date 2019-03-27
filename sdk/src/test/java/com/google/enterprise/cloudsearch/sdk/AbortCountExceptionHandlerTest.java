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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link AbortCountExceptionHandler}. */
@RunWith(MockitoJUnitRunner.class)
public class AbortCountExceptionHandlerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void init_missingUnit_throwsException() {
    thrown.expect(NullPointerException.class);
    new AbortCountExceptionHandler(1, 1, null);
  }

  @Test
  public void handleException_tooManyTries_returnsFalse() throws Exception {
     AbortCountExceptionHandler handler =
        new AbortCountExceptionHandler(1, 1, TimeUnit.SECONDS);
    assertFalse(handler.handleException(null, 3));
  }

  @Test
  public void handleException_wait_returnsTrue() throws Exception {
     AbortCountExceptionHandler handler =
        new AbortCountExceptionHandler(2, 1, TimeUnit.SECONDS);
    assertTrue(handler.handleException(null, 1));
  }
}
