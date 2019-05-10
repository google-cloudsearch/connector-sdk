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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.google.api.services.cloudsearch.v1.model.RepositoryError;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link RepositoryException}. */
@RunWith(MockitoJUnitRunner.class)
public class RepositoryExceptionTest {

  @Test
  public void getRepositoryError_noData_valuesAreNull() {
    RepositoryException exception = new RepositoryException.Builder().build();
    RepositoryError error = exception.getRepositoryError();
    assertNull(error.getErrorMessage());
    assertNull(error.getHttpStatusCode());
    assertNull(error.getType());
  }

  @Test
  public void getRepositoryError_withMessageAndCause_optionalsAreNull() {
    RepositoryException exception = new RepositoryException.Builder()
        .setErrorMessage("error message")
        .setCause(new Throwable("cause"))
        .build();
    assertEquals("cause", exception.getCause().getMessage());
    RepositoryError error = exception.getRepositoryError();
    assertEquals("error message", error.getErrorMessage());
    // RepositoryError doesn't preserve cause
    assertNull(error.getHttpStatusCode());
    assertNull(error.getType());
  }

  @Test
  public void getRepositoryError_withErrorType_hasType() {
    RepositoryException exception = new RepositoryException.Builder()
        .setErrorType(RepositoryException.ErrorType.UNKNOWN)
        .build();
    RepositoryError error = exception.getRepositoryError();
    assertEquals(RepositoryException.ErrorType.UNKNOWN.name(), error.getType());
  }

  @Test
  public void getRepositoryError_withErrorCode_hasCode() {
    RepositoryException exception = new RepositoryException.Builder()
        .setErrorCode(400)
        .build();
    RepositoryError error = exception.getRepositoryError();
    assertEquals(Integer.valueOf(400), error.getHttpStatusCode());
  }
}
