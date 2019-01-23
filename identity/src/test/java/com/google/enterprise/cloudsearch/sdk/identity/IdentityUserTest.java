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
package com.google.enterprise.cloudsearch.sdk.identity;

import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.api.services.admin.directory.model.User;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IdentityUserTest {
  @Rule public ExpectedException thrown = ExpectedException.none();
  @Mock private IdentityService mockIdentityService;

  @Test
  public void testBuilderNullIdentity() {
    thrown.expect(IllegalArgumentException.class);
    new IdentityUser.Builder()
        .setAttribute("attrib")
        .setSchema("schema")
        .setGoogleIdentity("user@domain.com")
        .build();
  }

  @Test
  public void testBuilderNullGoogleIdentity() {
    thrown.expect(IllegalArgumentException.class);
    new IdentityUser.Builder()
        .setUserIdentity("user1")
        .setAttribute("attrib")
        .setSchema("schema")
        .build();
  }

  @Test
  public void testBuilderNullSchema() {
    thrown.expect(IllegalArgumentException.class);
    new IdentityUser.Builder()
        .setUserIdentity("user1")
        .setAttribute("attrib")
        .setGoogleIdentity("user@domain.com")
        .build();
  }

  @Test
  public void testBuilderNullAttribute() {
    thrown.expect(IllegalArgumentException.class);
    new IdentityUser.Builder()
        .setUserIdentity("user1")
        .setSchema("schema")
        .setGoogleIdentity("user@domain.com")
        .build();
  }

  @Test
  public void testBuilder() {
    IdentityUser user = new IdentityUser.Builder()
        .setUserIdentity("user1")
        .setSchema("schema")
        .setAttribute("attrib")
        .setGoogleIdentity("user@domain.com")
        .build();
    assertEquals("user1", user.getIdentity());
    assertEquals("schema", user.getSchema());
    assertEquals("attrib", user.getAttribute());
    assertEquals("user@domain.com", user.getGoogleIdentity());
  }

  @Test
  public void testSync() throws IOException, InterruptedException, ExecutionException {
    IdentityUser user =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("user@domain.com")
            .build();
    when(mockIdentityService.updateUserMapping(
            "user@domain.com", "schema", "attrib", Optional.of("user1")))
        .thenReturn(Futures.immediateFuture(new User().setPrimaryEmail("user@domain.com")));
    ListenableFuture<IdentityUser> sync = user.sync(null, mockIdentityService);
    assertEquals(user, sync.get());
  }

  @Test
  public void testSyncIgnoreCase() throws IOException, InterruptedException, ExecutionException {
    IdentityUser user =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("user@domain.com")
            .build();
    when(mockIdentityService.updateUserMapping(
            "user@domain.com", "schema", "attrib", Optional.of("user1")))
        .thenReturn(Futures.immediateFuture(new User().setPrimaryEmail("USER@domain.com")));
    ListenableFuture<IdentityUser> sync = user.sync(null, mockIdentityService);
    assertEquals(user, sync.get());
  }

  @Test
  public void testSyncAliasMatch() throws IOException, InterruptedException, ExecutionException {
    IdentityUser user =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("alias@domain.com")
            .build();
    when(mockIdentityService.updateUserMapping(
            "alias@domain.com", "schema", "attrib", Optional.of("user1")))
        .thenReturn(
            Futures.immediateFuture(
                new User()
                    .setPrimaryEmail("USER@domain.com")
                    .setAliases(ImmutableList.of("alias@domain.com"))));
    ListenableFuture<IdentityUser> sync = user.sync(null, mockIdentityService);
    assertEquals(user, sync.get());
  }

  @Test
  public void testSyncMismatch() throws IOException, InterruptedException, ExecutionException {
    IdentityUser user =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("alias@domain.com")
            .build();
    when(mockIdentityService.updateUserMapping(
            "alias@domain.com", "schema", "attrib", Optional.of("user1")))
        .thenReturn(Futures.immediateFuture(new User().setPrimaryEmail("USER@domain.com")));
    thrown.expectCause(isA(IllegalArgumentException.class));
    user.sync(null, mockIdentityService).get();
  }

  @Test
  public void testSyncSameUser() throws IOException, InterruptedException, ExecutionException {
    IdentityUser previous =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("user@domain.com")
            .build();
    IdentityUser current =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("user@domain.com")
            .build();
    ListenableFuture<IdentityUser> sync = current.sync(previous, mockIdentityService);
    assertEquals(current, sync.get());
  }

  @Test
  public void testUnmap() throws IOException, InterruptedException, ExecutionException {
    IdentityUser user =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("user@domain.com")
            .build();
    when(mockIdentityService.updateUserMapping(
            "user@domain.com", "schema", "attrib", Optional.empty()))
        .thenReturn(Futures.immediateFuture(new User().setPrimaryEmail("user@domain.com")));
    ListenableFuture<Boolean> sync = user.unmap(mockIdentityService);
    assertTrue(sync.get());
  }

  @Test
  public void testUnmapFails() throws IOException, InterruptedException, ExecutionException {
    IdentityUser user =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("user@domain.com")
            .build();
    when(mockIdentityService.updateUserMapping(
            "user@domain.com", "schema", "attrib", Optional.empty()))
        .thenReturn(Futures.immediateFailedFuture(new IOException("unable to update user")));
    ListenableFuture<Boolean> sync = user.unmap(mockIdentityService);
    thrown.expectCause(isA(IOException.class));
    sync.get();
  }

  @Test
  public void testSyncFails() throws IOException, InterruptedException, ExecutionException {
    IdentityUser user =
        new IdentityUser.Builder()
            .setUserIdentity("user1")
            .setSchema("schema")
            .setAttribute("attrib")
            .setGoogleIdentity("user@domain.com")
            .build();
    when(mockIdentityService.updateUserMapping(
            "user@domain.com", "schema", "attrib", Optional.of("user1")))
        .thenReturn(Futures.immediateFailedFuture(new IOException("unable to update user")));
    ListenableFuture<IdentityUser> sync = user.sync(null, mockIdentityService);
    thrown.expectCause(isA(IOException.class));
    sync.get();
  }
}
