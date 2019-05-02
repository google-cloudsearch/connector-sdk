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

import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/**
 * Unit tests for {@link FakeIdentityRepository}.
 */
@RunWith(JUnit4.class)
public class FakeIdentityRepositoryTest {

  @Mock private RepositoryContext mockRepoContext;
  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();
  @Rule public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testHappyPath() throws IOException {
    final String domain = "example.com";
    String[][][] userSnapshots = {
        { {"u1", "u2"}, {"u3"} },
        { {"u4", "u5"}, {"u6"} }
    };
    String[][][] groupSnapshots = {
        { {"g1", "g2"}, {"g3"} },
        { {"g4", "g5"}, {"g6"} }
    };

    FakeIdentityRepository repo =
        new FakeIdentityRepository.Builder(domain)
        .addSnapshot(userSnapshots[0], groupSnapshots[0])
        .addSnapshot(userSnapshots[1], groupSnapshots[1])
        .build();
    configureMockRepositoryContext();

    for (int i = 0; i < userSnapshots.length; i++) {
      String[][] userPages = userSnapshots[i];
      String[][] groupPages = groupSnapshots[i];

      assertTrue(repo.hasMoreSnapshots());
      assertNotNull(repo.nextSnapshot());
      repo.init(mockRepoContext);
      for (int j = 0; j < userPages.length; j++) {
        String checkpoint = (j == 0) ? "" : String.valueOf(j);
        Set<String> usersListed = mapSet(
            Sets.newHashSet(repo.listUsers(checkpoint.getBytes())),
            user -> user.getGoogleIdentity());
        assertEquals(
            mapSet(Sets.newHashSet(userPages[j]), u -> u + "@" + domain),
            usersListed);
      }
      for (int j = 0; j < groupPages.length; j++) {
        String checkpoint = (j == 0) ? "" : String.valueOf(j);
        Set<String> groupsListed = mapSet(
            Sets.newHashSet(repo.listGroups(checkpoint.getBytes())),
            group -> group.getIdentity());
        assertEquals(
            mapSet(Sets.newHashSet(groupPages[j]), g -> domain + "/" + g),
            groupsListed);
      }
      if (i > 0) {
        assertEquals(
            repo.getRemovedUserNames(),
            Sets.difference(flatten(userSnapshots[i - 1]), flatten(userSnapshots[i]))
        );
        Set<String> removedGroupNames =
            Sets.difference(flatten(groupSnapshots[i - 1]), flatten(groupSnapshots[i]));
        assertEquals(
            repo.getRemovedGroupIds(),
            removedGroupNames.stream().map(n -> domain + "/" + n).collect(Collectors.toSet())
        );
      }
    }
    assertFalse(repo.hasMoreSnapshots());
  }

  @Test
  public void testUserEmailsAreTrimmed() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(
            new String[][] { {"  u1", "  u2", "  u3  "} }, new String[][] {})
        .build();
    assertThat(repo.getAllUserEmails(), hasItems("u1@X", "u2@X", "u3@X"));
  }

  @Test
  public void testGroupIdsAreTrimmed() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(
            new String[][] {}, new String[][] { {"  g1", "  g2", "  g3  "} })
        .build();
    assertThat(repo.getAllGroupIds(), hasItems("X/g1", "X/g2", "X/g3"));
  }

  @Test
  public void testGetAllUserNamesInSnapshot() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(
            new String[][] { {"u1", "u2"}, {"u3"}, {"u4", "u5"} }, new String[][]{})
        .build();
    FakeIdentityRepository.Snapshot snapshot = repo.nextSnapshot();
    assertEquals(
        snapshot.getAllUserNames(),
        ImmutableSet.of("u1", "u2", "u3", "u4", "u5")
    );
  }

  @Test
  public void testGetAllGroupIdsInSnapshot() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(
            new String[][]{}, new String[][] { {"g1", "g2"}, {"g3"}, {"g4", "g5"} })
        .build();
    FakeIdentityRepository.Snapshot snapshot = repo.nextSnapshot();
    assertEquals(
        snapshot.getAllGroupIds(),
        ImmutableSet.of("X/g1", "X/g2", "X/g3", "X/g4", "X/g5")
    );
  }

  @Test
  public void testGetAllUserEmailsFromAllSnapshots() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(new String[][] { {"u1", "u2"} }, new String[][]{})
        .addSnapshot(new String[][] { {"u3"} }, new String[][]{})
        .addSnapshot(new String[][] { {"u4", "u5"} }, new String[][]{})
        .build();
    assertEquals(
        repo.getAllUserEmails(),
        ImmutableSet.of("u1@X", "u2@X", "u3@X", "u4@X", "u5@X")
    );
  }

  @Test
  public void testGetAllGroupIdsFromAllSnapshots() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(new String[][]{}, new String[][] { {"g1", "g2"} })
        .addSnapshot(new String[][]{}, new String[][] { {"g3"} })
        .addSnapshot(new String[][]{}, new String[][] { {"g4", "g5"} })
        .build();
    assertEquals(
        repo.getAllGroupIds(),
        ImmutableSet.of("X/g1", "X/g2", "X/g3", "X/g4", "X/g5")
    );
  }

  @Test
  public void testExceptionThrownWhenNoSnapshots() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("no snapshots");
    new FakeIdentityRepository.Builder("example.com").build();
  }

  @Test
  public void testExceptionThrownWhenDomainNull() {
    thrown.expect(NullPointerException.class);
    new FakeIdentityRepository.Builder(null).build();
  }

  @Test
  public void testExceptionThrownWhenDomainEmpty() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("empty");
    new FakeIdentityRepository.Builder("").build();
  }

  private void configureMockRepositoryContext() {
    when(mockRepoContext.buildIdentityUser(any(), any()))
        .thenAnswer(invocation -> {
          Object[] args = invocation.getArguments();
          return new IdentityUser.Builder()
              .setGoogleIdentity((String) args[0])
              .setUserIdentity((String) args[1])
              .setAttribute("MOCK_ATTRIBUTE")
              .setSchema("MOCK_SCHEMA")
              .build();
        });
    when(mockRepoContext.buildEntityKeyForGroup(any()))
        .thenAnswer(
            invocation -> {
              Object[] args = invocation.getArguments();
              return new EntityKey().setId((String) args[0]).setNamespace("MOCK_NAMESPACE");
            });
  }

  /**
   * Creates a {@link Set} containing all the individual elements of the matrix.
   */
  private <T> Set<T> flatten(T[][] matrix) {
    return Stream.of(matrix).flatMap(Stream::of).collect(Collectors.toSet());
  }

  /**
   * Maps a set to another one using a provided transformation function.
   */
  private static <T, S> Set<S> mapSet(Set<T> set, Function<T, S> transformer) {
    return set
        .stream()
        .map(transformer)
        .collect(ImmutableSet.toImmutableSet());
  }
}
