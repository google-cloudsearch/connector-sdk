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

import static com.google.common.truth.Truth.assertThat;
import static java.lang.String.format;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.api.services.cloudidentity.v1.model.EntityKey;
import com.google.common.collect.Sets;
import com.google.common.truth.Correspondence;
import java.io.IOException;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
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
    final IdentityUserCorrespondence userCorrespondence = new IdentityUserCorrespondence(domain);
    final IdentityGroupCorrespondence groupCorrespondence = new IdentityGroupCorrespondence(domain);

    FakeIdentityRepository repo =
        new FakeIdentityRepository.Builder(domain)
        .addSnapshot(userSnapshots[0], groupSnapshots[0])
        .addSnapshot(userSnapshots[1], groupSnapshots[1])
        .build();
    configureMockRepositoryContext();

    for (int i = 0; i < userSnapshots.length; i++) {
      String[][] userPages = userSnapshots[i];
      String[][] groupPages = groupSnapshots[i];

      assertThat(repo.hasMoreSnapshots()).isTrue();
      assertThat(repo.nextSnapshot()).isNotNull();
      repo.init(mockRepoContext);
      for (int j = 0; j < userPages.length; j++) {
        String checkpoint = (j == 0) ? "" : String.valueOf(j);
        assertThat(repo.listUsers(checkpoint.getBytes()))
            .comparingElementsUsing(userCorrespondence)
            .containsExactlyElementsIn(userPages[j]);
      }
      for (int j = 0; j < groupPages.length; j++) {
        String checkpoint = (j == 0) ? "" : String.valueOf(j);
        assertThat(repo.listGroups(checkpoint.getBytes()))
            .comparingElementsUsing(groupCorrespondence)
            .containsExactlyElementsIn(groupPages[j]);
      }
      if (i > 0) {
        assertThat(repo.getRemovedUserNames())
            .containsExactlyElementsIn(
                Sets.difference(flatten(userSnapshots[i - 1]), flatten(userSnapshots[i])));
        Set<String> removedGroupNames =
            Sets.difference(flatten(groupSnapshots[i - 1]), flatten(groupSnapshots[i]));
        assertThat(repo.getRemovedGroupIds())
            .containsExactlyElementsIn(
                removedGroupNames.stream().map(n -> domain + "/" + n).collect(Collectors.toSet()));
      }
    }
    assertThat(repo.hasMoreSnapshots()).isFalse();
  }

  @Test
  public void testUserEmailsAreTrimmed() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(
            new String[][] { {"  u1", "  u2", "  u3  "} }, new String[][] {})
        .build();
    assertThat(repo.getAllUserEmails())
        .containsAllOf("u1@X", "u2@X", "u3@X");
  }

  @Test
  public void testGroupIdsAreTrimmed() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(
            new String[][] {}, new String[][] { {"  g1", "  g2", "  g3  "} })
        .build();
    assertThat(repo.getAllGroupIds())
        .containsAllOf("X/g1", "X/g2", "X/g3");
  }

  @Test
  public void testGetAllUserNamesInSnapshot() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(
            new String[][] { {"u1", "u2"}, {"u3"}, {"u4", "u5"} }, new String[][]{})
        .build();
    FakeIdentityRepository.Snapshot snapshot = repo.nextSnapshot();
    assertThat(snapshot.getAllUserNames())
        .containsExactly("u1", "u2", "u3", "u4", "u5");
  }

  @Test
  public void testGetAllGroupIdsInSnapshot() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(
            new String[][]{}, new String[][] { {"g1", "g2"}, {"g3"}, {"g4", "g5"} })
        .build();
    FakeIdentityRepository.Snapshot snapshot = repo.nextSnapshot();
    assertThat(snapshot.getAllGroupIds())
        .containsExactly("X/g1", "X/g2", "X/g3", "X/g4", "X/g5");
  }

  @Test
  public void testGetAllUserEmailsFromAllSnapshots() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(new String[][] { {"u1", "u2"} }, new String[][]{})
        .addSnapshot(new String[][] { {"u3"} }, new String[][]{})
        .addSnapshot(new String[][] { {"u4", "u5"} }, new String[][]{})
        .build();
    assertThat(repo.getAllUserEmails())
        .containsExactly("u1@X", "u2@X", "u3@X", "u4@X", "u5@X");
  }

  @Test
  public void testGetAllGroupIdsFromAllSnapshots() {
    FakeIdentityRepository repo = new FakeIdentityRepository.Builder("X")
        .addSnapshot(new String[][]{}, new String[][] { {"g1", "g2"} })
        .addSnapshot(new String[][]{}, new String[][] { {"g3"} })
        .addSnapshot(new String[][]{}, new String[][] { {"g4", "g5"} })
        .build();
    assertThat(repo.getAllGroupIds())
        .containsExactly("X/g1", "X/g2", "X/g3", "X/g4", "X/g5");
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

  /**
   * Helper class to allow to compare IdentityUser objects with a user's email.
   */
  private static class IdentityUserCorrespondence extends Correspondence<IdentityUser, String> {
    final String domain;

    IdentityUserCorrespondence(String domain) {
      this.domain = domain;
    }

    @Override
    public boolean compare(@Nullable IdentityUser actual, @Nullable String expected) {
      String expectedEmail = format("%s@%s", expected, domain);
      return actual != null && expected != null && expectedEmail.equals(actual.getGoogleIdentity());
    }

    @Override
    public String toString() {
      // This must be implemented, but there's nothing really useful to say here.
      return "has a Google identity of";
    }
  }

  /**
   * Helper class to allow to compare IdentityGroup objects with a groups ID.
   */
  private static class IdentityGroupCorrespondence extends Correspondence<IdentityGroup, String> {
    final String domain;

    IdentityGroupCorrespondence(String domain) {
      this.domain = domain;
    }

    @Override
    public boolean compare(@Nullable IdentityGroup actual, @Nullable String expected) {
      String expectedGroupId = format("%s/%s", domain, expected);
      return actual != null && expected != null && expectedGroupId.equals(actual.getIdentity());
    }

    @Override
    public String toString() {
      // This must be implemented, but there's nothing really useful to say here.
      return "has an identity of";
    }
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
}
