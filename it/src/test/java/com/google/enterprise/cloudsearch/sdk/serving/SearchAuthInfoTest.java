package com.google.enterprise.cloudsearch.sdk.serving;

import static org.junit.Assert.assertEquals;

import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class SearchAuthInfoTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testConstructor_nonexistentClientSecrets() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    new SearchAuthInfo(new File("INVALID_PATH"), tempFolder.newFile("x"), "FAKE_EMAIL");
  }

  @Test
  public void testConstructor_nonexistentCredentialsDirectory() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    new SearchAuthInfo(tempFolder.newFile("FAKE_FILE"), new File("INVALID_PATH"), "FAKE_EMAIL");
  }

  @Test
  public void testConstructor_plainFileAsCredentialsDirectory() throws IOException {
    expectedException.expect(IllegalArgumentException.class);
    new SearchAuthInfo(
        tempFolder.newFile("FAKE_FILE1"), tempFolder.newFile("FAKE_FILE2"), "FAKE_EMAIL");
  }

  @Test
  public void testConstructor_nullUserEmail() throws IOException {
    expectedException.expect(NullPointerException.class);
    new SearchAuthInfo(tempFolder.newFile("FAKE_FILE"), tempFolder.newFolder("FAKE_DIR"), null);
  }

  @Test
  public void testGetters() throws IOException {
    File clientSecrets = tempFolder.newFile("FAKE_FILE");
    File credentialsDirectory = tempFolder.newFolder("FAKE_DIR");
    SearchAuthInfo searchAuthInfo =
        new SearchAuthInfo(clientSecrets, credentialsDirectory, "FAKE_EMAIL");
    assertEquals("FAKE_EMAIL", searchAuthInfo.getUserEmail());
    assertEquals(credentialsDirectory, searchAuthInfo.getCredentialsDirectory());
    Files.write(clientSecrets.toPath(), "FAKE_CONTENT".getBytes());
    assertEquals(
        "FAKE_CONTENT",
        new String(ByteStreams.toByteArray(searchAuthInfo.getClientSecretsStream())));
  }
}
