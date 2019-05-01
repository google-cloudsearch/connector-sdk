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
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.template.LocalFileCheckpointHandler.FileHelper;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Properties;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link LocalFileCheckpointHandler}. */

@RunWith(MockitoJUnitRunner.class)
public class LocalFileCheckpointHandlerTest {

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  private static final byte[] golden = "golden".getBytes();

  @Mock private FileHelper mockFileHelper;
  @Mock private File mockCheckpointFile;
  private Properties testProperties;

  @Before
  public void init() throws IOException {
    testProperties = new Properties();
    testProperties.put(
        LocalFileCheckpointHandler.CONNECTOR_CHECKPOINT_DIRECTORY, "checkpointpath");
    Path checkpointPath = createPath("checkpointpath/test_checkpoint");
    when(mockFileHelper.getFile(checkpointPath)).thenReturn(mockCheckpointFile);
    when(mockFileHelper.readFile(mockCheckpointFile)).thenReturn(golden);
    when(mockCheckpointFile.exists()).thenReturn(true);
    when(mockCheckpointFile.isFile()).thenReturn(true);
  }

  private Path createPath(String path) {
    return FileSystems.getDefault().getPath(path);
  }

  @Test
  public void testFromConfigurationNotInitialized() {
    thrown.expect(IllegalStateException.class);
    LocalFileCheckpointHandler.fromConfiguration();
  }

  @Test
  public void testFromConfigurationSuccess() {
    setupConfig.initConfig(new Properties());
    LocalFileCheckpointHandler handler = LocalFileCheckpointHandler.fromConfiguration();
    assertEquals(createPath(
            LocalFileCheckpointHandler.DEFAULT_CHECKPOINT_DIRECTORY + "/test_checkpoint"),
        handler.getCheckpointFilePath("test_checkpoint"));
  }

  @Test
  public void testFromConfigurationSuccessCustomPath() {
    Properties properties = new Properties();
    properties.put(LocalFileCheckpointHandler.CONNECTOR_CHECKPOINT_DIRECTORY, "./myDir");
    setupConfig.initConfig(properties);
    LocalFileCheckpointHandler handler = LocalFileCheckpointHandler.fromConfiguration();
    assertEquals(createPath("./myDir/test_checkpoint"),
        handler.getCheckpointFilePath("test_checkpoint"));
  }

  @Test
  public void testConstructorNullPath() {
    thrown.expect(NullPointerException.class);
    new LocalFileCheckpointHandler(null, new FileHelper());
  }

  @Test
  public void testConstructorNullFileHelper() {
    thrown.expect(NullPointerException.class);
    new LocalFileCheckpointHandler("validpath", null);
  }

  @Test
  public void testConstructor() {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("checkpointpath", new FileHelper());
    assertEquals(createPath("checkpointpath/test_checkpoint"),
        handler.getCheckpointFilePath("test_checkpoint"));
  }

  @Test
  public void testConstructorEmptyPath() {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("", new FileHelper());
    assertEquals(createPath("test_checkpoint"),
        handler.getCheckpointFilePath("test_checkpoint"));
  }

  @Test
  public void testReadCheckpointFileNotExist() throws IOException {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("checkpointpath", mockFileHelper);
    when(mockCheckpointFile.exists()).thenReturn(false);
    assertNull(handler.readCheckpoint("test_checkpoint"));
  }

  @Test
  public void testReadCheckpointNotFile() throws IOException {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("checkpointpath", mockFileHelper);
    when(mockCheckpointFile.isFile()).thenReturn(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("checkpoint file is not pointing to file");
    handler.readCheckpoint("test_checkpoint");
  }

  @Test
  public void testReadCheckpointFileSuccess() throws IOException {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("checkpointpath", mockFileHelper);
    assertEquals(golden, handler.readCheckpoint("test_checkpoint"));
  }

  @Test
  public void testReadMultipleCheckpoints() throws IOException {
    File fooFile = mock(File.class);
    when(fooFile.exists()).thenReturn(true);
    when(fooFile.isFile()).thenReturn(true);
    when(fooFile.toPath()).thenReturn(createPath("path/foo.txt"));
    File barFile = mock(File.class);
    when(barFile.exists()).thenReturn(true);
    when(barFile.isFile()).thenReturn(true);
    when(barFile.toPath()).thenReturn(createPath("path/bar.txt"));
    when(mockFileHelper.getFile(fooFile.toPath())).thenReturn(fooFile);
    when(mockFileHelper.getFile(barFile.toPath())).thenReturn(barFile);
    when(mockFileHelper.readFile(fooFile)).thenReturn("foo".getBytes());
    when(mockFileHelper.readFile(barFile)).thenReturn("bar".getBytes());

    LocalFileCheckpointHandler handler = new LocalFileCheckpointHandler("path", mockFileHelper);
    assertEquals("foo", new String(handler.readCheckpoint("foo.txt")));
    assertEquals("bar", new String(handler.readCheckpoint("bar.txt")));
  }

  @Test
  public void testSaveCheckpointNotFile() throws IOException {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("checkpointpath", mockFileHelper);
    when(mockCheckpointFile.isFile()).thenReturn(false);
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("checkpoint file is not pointing to file");
    handler.saveCheckpoint("test_checkpoint", golden);
  }

  @Test
  public void testSaveCheckpointNullPayloadDeleteFile() throws IOException {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("checkpointpath", mockFileHelper);
    handler.saveCheckpoint("test_checkpoint", null);
    verify(mockCheckpointFile).delete();
  }

  @Test
  public void testSaveCheckpointNullPayloadFileNotExist() throws IOException {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("checkpointpath", mockFileHelper);
    when(mockCheckpointFile.exists()).thenReturn(false);
    handler.saveCheckpoint("test_checkpoint", null);
    InOrder inOrder = Mockito.inOrder(mockCheckpointFile);
    inOrder.verify(mockCheckpointFile).exists();
    verifyNoMoreInteractions(mockCheckpointFile);
  }

  @Test
  public void testSaveCheckpointGolden() throws IOException {
    LocalFileCheckpointHandler handler =
        new LocalFileCheckpointHandler("checkpointpath", mockFileHelper);
    when(mockCheckpointFile.exists()).thenReturn(false);
    handler.saveCheckpoint("test_checkpoint", golden);
    verify(mockFileHelper).writeFile(mockCheckpointFile, golden);
  }
}
