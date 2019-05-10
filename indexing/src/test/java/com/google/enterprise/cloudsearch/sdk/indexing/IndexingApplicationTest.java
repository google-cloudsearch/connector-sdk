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
package com.google.enterprise.cloudsearch.sdk.indexing;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudsearch.v1.model.Schema;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.Service.State;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.Application.ShutdownHook;
import com.google.enterprise.cloudsearch.sdk.ConnectorScheduler.ShutdownHolder;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.InvalidConfigurationException;
import com.google.enterprise.cloudsearch.sdk.LocalFileCredentialFactory;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.indexing.IndexingApplication.ApplicationHelper;
import com.google.enterprise.cloudsearch.sdk.indexing.StructuredData.ResetStructuredDataRule;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

/** Tests for {@link IndexingApplication}. */

@RunWith(MockitoJUnitRunner.class)
public class IndexingApplicationTest {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();
  @Rule public ResetStructuredDataRule resetStructuredData = new ResetStructuredDataRule();

  @Mock IndexingApplication mockApplication;
  @Mock ApplicationHelper mockHelper;
  @Mock IndexingConnector mockConnector;
  @Mock IndexingConnectorContext mockContext;
  @Mock IndexingConnectorContextImpl.Builder mockContextBuilder;
  @Mock ConnectorTraverser.Builder mockTraverserBuilder;
  @Mock ConnectorTraverser mockTraverser;
  @Mock ExceptionHandler mockExceptionHandler;
  @Mock File mockFile;
  @Mock Runtime mockRuntime;
  @Mock Thread mockShutdownThread;
  @Mock CredentialFactory mockCredentialFactory;
  @Mock IndexingService mockIndexingService;
  @Mock GoogleCredential credential;
  @Spy Logger spyLogger = Logger.getLogger(getClass().getName());

  @Before
  @SuppressWarnings("unchecked")
  public void init() throws Exception {
    // General mock initialization
    when(mockContextBuilder.setIndexingService(any())).thenReturn(mockContextBuilder);
    when(mockHelper.createContextBuilderInstance()).thenReturn(mockContextBuilder);
    when(mockHelper.createShutdownHookThread(any())).thenReturn(mockShutdownThread);
    when(mockTraverserBuilder.setConnector(any())).thenReturn(mockTraverserBuilder);
    when(mockTraverserBuilder.setContext(any())).thenReturn(mockTraverserBuilder);
    when(mockTraverserBuilder.setShutdownHolder(any())).thenReturn(mockTraverserBuilder);
    when(mockTraverserBuilder.build()).thenReturn(mockTraverser);
    when(mockHelper.createSchedulerBuilderInstance()).thenReturn(mockTraverserBuilder);
    when(mockHelper.getRuntimeInstance()).thenReturn(mockRuntime);
    when(mockContextBuilder.build()).thenReturn(mockContext);
    when(mockIndexingService.startAsync()).thenReturn(mockIndexingService);
    Schema schema = new Schema().setObjectDefinitions(Collections.emptyList());
    when(mockIndexingService.getSchema()).thenReturn(schema);

    // Inject mocks / spies
    IndexingApplication.setLogger(spyLogger);
    Properties requiredProperties = new Properties();
    File serviceAcctFile = temporaryFolder.newFile("serviceaccount.json");
    requiredProperties.put(
        LocalFileCredentialFactory.SERVICE_ACCOUNT_KEY_FILE_CONFIG,
        serviceAcctFile.getAbsolutePath());
    requiredProperties.put("api.sourceId", "sourceId");
    requiredProperties.put("api.identitySourceId", "identitySourceId");
    setupConfig.initConfig(requiredProperties);
  }

  @Test
  public void createApplicationAndFullConnectorLifecycle() throws Exception {
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    // Stub helper.createTraverserInstance(..) to shoot a traverse callback immediately
    doAnswer(
            invocation -> {
              mockConnector.traverse();
              return mockTraverser;
            })
        .when(mockTraverser)
        .start();

    subject.start();

    // Verify order of callbacks to Connector
    InOrder inOrder = inOrder(mockConnector);
    inOrder.verify(mockConnector).init(mockContext);
    inOrder.verify(mockConnector).traverse();

    subject.shutdown("From Test");
    inOrder.verify(mockConnector).destroy();
  }

  // TODO(jlacey): Add a test to confirm that the default ID is use in API requests.
  @Test
  public void defaultIdIsConnectorClassName() {
    // Use a real implementation class here instead of always using mocks.
    // (They don't actually behave differently.)
    class TestConnector implements IndexingConnector {
      @Override public void init(IndexingConnectorContext context) {}
      @Override public void traverse() throws IOException, InterruptedException {}
      @Override public void saveCheckpoint(boolean isShutdown) {}
      @Override public void destroy() {}
    }
    IndexingConnector connector = new TestConnector();
    assertEquals(connector.getClass().getName(), connector.getDefaultId());
  }

  @Test
  public void startShouldFailOnInterruptedException() throws Exception {
    doThrow(InterruptedException.class).when(mockConnector).init(mockContext);
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, InterruptedException.class, null);
  }

  @Test
  public void startShouldFailOnStartupExceptionOnInitContext() throws Exception {
    doThrow(StartupException.class).when(mockConnector).init(mockContext);
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class, null);
  }

  @Test
  public void startShouldFailOnRuntimeExceptionOnInitContext() throws Exception {
    doThrow(RuntimeException.class).when(mockConnector).init(mockContext);
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class, null);
  }

  @Test
  public void startShouldFailOnStartupExceptionOnFailedStructuredDataInit() throws Exception {
    when(mockIndexingService.getSchema()).thenThrow(new IOException("failed to get schema"));
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class, "Failed to initialize StructuredData");
  }

  @Test
  public void startShouldFailWithStartupExceptionWhenContextBuildException() throws Exception {
    doThrow(RuntimeException.class).when(mockContextBuilder).build();
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class, null);
  }

  @Test
  public void startShouldFailIfCannotRetryOnException() throws Exception {
    doThrow(IOException.class).when(mockConnector).init(mockContext);
    when(mockHelper.getDefaultExceptionHandler()).thenReturn(mockExceptionHandler);
    when(mockExceptionHandler.handleException(any(IOException.class), anyInt())).thenReturn(false);
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class, null);
  }

  @Test
  public void startShouldFailOnInvalidConfigurationExceptionDuringConnectorInit() throws Exception {
    doThrow(InvalidConfigurationException.class).when(mockConnector).init(mockContext);
    when(mockIndexingService.isRunning()).thenReturn(true);
    when(mockIndexingService.stopAsync()).thenReturn(mockIndexingService);
    IndexingApplication subject =
        spy(
            new IndexingApplication.Builder(mockConnector, new String[] {})
                .setIndexingService(mockIndexingService)
                .setHelper(mockHelper)
                .build());
    validateStartError(subject, InvalidConfigurationException.class, null);
    verify(mockIndexingService).stopAsync();
    verify(mockIndexingService).awaitTerminated();
  }

  @Test
  public void startShouldRetryUntilItCanThenFailOnException() throws Exception {
    IOException ex = new IOException();
    doThrow(ex).when(mockConnector).init(mockContext);
    when(mockHelper.getDefaultExceptionHandler()).thenReturn(mockExceptionHandler);
    when(mockExceptionHandler.handleException(ex, 1)).thenReturn(true);
    when(mockExceptionHandler.handleException(ex, 2)).thenReturn(false);
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class, null);
  }

  @Test
  public void startShouldSucceedWhenNoFurtherExceptionOccursAfterRetry() throws Exception {
    IOException ex = new IOException();
    // Throw exception on first call, then finish without exception on 2nd call
    doThrow(ex).doNothing().when(mockConnector).init(mockContext);
    when(mockHelper.getDefaultExceptionHandler()).thenReturn(mockExceptionHandler);
    when(mockExceptionHandler.handleException(ex, 1)).thenReturn(true);
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    InOrder inOrder =
        inOrder(
            mockExceptionHandler,
            mockIndexingService,
            mockHelper,
            mockTraverserBuilder,
            mockTraverser);
    inOrder.verify(mockHelper).createShutdownHookThread(any(ShutdownHook.class));
    inOrder.verify(mockIndexingService).startAsync();
    inOrder.verify(mockIndexingService).awaitRunning();
    inOrder.verify(mockIndexingService).getSchema();
    inOrder.verify(mockExceptionHandler).handleException(ex, 1);
    inOrder.verify(mockHelper).createSchedulerBuilderInstance();
    inOrder.verify(mockTraverserBuilder).setConnector(eq(mockConnector));
    inOrder.verify(mockTraverserBuilder).setContext(eq(mockContext));
    inOrder.verify(mockTraverserBuilder).setShutdownHolder(any());
    inOrder.verify(mockTraverser).start();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void startSuccessful() throws Exception {
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    InOrder inOrder = inOrder(mockRuntime, mockHelper, mockTraverserBuilder, mockTraverser);
    inOrder.verify(mockHelper).createShutdownHookThread(any(ShutdownHook.class));
    inOrder.verify(mockHelper).getRuntimeInstance();
    inOrder.verify(mockRuntime).addShutdownHook(mockShutdownThread);
    inOrder.verify(mockHelper).createContextBuilderInstance();
    inOrder.verify(mockHelper).createSchedulerBuilderInstance();
    inOrder.verify(mockTraverserBuilder).setConnector(eq(mockConnector));
    inOrder.verify(mockTraverserBuilder).setContext(eq(mockContext));
    inOrder.verify(mockTraverserBuilder).setShutdownHolder(any());
    inOrder.verify(mockTraverser).start();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void startAndStopFromTraverser() throws Exception {
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();

    SettableFuture<ShutdownHolder> function = SettableFuture.create();
    Mockito.when(mockTraverserBuilder.setShutdownHolder(any()))
        .thenAnswer(invocation -> {
          Object[] args = invocation.getArguments();
          function.set((ShutdownHolder) args[0]);
          return mockTraverserBuilder;
        });

    subject.start();
    function.get(1L, TimeUnit.SECONDS).shutdown();
    verify(mockShutdownThread).start();
  }

  @Test
  public void multipleStarts() throws Exception {
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    thrown.expect(IllegalStateException.class);
    subject.start();
  }

  @Test
  public void shutdownWithNotRunningTraverser() throws InterruptedException {
    when(mockTraverser.isStarted()).thenReturn(false);
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    subject.shutdown("TestEvent");
    InOrder inOrder = inOrder(spyLogger, mockTraverser, mockConnector);
    inOrder.verify(spyLogger).log(eq(Level.INFO), any(), anyString());
    inOrder.verify(mockTraverser).isStarted();
    inOrder.verify(mockConnector).destroy();
  }

  @Test
  public void shutdownWithRunningTraverser() throws InterruptedException {
    when(mockTraverser.isStarted()).thenReturn(true);
    IndexingApplication subject =
        new IndexingApplication.Builder(mockConnector, new String[] {})
            .setIndexingService(mockIndexingService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    subject.shutdown("TestEvent");
    InOrder inOrder = inOrder(spyLogger, mockTraverser, mockConnector);
    inOrder.verify(spyLogger).log(eq(Level.INFO), any(), anyString());
    inOrder.verify(mockTraverser).isStarted();
    inOrder.verify(mockTraverser).stop();
    inOrder.verify(mockConnector).destroy();
  }

  @Test
  public void shutdownHookTaskShouldCallShutdown() {
    ShutdownHook shutdownHook = mockApplication.new ShutdownHook();
    shutdownHook.run();
    verify(mockApplication).shutdown(any());
  }

  private static void validateStartError(
      IndexingApplication subject, Class<? extends Exception> causeType, String errorMessage)
      throws InterruptedException {
    try {
      subject.start();
    } catch (IllegalStateException e) {
      validateStartFailure(e, causeType, errorMessage);
      return;
    } finally {
      assertEquals(State.FAILED, subject.state());
    }
    // missed expected exception
    fail(causeType + " was expected");
  }

  private static void validateStartFailure(
      IllegalStateException e, Class<? extends Exception> causeType, String errorMessage) {
    assertEquals(causeType, e.getCause().getClass());
    if (!Strings.isNullOrEmpty(errorMessage)) {
      assertThat(e.getCause().getMessage(), containsString(errorMessage));
    }
  }
}
