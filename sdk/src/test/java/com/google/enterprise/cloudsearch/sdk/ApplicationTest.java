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
package com.google.enterprise.cloudsearch.sdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.util.concurrent.Service.State;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.Application.ApplicationHelper;
import com.google.enterprise.cloudsearch.sdk.Application.ShutdownHook;
import com.google.enterprise.cloudsearch.sdk.ConnectorScheduler.ShutdownHolder;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import java.io.File;
import java.io.IOException;
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

/** Tests for {@link Application}. */
@RunWith(MockitoJUnitRunner.class)
public class ApplicationTest {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock Application<ApplicationHelper, ConnectorContext> mockApplication;
  @Mock ApplicationHelper mockHelper;
  @Mock Connector<ConnectorContext> mockConnector;
  @Mock ConnectorContext mockContext;
  @Mock ConnectorContextImpl.Builder mockContextBuilder;
  @Mock ConnectorScheduler.Builder mockTraverserBuilder;
  @Mock ConnectorScheduler mockTraverser;
  @Mock ExceptionHandler mockExceptionHandler;
  @Mock File mockFile;
  @Mock Runtime mockRuntime;
  @Mock Thread mockShutdownThread;
  @Spy Logger spyLogger = Logger.getLogger(getClass().getName());

  @Before
  @SuppressWarnings("unchecked")
  public void init() throws Exception {
    // General mock initialization
    when(mockHelper.createContextBuilderInstance()).thenReturn(mockContextBuilder);
    when(mockHelper.createShutdownHookThread(any())).thenReturn(mockShutdownThread);
    when(mockTraverserBuilder.setConnector(any())).thenReturn(mockTraverserBuilder);
    when(mockTraverserBuilder.setContext(any())).thenReturn(mockTraverserBuilder);
    when(mockTraverserBuilder.setShutdownHolder(any())).thenReturn(mockTraverserBuilder);
    when(mockTraverserBuilder.build()).thenReturn(mockTraverser);
    when(mockHelper.createSchedulerBuilderInstance()).thenReturn(mockTraverserBuilder);

    when(mockHelper.getRuntimeInstance()).thenReturn(mockRuntime);
    when(mockContextBuilder.build()).thenReturn(mockContext);

    // Inject mocks / spies
    Application.setLogger(spyLogger);
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
    Application<ApplicationHelper, ConnectorContext> subject =
        new Application.Builder(mockConnector, new String[] {})
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

  @Test
  public void startShouldFailOnInterruptedException() throws Exception {
    doThrow(InterruptedException.class).when(mockConnector).init(mockContext);
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, InterruptedException.class);
  }

  @Test
  public void startShouldFailOnStartupExceptionOnInitContext() throws Exception {
    doThrow(StartupException.class).when(mockConnector).init(mockContext);
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class);
  }

  @Test
  public void startShouldFailOnRuntimeExceptionOnInitContext() throws Exception {
    doThrow(RuntimeException.class).when(mockConnector).init(mockContext);
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class);
  }

  @Test
  public void startShouldFailWithStartupExceptionWhenContextBuildException() throws Exception {
    doThrow(RuntimeException.class).when(mockContextBuilder).build();
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class);
  }

  @Test
  public void startShouldFailIfCannotRetryOnException() throws Exception {
    doThrow(IOException.class).when(mockConnector).init(mockContext);
    when(mockHelper.getDefaultExceptionHandler()).thenReturn(mockExceptionHandler);
    when(mockExceptionHandler.handleException(any(IOException.class), anyInt())).thenReturn(false);
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class);
  }

  @Test
  public void startShouldRetryUntilItCanThenFailOnException() throws Exception {
    IOException ex = new IOException();
    doThrow(ex).when(mockConnector).init(mockContext);
    when(mockHelper.getDefaultExceptionHandler()).thenReturn(mockExceptionHandler);
    when(mockExceptionHandler.handleException(ex, 1)).thenReturn(true);
    when(mockExceptionHandler.handleException(ex, 2)).thenReturn(false);
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class);
  }

  @Test
  public void startShouldSucceedWhenNoFurtherExceptionOccursAfterRetry() throws Exception {
    IOException ex = new IOException();
    // Throw exception on first call, then finish without exception on 2nd call
    doThrow(ex).doNothing().when(mockConnector).init(mockContext);
    when(mockHelper.getDefaultExceptionHandler()).thenReturn(mockExceptionHandler);
    when(mockExceptionHandler.handleException(ex, 1)).thenReturn(true);
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    subject.start();
    InOrder inOrder =
        inOrder(
            mockExceptionHandler,
            mockHelper,
            mockTraverserBuilder,
            mockTraverser);
    inOrder.verify(mockHelper).createShutdownHookThread(any(ShutdownHook.class));
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
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
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
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
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
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    subject.start();
    thrown.expect(IllegalStateException.class);
    subject.start();
  }

  @Test
  public void shutdownWithNotRunningTraverser() throws InterruptedException {
    when(mockTraverser.isStarted()).thenReturn(false);
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
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
    Application subject =
        new Application.Builder(mockConnector, new String[] {})
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

  private static void validateStartError(Application subject, Class<? extends Exception> causeType)
      throws InterruptedException {
    try {
      subject.start();
    } catch (IllegalStateException e) {
      validateStartFailure(e, causeType);
      return;
    } finally {
      assertEquals(State.FAILED, subject.state());
    }
    // missed expected exception
    fail(causeType + " was expected");
  }

  private static void validateStartFailure(
      IllegalStateException e, Class<? extends Exception> causeType) {
    assertEquals(causeType, e.getCause().getClass());
  }
}
