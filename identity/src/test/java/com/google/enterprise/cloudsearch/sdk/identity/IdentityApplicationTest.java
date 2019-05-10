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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.util.concurrent.Service.State;
import com.google.common.util.concurrent.SettableFuture;
import com.google.enterprise.cloudsearch.sdk.Application.ShutdownHook;
import com.google.enterprise.cloudsearch.sdk.ConnectorScheduler.ShutdownHolder;
import com.google.enterprise.cloudsearch.sdk.CredentialFactory;
import com.google.enterprise.cloudsearch.sdk.ExceptionHandler;
import com.google.enterprise.cloudsearch.sdk.LocalFileCredentialFactory;
import com.google.enterprise.cloudsearch.sdk.StartupException;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.ResetConfigRule;
import com.google.enterprise.cloudsearch.sdk.config.Configuration.SetupConfigRule;
import com.google.enterprise.cloudsearch.sdk.identity.IdentityApplication.ApplicationHelper;
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

/** Tests for {@link IdentityApplication}. */

@RunWith(MockitoJUnitRunner.class)
public class IdentityApplicationTest {
  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  @Rule public ExpectedException thrown = ExpectedException.none();
  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
  @Rule public ResetConfigRule resetConfig = new ResetConfigRule();
  @Rule public SetupConfigRule setupConfig = SetupConfigRule.uninitialized();

  @Mock IdentityApplication mockApplication;
  @Mock ApplicationHelper mockHelper;
  @Mock IdentityConnector mockConnector;
  @Mock IdentityConnectorContext mockContext;
  @Mock IdentityConnectorContextImpl.Builder mockContextBuilder;
  @Mock IdentityScheduler.Builder identitySchedulerBuilder;
  @Mock IdentityScheduler identityScheduler;
  @Mock ExceptionHandler mockExceptionHandler;
  @Mock File mockFile;
  @Mock Runtime mockRuntime;
  @Mock Thread mockShutdownThread;
  @Mock CredentialFactory mockCredentialFactory;
  @Mock IdentityService mockIdentityService;
  @Mock GoogleCredential credential;
  @Spy Logger spyLogger = Logger.getLogger(getClass().getName());

  private static final CredentialFactory credentialFactory =
      scopes -> new MockGoogleCredential.Builder()
          .setTransport(GoogleNetHttpTransport.newTrustedTransport())
          .setJsonFactory(JSON_FACTORY)
          .build();

  @Before
  @SuppressWarnings("unchecked")
  public void init() throws Exception {
    // General mock initialization
    when(mockContextBuilder.setIdentityService(any())).thenReturn(mockContextBuilder);
    when(mockHelper.createContextBuilderInstance()).thenReturn(mockContextBuilder);
    when(mockHelper.createCredentialFactory()).thenReturn(credentialFactory);
    when(mockHelper.createShutdownHookThread(any())).thenReturn(mockShutdownThread);
    when(identitySchedulerBuilder.setConnector(any())).thenReturn(identitySchedulerBuilder);
    when(identitySchedulerBuilder.setContext(any())).thenReturn(identitySchedulerBuilder);
    when(identitySchedulerBuilder.setShutdownHolder(any())).thenReturn(identitySchedulerBuilder);
    when(identitySchedulerBuilder.build()).thenReturn(identityScheduler);
    when(mockHelper.createSchedulerBuilderInstance()).thenReturn(identitySchedulerBuilder);
    when(mockHelper.getRuntimeInstance()).thenReturn(mockRuntime);
    when(mockContextBuilder.build()).thenReturn(mockContext);
    when(mockIdentityService.startAsync()).thenReturn(mockIdentityService);
    when(mockIdentityService.stopAsync()).thenReturn(mockIdentityService);

    // Inject mocks / spies
    IdentityApplication.setLogger(spyLogger);
    Properties requiredProperties = new Properties();
    File serviceAcctFile = temporaryFolder.newFile("serviceaccount.json");
    requiredProperties.put(
        LocalFileCredentialFactory.SERVICE_ACCOUNT_KEY_FILE_CONFIG,
        serviceAcctFile.getAbsolutePath());
    requiredProperties.put("api.sourceId", "sourceId");
    requiredProperties.put("api.identitySourceId", "identitySourceId");
    requiredProperties.put("api.customerId", "customer1");
    setupConfig.initConfig(requiredProperties);
  }

  @Test
  public void createApplicationAndFullConnectorLifecycle() throws Exception {
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setHelper(mockHelper)
            .build();
    // Stub helper.createTraverserInstance(..) to shoot a traverse callback immediately
    doAnswer(
            invocation -> {
              mockConnector.traverse();
              return identityScheduler;
            })
        .when(identityScheduler)
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
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, InterruptedException.class);
  }

  @Test
  public void startShouldFailOnStartupExceptionOnInitContext() throws Exception {
    doThrow(StartupException.class).when(mockConnector).init(mockContext);
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class);
  }

  @Test
  public void startShouldFailOnRuntimeExceptionOnInitContext() throws Exception {
    doThrow(RuntimeException.class).when(mockConnector).init(mockContext);
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class);
  }

  @Test
  public void startShouldFailWithStartupExceptionWhenContextBuildException() throws Exception {
    doThrow(RuntimeException.class).when(mockContextBuilder).build();
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    validateStartError(subject, StartupException.class);
  }

  @Test
  public void startShouldFailIfCannotRetryOnException() throws Exception {
    doThrow(IOException.class).when(mockConnector).init(mockContext);
    when(mockHelper.getDefaultExceptionHandler()).thenReturn(mockExceptionHandler);
    when(mockExceptionHandler.handleException(any(IOException.class), anyInt())).thenReturn(false);
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
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
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
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
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    InOrder inOrder =
        inOrder(
            mockExceptionHandler,
            mockIdentityService,
            mockHelper,
            identitySchedulerBuilder,
            identityScheduler);
    inOrder.verify(mockHelper).createShutdownHookThread(any(ShutdownHook.class));
    inOrder.verify(mockIdentityService).startAsync();
    inOrder.verify(mockIdentityService).awaitRunning();
    inOrder.verify(mockExceptionHandler).handleException(ex, 1);
    inOrder.verify(mockHelper).createSchedulerBuilderInstance();
    inOrder.verify(identitySchedulerBuilder).setConnector(eq(mockConnector));
    inOrder.verify(identitySchedulerBuilder).setContext(eq(mockContext));
    inOrder.verify(identitySchedulerBuilder).setShutdownHolder(any());
    inOrder.verify(identityScheduler).start();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void startSuccessful() throws Exception {
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    InOrder inOrder = inOrder(mockRuntime, mockHelper, identitySchedulerBuilder, identityScheduler);
    inOrder.verify(mockHelper).createShutdownHookThread(any(ShutdownHook.class));
    inOrder.verify(mockHelper).getRuntimeInstance();
    inOrder.verify(mockRuntime).addShutdownHook(mockShutdownThread);
    inOrder.verify(mockHelper).createContextBuilderInstance();
    inOrder.verify(mockHelper).createSchedulerBuilderInstance();
    inOrder.verify(identitySchedulerBuilder).setConnector(eq(mockConnector));
    inOrder.verify(identitySchedulerBuilder).setContext(eq(mockContext));
    inOrder.verify(identitySchedulerBuilder).setShutdownHolder(any());
    inOrder.verify(identityScheduler).start();
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void startAndStopFromTraverser() throws Exception {
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();

    SettableFuture<ShutdownHolder> function = SettableFuture.create();
    Mockito.when(identitySchedulerBuilder.setShutdownHolder(any()))
        .thenAnswer(invocation -> {
          Object[] args = invocation.getArguments();
          function.set((ShutdownHolder) args[0]);
          return identitySchedulerBuilder;
        });

    subject.start();
    function.get(1L, TimeUnit.SECONDS).shutdown();
    verify(mockShutdownThread).start();
  }

  @Test
  public void multipleStarts() throws Exception {
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    thrown.expect(IllegalStateException.class);
    subject.start();
  }

  @Test
  public void shutdownWithNotRunningTraverser() throws InterruptedException {
    when(identityScheduler.isStarted()).thenReturn(false);
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    subject.shutdown("TestEvent");
    InOrder inOrder = inOrder(spyLogger, identityScheduler, mockConnector);
    inOrder.verify(spyLogger).log(eq(Level.INFO), any(), anyString());
    inOrder.verify(identityScheduler).isStarted();
    inOrder.verify(mockConnector).destroy();
  }

  @Test
  public void shutdownWithRunningTraverser() throws InterruptedException {
    when(identityScheduler.isStarted()).thenReturn(true);
    IdentityApplication subject =
        new IdentityApplication.Builder(mockConnector, new String[] {})
            .setIdentityService(mockIdentityService)
            .setHelper(mockHelper)
            .build();
    subject.start();
    subject.shutdown("TestEvent");
    InOrder inOrder = inOrder(spyLogger, identityScheduler, mockConnector);
    inOrder.verify(spyLogger).log(eq(Level.INFO), any(), anyString());
    inOrder.verify(identityScheduler).isStarted();
    inOrder.verify(identityScheduler).stop();
    inOrder.verify(mockConnector).destroy();
  }

  @Test
  public void shutdownHookTaskShouldCallShutdown() {
    ShutdownHook shutdownHook = mockApplication.new ShutdownHook();
    shutdownHook.run();
    verify(mockApplication).shutdown(any());
  }

  private static void validateStartError(
      IdentityApplication subject, Class<? extends Exception> causeType)
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
