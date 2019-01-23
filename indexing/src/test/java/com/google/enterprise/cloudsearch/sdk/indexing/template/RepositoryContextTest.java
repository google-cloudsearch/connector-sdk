package com.google.enterprise.cloudsearch.sdk.indexing.template;


import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

import com.google.api.client.json.GenericJson;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RepositoryContextTest {

  @Mock
  private EventBus mockEventBus;
  private RepositoryContext repositoryContext;

  private static final String DOC_ID = "134";

  @Before
  public void setup() {
    repositoryContext = new RepositoryContext.Builder()
        .setEventBus(mockEventBus).build();
  }

  @Test
  public void invokePostApiOperationAsync_resultIsSet() throws Exception {
    ListenableFuture<List<GenericJson>> result = Futures.immediateFuture(ImmutableList.of());
    doAnswer(invocation -> {
      AsyncApiOperation asyncApiOperation = invocation.getArgument(0);
      asyncApiOperation.setResult(result);
      return null;
    }).when(mockEventBus).post(any());

    ApiOperation apiOperation = ApiOperations.deleteItem(DOC_ID);
    ListenableFuture<List<GenericJson>> res = repositoryContext.postApiOperationAsync(apiOperation);

    ArgumentCaptor<AsyncApiOperation> argumentCaptor =
        ArgumentCaptor.forClass(AsyncApiOperation.class);
    verify(mockEventBus).post(argumentCaptor.capture());
    assertSame(argumentCaptor.getValue().getOperation(), apiOperation);
    assertSame(result.get(), res.get());
  }
}
