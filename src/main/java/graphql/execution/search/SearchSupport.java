package graphql.execution.search;

import graphql.Directives;
import graphql.ExecutionResult;
import graphql.Internal;
import graphql.execution.MergedField;
import graphql.execution.defer.DeferredCall;
import graphql.execution.reactive.SingleSubscriberPublisher;
import graphql.language.Field;
import org.reactivestreams.Publisher;

import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

@Internal
public class SearchSupport {

  private final AtomicBoolean searchDetected = new AtomicBoolean(false);
  //private final Deque<DeferredCall> deferredCalls = new ConcurrentLinkedDeque<>();
  private final SingleSubscriberPublisher<ExecutionResult> publisher = new SingleSubscriberPublisher<>();

  public boolean checkForSearchDirective(MergedField currentField) {
    for (Field field : currentField.getFields()) {
      if (field.getDirective(Directives.DeferDirective.getName()) != null) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private void drainDeferredCalls() {
    if (deferredCalls.isEmpty()) {
      publisher.noMoreData();
      return;
    }
    DeferredCall deferredCall = deferredCalls.pop();
    CompletableFuture<ExecutionResult> future = deferredCall.invoke();
    future.whenComplete((executionResult, exception) -> {
      if (exception != null) {
        publisher.offerError(exception);
        return;
      }
      publisher.offer(executionResult);
      drainDeferredCalls();
    });
  }

  public void enqueue(DeferredCall deferredCall) {
    searchDetected.set(true);
    deferredCalls.offer(deferredCall);
  }

  public boolean isDeferDetected() {
    return searchDetected.get();
  }

  /**
   * When this is called the deferred execution will begin
   *
   * @return the publisher of deferred results
   */
  public Publisher<ExecutionResult> startDeferredCalls() {
    drainDeferredCalls();
    return publisher;
  }
}
