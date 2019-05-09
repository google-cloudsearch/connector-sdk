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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Object used to manage operation execution statistics.
 *
 * <p>Logically structured to look like (in RESTful representation):
 * <pre>
 *   /                                   - list of components
 *   /:component
 *   /:component/:operation              - long (counter)
 *   /:component/:event
 *                    |success           - long (counter)
 *                    |failure           - long (counter)
 *                    |latency           - map of &lt;latency_range, counts&gt;
 *              /:operationWithVariants  - map of &lt;variant, counts&gt;
 *  </pre>
 *
 * Example:
 * <pre>
 *   /requests/SendFeeds    - returns {CODE_OK: 155, CODE_404: 105}
 *  </pre>
 */
// TODO(imysak): make StatsManager configurable
public class StatsManager {

  // scaling range for latency metrics
  private final NavigableSet<Long> latencyRange =
      Collections.unmodifiableNavigableSet(new TreeSet<>(Arrays.asList(
          0L, 10L, 50L, 100L, 200L, 300L, 500L, 1000L,
          1500L, 2000L, 3000L, 5000L, 10000L, 20000L, 30000L, 60000L)));

  // collection that store OperationStats, stored in a map where the key is a component name
  private ConcurrentMap<String, OperationStats> stats = new ConcurrentHashMap<>();
  // true if stats is active and we store all incoming events/operations
  private boolean running = true;

  private static class InstanceHolder {
    private static final StatsManager instance = new StatsManager();
  }

  private StatsManager() {}

  /**
   * Returns a singleton reference to this object.
   */
  public static StatsManager getInstance() {
    return InstanceHolder.instance;
  }

  public static List<String> getComponentNames() {
    return getInstance().stats.keySet().stream().sorted().collect(Collectors.toList());
  }

  /**
   * Retrieves the {@link OperationStats} instance for the desired component.
   *
   * @param component - name of component that defines the name space of its statistics
   * @return OperationStats object for specified component (create instance if needed)
   */
  public static OperationStats getComponent(String component) {
    // atomic, same as putIfAbsent, but with lambda interface, so we can avoid creating objects
    return getInstance()
        .stats
        .computeIfAbsent(component, key -> new OperationStats(getInstance()));
  }

  /**
   * Stops capturing all statistics.
   */
  public synchronized void stop() {
    running = false;
    stats.forEach((component, opStats) -> opStats.stop());
  }

  /**
   * Starts/resumes capturing statistics.
   */
  public synchronized void resume() {
    running = true;
    stats.forEach((component, opStats) -> opStats.start());
  }

  /**
   * Determines whether the statistic capture is active.
   */
  public synchronized boolean isRunning() {
    return running;
  }

  public synchronized Object takeSnapshot() {
    // TODO(imysak): implement this
    throw new UnsupportedOperationException("not implemented yet");
  }

  @SuppressWarnings("unused")
  public synchronized void mergeWithSnapshot(Object snapshot) {
    // TODO(imysak): implement this (and remove SuppressWarnings)
    throw new UnsupportedOperationException("not implemented yet");
  }

  @SuppressWarnings("unused")
  public synchronized void startFromSnapshot(Object snapshot) {
    // TODO(imysak): implement this (and remove SuppressWarnings)
    throw new UnsupportedOperationException("not implemented yet");
  }

  /**
   * Builds human-readable statistics string to write in logs.
   */
  //TODO(imysak): build JSON object instead of string
  public String printStats() {
    StringBuilder sb = new StringBuilder();
    sb.append("Stats(active:").append(running).append("):\n");
    stats.forEach(
        (component, stats) -> {
          sb.append("  Component: ").append(component).append('\n');
          stats.printStats(sb);
          sb.append('\n');
        });

    return sb.toString();
  }

  /**
   * Object used to log events, operations, and actions
   */
  public static class OperationStats {

    // reference on scaling range for latency metrics
    private NavigableSet<Long> latencyRange;

    /**
     * map that present dispersion response time granulated by specific scale {@link #latencyRange}
     * per operation(key of map)
     * <pre>map < operation, map < scaling_key, count >></pre>
     */
    private final ConcurrentMap<String, Multiset<Long>> latency = new ConcurrentHashMap<>();
    /**
     * map that present counts of success-ended operations
     */
    private final Multiset<String> successCounter = ConcurrentHashMultiset.create();
    /**
     * map that present counts of failure-ended operations
     */
    private final Multiset<String> failureCounter = ConcurrentHashMultiset.create();
    /**
     * map that present counts of registered not-traced operations
     */
    private final Multiset<String> opCounter = ConcurrentHashMultiset.create();
    /**
     * map that present counts of different results on operation
     * <pre>map < operation, map < result, count >></pre>
     */
    private final ConcurrentMap<String, Multiset<String>> opWithResult = new ConcurrentHashMap<>();

    /** <i>true</i> if component is active and we store all income events/operations */
    private boolean running;

    private OperationStats(StatsManager manager) {
      this.running = manager.running;
      this.latencyRange = manager.latencyRange;
    }

    private void start() {
      running = true;
    }

    private void stop() {
      running = false;
    }

    public <T extends Enum<?>> Event event(T operation) {
      return event(operation.name());
    }

    public Event event(String operation) {
      if (running) {
        return new SimpleEvent(operation);
      } else {
        return FAKE_EVENT;
      }
    }

    public void register(String operation) {
      if (running) {
        opCounter.add(operation);
      }
    }

    public int getSuccessCount(String operation) {
      return successCounter.count(operation);
    }

    @VisibleForTesting
    int getRegisteredCount(String operation) {
      return opCounter.count(operation);
    }

    @VisibleForTesting
    int getFailureCount(String operation) {
      return failureCounter.count(operation);
    }

    public void logResult(String operation, String result) {
      if (running) {
        Multiset<String> multiset =
            opWithResult.computeIfAbsent(operation, op -> ConcurrentHashMultiset.create());
        multiset.add(result);
      }
    }

    @VisibleForTesting
    public int getLogResultCounter(String operation, String result) {
      Multiset<String> multiset = opWithResult.get(operation);
      return multiset != null ? multiset.count(result) : 0;
    }

    public void clear() {
      opWithResult.clear();
      failureCounter.clear();
      successCounter.clear();
      latency.clear();
      opCounter.clear();
    }

    private void printStats(StringBuilder sb) {
      sb.append("\tRegistered operations:\n");
      opCounter
          .elementSet()
          .forEach(
              op -> {
                sb.append("\t\t").append(op).append(" ").append(opCounter.count(op)).append('\n');
              });
      sb.append('\n');
      sb.append("\tRegistered operations with responses:\n");
      opWithResult.forEach(
          (op, results) -> {
            sb.append("\t\t").append(op).append('\n');
            results
                .elementSet()
                .forEach(
                    value -> {
                      sb.append("\t\t\t")
                          .append(value)
                          .append(" : ")
                          .append(results.count(value))
                          .append('\n');
                    });
          });
      sb.append('\n');
      sb.append("\tOperations completed with success:\n");
      successCounter
          .elementSet()
          .forEach(
              op -> {
                sb.append("\t\t")
                    .append(op)
                    .append(" : ")
                    .append(successCounter.count(op))
                    .append('\n');
              });
      sb.append('\n');
      sb.append("\tOperations completed with failure:\n");
      failureCounter
          .elementSet()
          .forEach(
              op -> {
                sb.append("\t\t")
                    .append(op)
                    .append(" : ")
                    .append(failureCounter.count(op))
                    .append('\n');
              });
      sb.append('\n');
      sb.append("\tResponse latency on operations divided by range:\n");
      latency.forEach(
          (op, results) -> {
            sb.append("\t\t").append(op).append('\n');
            results
                .elementSet()
                .forEach(
                    value -> {
                      sb.append("\t\t\t")
                          .append(value)
                          .append(" = ")
                          .append(results.count(value))
                          .append('\n');
                    });
          });
    }

    /**
     * Counter for single operation to wrap Stopwatch
     */
    public interface Event {

      /**
       * Starts the stopwatch.
       *
       * @return a reference to this object.
       */
      public Event start();

      /**
       * Stops the stopwatch and stores stats for the operation in the related component.
       *
       * @param success - results of operation that is stored
       */
      public void end(boolean success);

      /**
       * Same as {@code end(true)}.
       */
      public void success();

      /**
       * same as {@code end(false)}.
       */
      public void failure();
    }

    private static final Event FAKE_EVENT = new FakeEvent();

    private static class FakeEvent implements Event {

      @Override
      public Event start() {
        return this;
      }

      @Override
      public void success() {}

      @Override
      public void failure() {}

      @Override
      public void end(boolean success) {}
    }

    /** Counter for single operation to wrap Stopwatch */
    private class SimpleEvent implements Event {
      private final String op;
      private final Stopwatch watch;

      private SimpleEvent(String op) {
        this.op = op;
        this.watch = Stopwatch.createUnstarted();
      }

      @Override
      public Event start() {
        watch.start();
        return this;
      }

      @Override
      public void success() {
        this.end(true);
      }

      @Override
      public void failure() {
        this.end(false);
      }

      @Override
      public void end(boolean success) {
        if (watch.isRunning()) {
          watch.stop();
        }
        if (success) {
          OperationStats.this.successCounter.add(op);
          Multiset<Long> latency =
              OperationStats.this.latency.computeIfAbsent(
                  op, o -> ConcurrentHashMultiset.create());

          long elapsed = watch.elapsed(TimeUnit.MILLISECONDS);
          latency.add(keyFromRange(elapsed));
        } else {
          OperationStats.this.failureCounter.add(op);
        }
      }

      private Long keyFromRange(long latency) {
        Long top = OperationStats.this.latencyRange.ceiling(latency);
        return top != null ? top : Long.MAX_VALUE;
      }
    }
  }

  private static synchronized void resetStatsManager() {
    // Not clearing out or reinitializing stats map here. There may have been static
    // references initialized for OperationStats. We are just clearing out values recorded under
    // OperationStats.
    for (ConcurrentMap.Entry<String, OperationStats> entry : getInstance().stats.entrySet()) {
      entry.getValue().clear();
    }
  }

  /**
   * {@link TestRule} to reset static {@link StatsManager} object for unit tests.
   */
  public static class ResetStatsRule implements TestRule {

    @Override
    public Statement apply(Statement base, Description description) {
      resetStatsManager();
      return base;
    }
  }
}
