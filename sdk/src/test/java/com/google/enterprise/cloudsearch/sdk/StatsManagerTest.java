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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats;
import com.google.enterprise.cloudsearch.sdk.StatsManager.OperationStats.Event;
import com.google.enterprise.cloudsearch.sdk.StatsManager.ResetStatsRule;
import java.util.concurrent.TimeUnit;
import org.junit.Rule;
import org.junit.Test;

/** Unit test methods for {@link StatsManager}. */
public class StatsManagerTest {

  @Rule public ResetStatsRule resetStats = new ResetStatsRule();

  /** Test method for {@link com.google.enterprise.cloudsearch.sdk.StatsManager#getInstance()}. */
  @Test
  public void testGetInstance() {
    StatsManager service = StatsManager.getInstance();
    assertNotNull(service);
    assertTrue(service.isRunning());
  }

  /**
   * Test method for {@link
   * com.google.enterprise.cloudsearch.sdk.StatsManager#getComponent(java.lang.String)}.
   */
  @Test
  public void testGetComponent() {
    StatsManager.OperationStats component1 = StatsManager.getComponent("testComponent1");
    StatsManager.OperationStats component2 = StatsManager.getComponent("testComponent2");
    StatsManager.OperationStats component2again = StatsManager.getComponent("testComponent2");
    assertNotNull(component1);
    assertNotNull(component2);
    assertNotNull(component2again);
    assertNotEquals(component1, component2);
    assertEquals(component2, component2again);
  }

  /**
   * Test method for {@link com.google.enterprise.cloudsearch.sdk.StatsManager#stop()} and {@link
   * com.google.enterprise.cloudsearch.sdk.StatsManager#resume()}.
   */
  @Test
  public void testStopAndResume() {
    StatsManager service = StatsManager.getInstance();
    service.resume();
    assertTrue(service.isRunning());
    service.stop();
    assertFalse(service.isRunning());
    service.resume();
    assertTrue(service.isRunning());
  }

  @Test
  public void testEvent() {
    StatsManager.OperationStats component = StatsManager.getComponent("testComponent1");
    Event event = component.event(TestEnum.VALUE1);
    assertNotNull(event);
  }

  @Test
  public void testEvent2() {
    StatsManager.OperationStats component = StatsManager.getComponent("testComponent2");
    Event event = component.event("event");
    assertNotNull(event);
  }

  @Test
  public void testRegister() {
    StatsManager.OperationStats component = StatsManager.getComponent("testComponent3");
    int before = component.getRegisteredCount("operation");
    component.register("operation");
    int after = component.getRegisteredCount("operation");
    assertEquals(1, after - before);
  }

  @Test
  public void testSuccessCount() {
    StatsManager.OperationStats component = StatsManager.getComponent("testComponent5");
    int before = component.getSuccessCount("operation");
    component.event("operation").start().success();
    int after = component.getSuccessCount("operation");
    assertEquals(1, after - before);
  }

  @Test
  public void testFailureCount() {
    StatsManager.OperationStats component = StatsManager.getComponent("testComponent5");
    int before = component.getFailureCount("operation");
    component.event("operation").start().failure();
    int after = component.getFailureCount("operation");
    assertEquals(1, after - before);
  }

  @Test
  public void testLogResult() {
    StatsManager.OperationStats component = StatsManager.getComponent("testComponent4");
    int beforeOne = component.getLogResultCounter("operation", "one");
    int beforeTwo = component.getLogResultCounter("operation", "two");
    component.logResult("operation", "one");
    component.logResult("operation", "two");
    component.logResult("operation", "two");
    int afterOne = component.getLogResultCounter("operation", "one");
    int afterTwo = component.getLogResultCounter("operation", "two");
    assertEquals(1, afterOne - beforeOne);
    assertEquals(2, afterTwo - beforeTwo);
  }

  @Test
  public void event_canFailEventWithoutStarting() {
    OperationStats stats = StatsManager.getComponent("StatsManagerTest");
    Event event = stats.event("operation");
    event.failure();
    assertEquals(1, stats.getFailureCount("operation"));
  }

  @Test
  public void event_canPassEventWithoutStarting() {
    OperationStats stats = StatsManager.getComponent("StatsManagerTest");
    Event event = stats.event("operation");
    event.success();
    assertEquals(1, stats.getSuccessCount("operation"));
  }

  @Test
  public void manual_test() {
    OperationStats stats = StatsManager.getComponent("component_1");
    Event event = stats.event(TimeUnit.MICROSECONDS);
    event.start();
    // do something
    event.success();
    Event event2 = stats.event(TimeUnit.MICROSECONDS);
    event2.start();
    // do something
    event2.success();
    Event event3 = stats.event(TimeUnit.SECONDS);
    event3.start();
    event3.success();
    Event event4 = stats.event(TimeUnit.MICROSECONDS).start();
    // do something
    event4.failure();

    OperationStats stats2 = StatsManager.getComponent("component_2");
    Event event5 = stats2.event("operation").start();
    // do something
    event5.success();

    stats2.logResult("loggedPositiveOnce", "positive");
    stats2.logResult("loggedNegativeTwoTimes", "negative");
    stats2.logResult("loggedNegativeTwoTimes", "negative");

    stats2.register("oneTime");
    stats2.register("twoTime");
    stats2.register("twoTime");

    System.out.println(StatsManager.getInstance().printStats());
    // TODO(imysak): update test when snapshot functional will be added
  }

  private static enum TestEnum {
    VALUE1,
    VALUE2,
    VALUE3
  }
}
