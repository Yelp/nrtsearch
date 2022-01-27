/*
 * Copyright 2022 Yelp Inc.
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
package com.yelp.nrtsearch.server.grpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.yelp.nrtsearch.server.monitoring.DeadlineMetrics;
import io.grpc.Context;
import io.grpc.Context.CancellableContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.junit.Test;

public class DeadlineUtilsTest {
  private static final ScheduledExecutorService executorService =
      new ScheduledThreadPoolExecutor(1);

  @Test
  public void testOutOfContextNoop() {
    DeadlineUtils.setCancellationEnabled(false);
    DeadlineUtils.checkDeadline("test", "TEST");
    DeadlineUtils.setCancellationEnabled(true);
    DeadlineUtils.checkDeadline("test", "TEST");
  }

  @Test
  public void testInContextNoDeadline() {
    DeadlineUtils.setCancellationEnabled(false);
    Context.current().run(() -> DeadlineUtils.checkDeadline("test", "TEST"));
    DeadlineUtils.setCancellationEnabled(true);
    Context.current().run(() -> DeadlineUtils.checkDeadline("test", "TEST"));
  }

  @Test
  public void testWithDeadlineNotReached() {
    DeadlineUtils.setCancellationEnabled(false);
    CancellableContext context =
        Context.current().withDeadlineAfter(100, TimeUnit.SECONDS, executorService);
    try {
      context.run(() -> DeadlineUtils.checkDeadline("test", "TEST"));
    } finally {
      context.cancel(null);
    }
    DeadlineUtils.setCancellationEnabled(true);
    context = Context.current().withDeadlineAfter(100, TimeUnit.SECONDS, executorService);
    try {
      context.run(() -> DeadlineUtils.checkDeadline("test", "TEST"));
    } finally {
      context.cancel(null);
    }
  }

  @Test
  public void testWithDeadlineReached() {
    DeadlineUtils.setCancellationEnabled(false);
    CancellableContext context =
        Context.current().withDeadlineAfter(1, TimeUnit.MILLISECONDS, executorService);
    try {
      context.run(
          () -> {
            try {
              Thread.sleep(5);
            } catch (InterruptedException ignored) {
            }
            DeadlineUtils.checkDeadline("test", "TEST");
          });
    } finally {
      context.cancel(null);
    }
    DeadlineUtils.setCancellationEnabled(true);
    context = Context.current().withDeadlineAfter(1, TimeUnit.MILLISECONDS, executorService);
    try {
      context.run(
          () -> {
            try {
              Thread.sleep(5);
            } catch (InterruptedException ignored) {
            }
            DeadlineUtils.checkDeadline("test", "TEST");
          });
      fail();
    } catch (StatusRuntimeException e) {
      assertEquals(Status.CANCELLED.getCode(), e.getStatus().getCode());
      assertEquals("Request deadline exceeded: test", e.getStatus().getDescription());
    } finally {
      context.cancel(null);
    }
  }

  @Test
  public void testMetricsCounter() {
    int initialCount = getCancelMetricCount();
    DeadlineUtils.setCancellationEnabled(true);
    CancellableContext context =
        Context.current().withDeadlineAfter(1, TimeUnit.MILLISECONDS, executorService);
    try {
      context.run(
          () -> {
            try {
              Thread.sleep(5);
            } catch (InterruptedException ignored) {
            }
            DeadlineUtils.checkDeadline("test", "TEST");
          });
      fail();
    } catch (StatusRuntimeException e) {
      assertEquals(Status.CANCELLED.getCode(), e.getStatus().getCode());
      assertEquals("Request deadline exceeded: test", e.getStatus().getDescription());
    } finally {
      context.cancel(null);
    }
    int newCount = getCancelMetricCount();
    assertEquals(initialCount + 1, newCount);
  }

  private int getCancelMetricCount() {
    return (int) DeadlineMetrics.nrtDeadlineCancelCount.labels("TEST").get();
  }
}
