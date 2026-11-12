/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.clientlib.util;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runs a task on an executor for every request, merging requests made while it runs: the task runs
 * once more after it finishes if any request arrived during it, and never runs concurrently with
 * itself. No request is lost: every request is followed by a run that started after it.
 */
public class CoalescingRunner {
  private final Executor executor;
  private final Runnable task;
  // requests not yet covered by a completed run; nonzero while a run loop is active
  private final AtomicInteger requests = new AtomicInteger();

  public CoalescingRunner(Executor executor, Runnable task) {
    this.executor = Objects.requireNonNull(executor);
    this.task = Objects.requireNonNull(task);
  }

  /** Request a run. */
  public void request() {
    // only the request that finds no loop active starts one
    if (requests.getAndIncrement() > 0) {
      return;
    }
    try {
      executor.execute(this::loop);
    } catch (RejectedExecutionException e) {
      requests.set(0);
    }
  }

  private void loop() {
    int covered = requests.get();
    while (true) {
      try {
        task.run();
      } catch (RuntimeException e) {
        // the task is expected to handle its own errors; keep the loop invariant regardless
      }
      // the run covered requests counted before it started; run again if more arrived
      if (requests.compareAndSet(covered, 0)) {
        return;
      }
      covered = requests.get();
    }
  }
}
