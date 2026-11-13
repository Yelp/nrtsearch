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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a task on an executor for every request, merging requests made while it runs: the task runs
 * once more after it finishes if any request arrived during it, and never runs concurrently with
 * itself. No request is lost: every request is followed by a run that started after it.
 *
 * <p>A follow up run is submitted to the executor again rather than run in a loop, so a stream of
 * requests does not hold a shared executor thread and starve its other tasks. Failures of the task
 * (including errors) are logged and do not stop later runs.
 */
public class CoalescingRunner {
  private static final Logger logger = LoggerFactory.getLogger(CoalescingRunner.class);
  private final Executor executor;
  private final Runnable task;
  // requests not yet covered by a completed run; nonzero while a run is active or queued
  private final AtomicInteger requests = new AtomicInteger();

  public CoalescingRunner(Executor executor, Runnable task) {
    this.executor = Objects.requireNonNull(executor);
    this.task = Objects.requireNonNull(task);
  }

  /** Request a run. */
  public void request() {
    // only the request that finds no run active or queued submits one
    if (requests.getAndIncrement() > 0) {
      return;
    }
    submit();
  }

  private void submit() {
    try {
      executor.execute(this::runOnce);
    } catch (RejectedExecutionException e) {
      // executor shut down: nothing will run, so later requests must not wait on this one
      requests.set(0);
    }
  }

  private void runOnce() {
    int covered = requests.get();
    try {
      task.run();
    } catch (Throwable t) {
      logger.warn("Coalesced task failed", t);
    } finally {
      // the run covered requests counted before it started; run again if more arrived. Done in
      // finally so even an error leaves the runner usable.
      if (!requests.compareAndSet(covered, 0)) {
        requests.set(1);
        submit();
      }
    }
  }
}
