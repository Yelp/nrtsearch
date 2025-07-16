/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.concurrent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link ExecutorService} implementation that wraps another {@link ExecutorService} and tracks
 * stats. Currently, only tracks the total number of tasks submitted to the executor.
 */
public class ExecutorServiceStatsWrapper implements ExecutorService {
  private final ExecutorService delegate;
  private final AtomicLong totalTasks = new AtomicLong(0);

  public ExecutorServiceStatsWrapper(ExecutorService delegate) {
    this.delegate = delegate;
  }

  /**
   * Returns the total number of tasks submitted to this executor.
   *
   * @return total number of tasks
   */
  public long getTotalTasks() {
    return totalTasks.get();
  }

  @Override
  public void shutdown() {
    delegate.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return delegate.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return delegate.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return delegate.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return delegate.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    totalTasks.incrementAndGet();
    return delegate.submit(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    totalTasks.incrementAndGet();
    return delegate.submit(task, result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    totalTasks.incrementAndGet();
    return delegate.submit(task);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    totalTasks.addAndGet(tasks.size());
    return delegate.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    totalTasks.addAndGet(tasks.size());
    return delegate.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    totalTasks.addAndGet(tasks.size());
    return delegate.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    totalTasks.addAndGet(tasks.size());
    return delegate.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command) {
    totalTasks.incrementAndGet();
    delegate.execute(command);
  }
}
