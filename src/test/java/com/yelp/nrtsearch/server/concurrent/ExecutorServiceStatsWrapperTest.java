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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ExecutorServiceStatsWrapperTest {

  @Mock private ExecutorService mockExecutorService;
  @Mock private Future<String> mockFuture;
  @Mock private Future<Object> mockObjectFuture;

  private ExecutorServiceStatsWrapper wrapper;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    wrapper = new ExecutorServiceStatsWrapper(mockExecutorService);
  }

  @Test
  public void testConstructor() {
    ExecutorService executorService = mock(ExecutorService.class);
    @SuppressWarnings("resource")
    ExecutorServiceStatsWrapper statsWrapper = new ExecutorServiceStatsWrapper(executorService);
    assertThat(statsWrapper.getTotalTasks()).isEqualTo(0);
  }

  @Test
  public void testGetTotalTasksInitiallyZero() {
    assertThat(wrapper.getTotalTasks()).isEqualTo(0);
  }

  @Test
  public void testSubmitCallableIncrementsTaskCount() {
    Callable<String> task = () -> "test";
    when(mockExecutorService.submit(task)).thenReturn(mockFuture);

    Future<String> result = wrapper.submit(task);

    assertThat(wrapper.getTotalTasks()).isEqualTo(1);
    assertThat(result).isEqualTo(mockFuture);
    verify(mockExecutorService).submit(task);
  }

  @Test
  public void testSubmitRunnableWithResultIncrementsTaskCount() {
    Runnable task = () -> {};
    String result = "result";
    when(mockExecutorService.submit(task, result)).thenReturn(mockFuture);

    Future<String> actualResult = wrapper.submit(task, result);

    assertThat(wrapper.getTotalTasks()).isEqualTo(1);
    assertThat(actualResult).isEqualTo(mockFuture);
    verify(mockExecutorService).submit(task, result);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSubmitRunnableIncrementsTaskCount() {
    Runnable task = () -> {};
    Future<Object> expectedFuture = mock(Future.class);
    doReturn(expectedFuture).when(mockExecutorService).submit(task);

    Future<?> result = wrapper.submit(task);

    assertThat(wrapper.getTotalTasks()).isEqualTo(1);
    assertThat(result).isEqualTo(expectedFuture);
    verify(mockExecutorService).submit(task);
  }

  @Test
  public void testExecuteIncrementsTaskCount() {
    Runnable command = () -> {};

    wrapper.execute(command);

    assertThat(wrapper.getTotalTasks()).isEqualTo(1);
    verify(mockExecutorService).execute(command);
  }

  @Test
  public void testInvokeAllIncrementsTaskCountByCollectionSize() throws InterruptedException {
    Callable<String> task1 = () -> "task1";
    Callable<String> task2 = () -> "task2";
    Callable<String> task3 = () -> "task3";
    Collection<Callable<String>> tasks = Arrays.asList(task1, task2, task3);

    @SuppressWarnings("unchecked")
    List<Future<String>> expectedFutures = mock(List.class);
    when(mockExecutorService.invokeAll(tasks)).thenReturn(expectedFutures);

    List<Future<String>> result = wrapper.invokeAll(tasks);

    assertThat(wrapper.getTotalTasks()).isEqualTo(3);
    assertThat(result).isEqualTo(expectedFutures);
    verify(mockExecutorService).invokeAll(tasks);
  }

  @Test
  public void testInvokeAllWithTimeoutIncrementsTaskCountByCollectionSize()
      throws InterruptedException {
    Callable<String> task1 = () -> "task1";
    Callable<String> task2 = () -> "task2";
    Collection<Callable<String>> tasks = Arrays.asList(task1, task2);

    @SuppressWarnings("unchecked")
    List<Future<String>> expectedFutures = mock(List.class);
    when(mockExecutorService.invokeAll(tasks, 10, TimeUnit.SECONDS)).thenReturn(expectedFutures);

    List<Future<String>> result = wrapper.invokeAll(tasks, 10, TimeUnit.SECONDS);

    assertThat(wrapper.getTotalTasks()).isEqualTo(2);
    assertThat(result).isEqualTo(expectedFutures);
    verify(mockExecutorService).invokeAll(tasks, 10, TimeUnit.SECONDS);
  }

  @Test
  public void testInvokeAnyIncrementsTaskCountByCollectionSize()
      throws InterruptedException, ExecutionException {
    Callable<String> task1 = () -> "task1";
    Callable<String> task2 = () -> "task2";
    Collection<Callable<String>> tasks = Arrays.asList(task1, task2);

    when(mockExecutorService.invokeAny(tasks)).thenReturn("task1");

    String result = wrapper.invokeAny(tasks);

    assertThat(wrapper.getTotalTasks()).isEqualTo(2);
    assertThat(result).isEqualTo("task1");
    verify(mockExecutorService).invokeAny(tasks);
  }

  @Test
  public void testInvokeAnyWithTimeoutIncrementsTaskCountByCollectionSize()
      throws InterruptedException, ExecutionException, TimeoutException {
    Callable<String> task1 = () -> "task1";
    Callable<String> task2 = () -> "task2";
    Collection<Callable<String>> tasks = Arrays.asList(task1, task2);

    when(mockExecutorService.invokeAny(tasks, 10, TimeUnit.SECONDS)).thenReturn("task1");

    String result = wrapper.invokeAny(tasks, 10, TimeUnit.SECONDS);

    assertThat(wrapper.getTotalTasks()).isEqualTo(2);
    assertThat(result).isEqualTo("task1");
    verify(mockExecutorService).invokeAny(tasks, 10, TimeUnit.SECONDS);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testMultipleOperationsAccumulateTaskCount() {
    Runnable runnable = () -> {};
    Callable<String> callable = () -> "test";

    doReturn(mockObjectFuture).when(mockExecutorService).submit(any(Runnable.class));
    doReturn(mockFuture).when(mockExecutorService).submit(any(Callable.class));

    wrapper.submit(runnable);
    wrapper.submit(callable);
    wrapper.execute(runnable);

    assertThat(wrapper.getTotalTasks()).isEqualTo(3);
  }

  @Test
  public void testShutdownDelegatesToUnderlyingExecutor() {
    wrapper.shutdown();
    verify(mockExecutorService).shutdown();
  }

  @Test
  public void testShutdownNowDelegatesToUnderlyingExecutor() {
    List<Runnable> expectedTasks = Arrays.asList(() -> {}, () -> {});
    when(mockExecutorService.shutdownNow()).thenReturn(expectedTasks);

    List<Runnable> result = wrapper.shutdownNow();

    assertThat(result).isEqualTo(expectedTasks);
    verify(mockExecutorService).shutdownNow();
  }

  @Test
  public void testIsShutdownDelegatesToUnderlyingExecutor() {
    when(mockExecutorService.isShutdown()).thenReturn(true);

    boolean result = wrapper.isShutdown();

    assertThat(result).isTrue();
    verify(mockExecutorService).isShutdown();
  }

  @Test
  public void testIsTerminatedDelegatesToUnderlyingExecutor() {
    when(mockExecutorService.isTerminated()).thenReturn(true);

    boolean result = wrapper.isTerminated();

    assertThat(result).isTrue();
    verify(mockExecutorService).isTerminated();
  }

  @Test
  public void testAwaitTerminationDelegatesToUnderlyingExecutor() throws InterruptedException {
    when(mockExecutorService.awaitTermination(10, TimeUnit.SECONDS)).thenReturn(true);

    boolean result = wrapper.awaitTermination(10, TimeUnit.SECONDS);

    assertThat(result).isTrue();
    verify(mockExecutorService).awaitTermination(10, TimeUnit.SECONDS);
  }

  @Test
  public void testEmptyCollectionDoesNotIncrementTaskCount() throws InterruptedException {
    Collection<Callable<String>> emptyTasks = Arrays.asList();
    @SuppressWarnings("unchecked")
    List<Future<String>> emptyFutures = mock(List.class);
    when(mockExecutorService.invokeAll(emptyTasks)).thenReturn(emptyFutures);

    wrapper.invokeAll(emptyTasks);

    assertThat(wrapper.getTotalTasks()).isEqualTo(0);
    verify(mockExecutorService).invokeAll(emptyTasks);
  }

  @Test
  public void testTaskCountThreadSafety() throws InterruptedException {
    // Test concurrent access to task counter
    int numThreads = 10;
    int tasksPerThread = 100;
    Thread[] threads = new Thread[numThreads];

    doReturn(mockObjectFuture).when(mockExecutorService).submit(any(Runnable.class));

    for (int i = 0; i < numThreads; i++) {
      threads[i] =
          new Thread(
              () -> {
                for (int j = 0; j < tasksPerThread; j++) {
                  wrapper.submit(() -> {});
                }
              });
    }

    for (Thread thread : threads) {
      thread.start();
    }

    for (Thread thread : threads) {
      thread.join();
    }

    assertThat(wrapper.getTotalTasks()).isEqualTo(numThreads * tasksPerThread);
  }
}
