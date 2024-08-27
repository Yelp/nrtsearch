/*
 * Copyright 2024 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver.nrt;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Test;

public class RefreshUploadFutureTest {

  private Thread getThread(RefreshUploadFuture refreshUploadFuture, Exception exception) {
    return new Thread(
        () -> {
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          refreshUploadFuture.setDone(exception);
        });
  }

  @Test
  public void testGet_success() throws ExecutionException, InterruptedException {
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    Thread thread = getThread(refreshUploadFuture, null);
    assertFalse(refreshUploadFuture.isDone());
    long startTime = System.currentTimeMillis();
    thread.start();
    refreshUploadFuture.get();
    long totalTime = System.currentTimeMillis() - startTime;
    assertTrue(refreshUploadFuture.isDone());
    assertTrue(totalTime >= 1000);
    thread.join(1000);
  }

  @Test
  public void testGet_error() throws InterruptedException {
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    Exception exception = new Exception();
    Thread thread = getThread(refreshUploadFuture, exception);
    assertFalse(refreshUploadFuture.isDone());
    long startTime = System.currentTimeMillis();
    thread.start();
    try {
      refreshUploadFuture.get();
      fail();
    } catch (ExecutionException e) {
      assertSame(exception, e.getCause());
    }
    long totalTime = System.currentTimeMillis() - startTime;
    assertTrue(refreshUploadFuture.isDone());
    assertTrue(totalTime >= 1000);
    thread.join(1000);
  }

  @Test
  public void testGetWithTimeout_success()
      throws ExecutionException, InterruptedException, TimeoutException {
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    Thread thread = getThread(refreshUploadFuture, null);
    long startTime = System.currentTimeMillis();
    thread.start();
    refreshUploadFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
    long totalTime = System.currentTimeMillis() - startTime;
    assertTrue(totalTime >= 1000);
    thread.join(1000);
  }

  @Test
  public void testGetWithTimeout_error() throws InterruptedException {
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    Exception exception = new Exception();
    Thread thread = getThread(refreshUploadFuture, exception);
    long startTime = System.currentTimeMillis();
    thread.start();
    try {
      refreshUploadFuture.get(30, java.util.concurrent.TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException e) {
      assertSame(exception, e.getCause());
    } catch (TimeoutException e) {
      throw new RuntimeException(e);
    }
    long totalTime = System.currentTimeMillis() - startTime;
    assertTrue(totalTime >= 1000);
    thread.join(1000);
  }

  @Test
  public void testGetWithTimeout_timeout() throws InterruptedException {
    RefreshUploadFuture refreshUploadFuture = new RefreshUploadFuture();
    Exception exception = new Exception();
    Thread thread = getThread(refreshUploadFuture, exception);
    long startTime = System.currentTimeMillis();
    thread.start();
    try {
      refreshUploadFuture.get(100, TimeUnit.MILLISECONDS);
      fail();
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    } catch (TimeoutException e) {
      // expected
    }
    long totalTime = System.currentTimeMillis() - startTime;
    assertTrue(totalTime >= 100);
    thread.join(1000);
  }
}
