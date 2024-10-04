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
package com.yelp.nrtsearch.server.nrt;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Future that is used to wait for a refresh to be made durable in a remote backend. */
public class RefreshUploadFuture implements Future<Void> {
  private volatile boolean done = false;
  private volatile Throwable t;

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public boolean isDone() {
    return done;
  }

  /**
   * Mark the future as done.
   *
   * @param t throwable if an error occurred, or null if successful
   */
  public void setDone(Throwable t) {
    this.t = t;
    done = true;
    synchronized (this) {
      notifyAll();
    }
  }

  @Override
  public Void get() throws InterruptedException, ExecutionException {
    synchronized (this) {
      while (!done) {
        wait();
      }
    }
    if (t != null) {
      throw new ExecutionException(t);
    }
    return null;
  }

  @Override
  public Void get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    synchronized (this) {
      if (!done) {
        wait(unit.toMillis(timeout));
      }
    }
    if (!done) {
      throw new TimeoutException();
    }
    if (t != null) {
      throw new ExecutionException(t);
    }
    return null;
  }
}
