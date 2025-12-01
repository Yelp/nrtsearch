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
package com.yelp.nrtsearch.server.utils;

import java.io.InputStream;
import org.apache.commons.io.input.ProxyInputStream;

/**
 * InputStream wrapper that uses a GlobalWindowRateLimiter to throttle read bytes. It acquires
 * permits from the rate limiter after each read operation.
 */
public class GlobalThrottledInputStream extends ProxyInputStream {
  private final GlobalWindowRateLimiter limiter;

  /**
   * Constructs a GlobalThrottledInputStream that wraps the given InputStream and uses the provided
   * GlobalWindowRateLimiter for throttling.
   *
   * @param in the InputStream to be wrapped
   * @param limiter the GlobalWindowRateLimiter to use for throttling
   */
  public GlobalThrottledInputStream(InputStream in, GlobalWindowRateLimiter limiter) {
    super(in);
    this.limiter = limiter;
  }

  @Override
  protected void afterRead(int numBytes) {
    if (numBytes != -1) {
      limiter.acquire(numBytes);
    }
  }
}
