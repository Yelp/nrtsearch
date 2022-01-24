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

import com.yelp.nrtsearch.server.monitoring.DeadlineMetrics;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Status;

/** Utility class for functionality related to gRPC request deadlines. */
public class DeadlineUtils {
  private static boolean cancellationEnabled = false;

  /** Set if deadline based request cancellation is enabled. */
  public static void setCancellationEnabled(boolean enabled) {
    cancellationEnabled = enabled;
  }

  /** Get if deadline based request cancellation is enabled. */
  public static boolean getCancellationEnabled() {
    return cancellationEnabled;
  }

  /**
   * Check if the deadline for the current request is expired, and cancel the request if needed.
   * This method is a noop if cancellation is disabled by {@link #setCancellationEnabled(boolean)},
   * or if the request has no deadline.
   *
   * @param message context to add to exception message
   * @param operation operation label for metrics collection
   * @throws io.grpc.StatusRuntimeException with CANCELLED status if deadline is expired
   */
  public static void checkDeadline(String message, String operation) {
    if (cancellationEnabled) {
      Deadline deadline = Context.current().getDeadline();
      if (deadline != null && deadline.isExpired()) {
        DeadlineMetrics.nrtDeadlineCancelCount.labels(operation).inc();
        throw Status.CANCELLED
            .withDescription("Request deadline exceeded: " + message)
            .asRuntimeException();
      }
    }
  }
}
