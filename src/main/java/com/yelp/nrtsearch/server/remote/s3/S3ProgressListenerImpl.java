/*
 * Copyright 2023 Yelp Inc.
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
package com.yelp.nrtsearch.server.remote.s3;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;

/**
 * {@link TransferListener} that logs the status of a s3 transfer using the {@link
 * software.amazon.awssdk.transfer.s3.S3TransferManager}.
 *
 * <p>Note on progress tracking semantics: The AWS SDK v2's {@code
 * progressSnapshot().transferredBytes()} returns the cumulative bytes transferred so far for an
 * individual file transfer, not a delta. This class tracks the last-seen value per transfer request
 * to compute actual deltas, which are then accumulated into {@code totalBytesTransferred}.
 */
public class S3ProgressListenerImpl implements TransferListener {
  private static final Logger logger = LoggerFactory.getLogger(S3ProgressListenerImpl.class);

  private static final long LOG_INTERVAL_NS = TimeUnit.SECONDS.toNanos(30);

  private final String serviceName;
  private final String resource;
  private final String operation;
  private final long totalExpectedBytes;

  // Tracks the last cumulative snapshot value per transfer request to compute deltas.
  private final ConcurrentHashMap<Object, Long> lastSeenBytes = new ConcurrentHashMap<>();
  private final AtomicLong totalBytesTransferred = new AtomicLong();
  private final AtomicLong lastLoggedNanos = new AtomicLong(System.nanoTime());

  /**
   * Constructor.
   *
   * @param serviceName service name
   * @param resource resource identifier
   * @param operation operation name (e.g. "download_index_files")
   * @param totalExpectedBytes total expected bytes across all files for this listener, used to
   *     compute percent complete in log output; use -1 if unknown
   */
  public S3ProgressListenerImpl(
      String serviceName, String resource, String operation, long totalExpectedBytes) {
    this.serviceName = serviceName;
    this.resource = resource;
    this.operation = operation;
    this.totalExpectedBytes = totalExpectedBytes;
  }

  @Override
  public void bytesTransferred(Context.BytesTransferred context) {
    // transferredBytes() is cumulative for this individual file transfer; compute the delta.
    long current = context.progressSnapshot().transferredBytes();
    Long prev = lastSeenBytes.put(context.request(), current);
    long delta = prev == null ? current : current - prev;
    long total = totalBytesTransferred.addAndGet(delta);

    long now = System.nanoTime();
    long last = lastLoggedNanos.get();
    if (now - last >= LOG_INTERVAL_NS && lastLoggedNanos.compareAndSet(last, now)) {
      if (totalExpectedBytes > 0) {
        double pct = 100.0 * total / totalExpectedBytes;
        logger.info(
            String.format(
                "service: %s, resource: %s, %s transferred bytes: %s (%.1f%%)",
                serviceName, resource, operation, total, pct));
      } else {
        logger.info(
            String.format(
                "service: %s, resource: %s, %s transferred bytes: %s",
                serviceName, resource, operation, total));
      }
    }
  }
}
