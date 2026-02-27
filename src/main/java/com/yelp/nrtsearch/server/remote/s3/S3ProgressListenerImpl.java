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

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;

/**
 * {@link TransferListener} that logs the status of a s3 transfer using the {@link
 * software.amazon.awssdk.transfer.s3.S3TransferManager}.
 *
 * <p>Note on progress tracking semantics: The AWS SDK v2's {@code
 * progressSnapshot().transferredBytes()} returns incremental bytes transferred since the last event
 * (delta), not cumulative total. This class maintains a cumulative total via {@code
 * totalBytesTransferred} for accurate progress reporting.
 */
public class S3ProgressListenerImpl implements TransferListener {
  private static final Logger logger = LoggerFactory.getLogger(S3ProgressListenerImpl.class);

  private static final long LOG_THRESHOLD_BYTES = 1024 * 1024 * 500; // 500 MB
  private static final long LOG_THRESHOLD_SECONDS = 30;

  private final String serviceName;
  private final String resource;
  private final String operation;

  private final Semaphore lock = new Semaphore(1);
  private final AtomicLong totalBytesTransferred = new AtomicLong();
  private long bytesTransferredSinceLastLog = 0;
  private LocalDateTime lastLoggedTime = LocalDateTime.now();

  public S3ProgressListenerImpl(String serviceName, String resource, String operation) {
    this.serviceName = serviceName;
    this.resource = resource;
    this.operation = operation;
  }

  @Override
  public void bytesTransferred(Context.BytesTransferred context) {
    // AWS SDK v2 returns incremental bytes (delta) per event, not cumulative
    long bytesTransferred = context.progressSnapshot().transferredBytes();
    long totalBytes = totalBytesTransferred.addAndGet(bytesTransferred);

    boolean acquired = lock.tryAcquire();

    if (acquired) {
      try {
        bytesTransferredSinceLastLog += bytesTransferred;
        long secondsSinceLastLog =
            Duration.between(lastLoggedTime, LocalDateTime.now()).getSeconds();

        if (bytesTransferredSinceLastLog > LOG_THRESHOLD_BYTES
            || secondsSinceLastLog > LOG_THRESHOLD_SECONDS) {
          logger.info(
              String.format(
                  "service: %s, resource: %s, %s transferred bytes: %s",
                  serviceName, resource, operation, totalBytes));
          bytesTransferredSinceLastLog = 0;
          lastLoggedTime = LocalDateTime.now();
        }
      } finally {
        lock.release();
      }
    }
  }
}
