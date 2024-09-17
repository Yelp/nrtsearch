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

import com.amazonaws.event.ProgressEvent;
import com.amazonaws.services.s3.transfer.PersistableTransfer;
import com.amazonaws.services.s3.transfer.internal.S3ProgressListener;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link S3ProgressListener} that logs the status of a s3 transfer using the {@link
 * com.amazonaws.services.s3.transfer.TransferManager}.
 */
public class S3ProgressListenerImpl implements S3ProgressListener {
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
  public void onPersistableTransfer(PersistableTransfer persistableTransfer) {}

  @Override
  public void progressChanged(ProgressEvent progressEvent) {
    long totalBytes = totalBytesTransferred.addAndGet(progressEvent.getBytesTransferred());

    boolean acquired = lock.tryAcquire();

    if (acquired) {
      try {
        bytesTransferredSinceLastLog += progressEvent.getBytesTransferred();
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
