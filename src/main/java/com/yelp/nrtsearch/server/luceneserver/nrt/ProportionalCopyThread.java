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

import java.util.PriorityQueue;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.Node;

/**
 * Copy thread that uses separate priority queues for high and low priority jobs. The value of
 * lowPriorityCopyPercentage determines the proportion of times a low priority job is selected over
 * a high priority job. For example, if lowPriorityCopyPercentage is 10, then a low priority job
 * will be selected 10% of the time and a high priority job will be selected 90% of the time. If
 * there are no high priority jobs, then a low priority job will always be selected.
 */
public class ProportionalCopyThread extends NrtCopyThread {
  private final PriorityQueue<CopyJob> highPriorityQueue = new PriorityQueue<>();
  private final PriorityQueue<CopyJob> lowPriorityQueue = new PriorityQueue<>();
  private final int lowPriorityCopyPercentage;
  private long counter = 0;

  public ProportionalCopyThread(Node node, int lowPriorityCopyPercentage) {
    super(node);
    if (lowPriorityCopyPercentage < 0 || lowPriorityCopyPercentage > 100) {
      throw new IllegalArgumentException(
          "lowPriorityCopyPercentage must be between 0 and 100, inclusive. Got: "
              + lowPriorityCopyPercentage);
    }
    this.lowPriorityCopyPercentage = lowPriorityCopyPercentage;
  }

  @Override
  boolean hasJob() {
    return !highPriorityQueue.isEmpty() || !lowPriorityQueue.isEmpty();
  }

  @Override
  CopyJob getJob() {
    CopyJob job;
    if (counter++ % 100 < lowPriorityCopyPercentage) {
      job = lowPriorityQueue.poll();
      if (job != null) {
        return job;
      }
    }
    job = highPriorityQueue.poll();
    if (job != null) {
      return job;
    }
    return lowPriorityQueue.poll();
  }

  @Override
  void addJob(CopyJob job) {
    if (job.highPriority) {
      highPriorityQueue.offer(job);
    } else {
      lowPriorityQueue.offer(job);
    }
  }
}
