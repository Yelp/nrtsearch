/*
 * Copyright 2020 Yelp Inc.
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
package com.yelp.nrtsearch.server.luceneserver;

import java.io.Closeable;
import java.io.IOException;
import java.util.Locale;
import java.util.PriorityQueue;
import org.apache.lucene.replicator.nrt.CopyJob;
import org.apache.lucene.replicator.nrt.Node;
import org.apache.lucene.store.AlreadyClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs CopyJob(s) in background thread; each ReplicaNode has an instance of this running. At a
 * given there could be one NRT copy job running, and multiple pre-warm merged segments jobs.
 */
class Jobs extends Thread implements Closeable {
  private static final Logger logger = LoggerFactory.getLogger(Jobs.class);

  private final PriorityQueue<CopyJob> queue = new PriorityQueue<>();

  private final Node node;

  public Jobs(Node node) {
    this.node = node;
  }

  private boolean finish;

  /**
   * Returns null if we are closing, else, returns the top job or waits for one to arrive if the
   * queue is empty.
   */
  private synchronized SimpleCopyJob getNextJob() {
    while (true) {
      if (finish) {
        return null;
      } else if (queue.isEmpty()) {
        try {
          wait();
        } catch (InterruptedException ie) {
          throw new RuntimeException(ie);
        }
      } else {
        return (SimpleCopyJob) queue.poll();
      }
    }
  }

  @Override
  public void run() {
    // nocommit: prioritize jobs better here, the way an OS assigns CPU to processes:
    while (true) {
      SimpleCopyJob topJob = getNextJob();
      if (topJob == null) {
        assert finish;
        break;
      }
      // node.message("JOBS: run " + topJob);
      // System.out.println("JOBS: run " + topJob);

      this.setName("jobs o" + topJob.ord);

      assert topJob != null;

      boolean result;
      try {
        result = topJob.visit();
      } catch (Throwable t) {
        if ((t instanceof AlreadyClosedException) == false) {
          logWarning("exception during job.visit job=" + topJob + "; now cancel", t);

        } else {
          logWarning("AlreadyClosedException during job.visit job=" + topJob + "; now cancel");
        }
        try {
          topJob.cancel("unexpected exception in visit", t);
        } catch (Throwable t2) {
          logWarning("ignore exception calling cancel: " + t2, t2);
        }
        try {
          topJob.onceDone.run(topJob);
        } catch (Throwable t2) {
          logWarning("ignore exception calling OnceDone: " + t2, t2);
        }
        continue;
      }

      if (result == false) {
        // Job isn't done yet; put it back:
        synchronized (this) {
          queue.offer(topJob);
        }
      } else {
        // Job finished, now notify caller:
        try {
          long startNS = System.nanoTime();
          topJob.onceDone.run(topJob);
          long endNS = System.nanoTime();
          node.message(
              String.format(
                  Locale.ROOT,
                  "%.2f msec to run job.onceDone for %s",
                  (endNS - startNS) / 1000000.,
                  topJob));
        } catch (Throwable t) {
          logWarning("ignore exception calling OnceDone: " + t, t);
        }
      }
    }

    node.message("top: jobs now exit run thread");

    synchronized (this) {
      // Gracefully cancel any jobs we didn't finish:
      while (queue.isEmpty() == false) {
        SimpleCopyJob job = (SimpleCopyJob) queue.poll();
        node.message("top: Jobs: now cancel job=" + job);
        try {
          job.cancel("jobs closing", null);
        } catch (Throwable t) {
          logWarning("ignore exception calling cancel", t);
        }
        try {
          job.onceDone.run(job);
        } catch (Throwable t) {
          logWarning("ignore exception calling OnceDone", t);
        }
      }
    }
  }

  public synchronized void launch(CopyJob job) {
    if (finish == false) {
      queue.offer(job);
      notify();
    } else {
      throw new AlreadyClosedException("closed");
    }
  }

  /** Cancels any existing jobs that are copying the same file names as this one */
  public synchronized void cancelConflictingJobs(CopyJob newJob) throws IOException {
    for (CopyJob job : queue) {
      if (job.conflicts(newJob)) {
        node.message(
            "top: now cancel existing conflicting job=" + job + " due to newJob=" + newJob);
        job.cancel("conflicts with new job", null);
      }
    }
  }

  @Override
  public synchronized void close() {
    finish = true;
    notify();
    try {
      join();
    } catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  private void logWarning(String msg) {
    node.message(msg);
    logger.warn(msg);
  }

  private void logWarning(String msg, Throwable t) {
    node.message(msg);
    logger.warn(msg, t);
  }
}
