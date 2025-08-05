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
package com.yelp.nrtsearch.server.monitoring;

import com.yelp.nrtsearch.server.concurrent.ExecutorServiceStatsWrapper;
import io.prometheus.metrics.core.metrics.Counter;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Collector implementation to gather metrics for {@link ThreadPoolExecutor}. Records thread and
 * queue usage, as well as rejection count. Executors must be added with {@link #addPool(String,
 * ThreadPoolExecutor)}.
 */
public class ThreadPoolCollector implements MultiCollector {

  private static final Gauge poolSize =
      Gauge.builder()
          .name("nrt_thread_pool_size")
          .help("Current number of threads in the pool.")
          .labelNames("pool")
          .build();
  private static final Gauge poolMax =
      Gauge.builder()
          .name("nrt_thread_pool_max")
          .help("Maximum number of threads in the pool.")
          .labelNames("pool")
          .build();
  private static final Gauge poolActive =
      Gauge.builder()
          .name("nrt_thread_pool_active")
          .help("Number of active threads in the pool.")
          .labelNames("pool")
          .build();
  private static final Gauge poolTasks =
      Gauge.builder()
          .name("nrt_thread_pool_tasks")
          .help("Number of tasks ever scheduled for execution.")
          .labelNames("pool")
          .build();
  private static final Gauge poolQueueSize =
      Gauge.builder()
          .name("nrt_thread_pool_queue_size")
          .help("Current size of pool task queue.")
          .labelNames("pool")
          .build();
  private static final Gauge poolQueueRemaining =
      Gauge.builder()
          .name("nrt_thread_pool_queue_remaining")
          .help("Capacity left in pool task queue.")
          .labelNames("pool")
          .build();

  /** Wrapper class to record the rejection count for a {@link ThreadPoolExecutor}. */
  public static class RejectionCounterWrapper implements RejectedExecutionHandler {
    public static final Counter rejectionCounter =
        Counter.builder()
            .name("nrt_thread_pool_reject_count")
            .help("Count of rejected tasks.")
            .labelNames("pool")
            .build();

    private final RejectedExecutionHandler in;
    private final String poolName;

    public RejectionCounterWrapper(RejectedExecutionHandler in, String poolName) {
      this.in = in;
      this.poolName = poolName;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      rejectionCounter.labelValues(poolName).inc();
      in.rejectedExecution(r, executor);
    }
  }

  private static final Map<String, ThreadPoolExecutor> pools = new ConcurrentHashMap<>();
  private static final Map<String, ExecutorServiceStatsWrapper> virtualPools =
      new ConcurrentHashMap<>();

  public static void addPool(String name, ThreadPoolExecutor threadPoolExecutor) {
    pools.put(name, threadPoolExecutor);
    RejectedExecutionHandler handler = threadPoolExecutor.getRejectedExecutionHandler();
    threadPoolExecutor.setRejectedExecutionHandler(new RejectionCounterWrapper(handler, name));
  }

  public static void addVirtualPool(String name, ExecutorServiceStatsWrapper executorStatsWrapper) {
    virtualPools.put(name, executorStatsWrapper);
  }

  @Override
  public MetricSnapshots collect() {
    List<MetricSnapshot> metrics = new ArrayList<>();

    for (Map.Entry<String, ThreadPoolExecutor> entry : pools.entrySet()) {
      String poolLabel = entry.getKey();
      poolSize.labelValues(poolLabel).set(entry.getValue().getPoolSize());
      poolMax.labelValues(poolLabel).set(entry.getValue().getMaximumPoolSize());
      poolActive.labelValues(poolLabel).set(entry.getValue().getActiveCount());
      poolTasks.labelValues(poolLabel).set(entry.getValue().getTaskCount());
      poolQueueSize.labelValues(poolLabel).set(entry.getValue().getQueue().size());
      poolQueueRemaining
          .labelValues(poolLabel)
          .set(entry.getValue().getQueue().remainingCapacity());
    }

    for (Map.Entry<String, ExecutorServiceStatsWrapper> entry : virtualPools.entrySet()) {
      String poolLabel = entry.getKey();
      poolTasks.labelValues(poolLabel).set(entry.getValue().getTotalTasks());
    }

    metrics.add(poolSize.collect());
    metrics.add(poolMax.collect());
    metrics.add(poolActive.collect());
    metrics.add(poolTasks.collect());
    metrics.add(poolQueueSize.collect());
    metrics.add(poolQueueRemaining.collect());

    return new MetricSnapshots(metrics);
  }
}
