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

import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.GaugeMetricFamily;
import java.util.ArrayList;
import java.util.Collections;
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
public class ThreadPoolCollector extends Collector {

  /** Wrapper class to record the rejection count for a {@link ThreadPoolExecutor}. */
  public static class RejectionCounterWrapper implements RejectedExecutionHandler {
    public static final Counter rejectionCounter =
        Counter.build()
            .name("nrt_thread_pool_reject_count")
            .help("Count of rejected tasks.")
            .labelNames("pool")
            .create();

    private final RejectedExecutionHandler in;
    private final String poolName;

    public RejectionCounterWrapper(RejectedExecutionHandler in, String poolName) {
      this.in = in;
      this.poolName = poolName;
    }

    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
      rejectionCounter.labels(poolName).inc();
      in.rejectedExecution(r, executor);
    }
  }

  private static final Map<String, ThreadPoolExecutor> pools = new ConcurrentHashMap<>();

  public static void addPool(String name, ThreadPoolExecutor threadPoolExecutor) {
    pools.put(name, threadPoolExecutor);
    RejectedExecutionHandler handler = threadPoolExecutor.getRejectedExecutionHandler();
    threadPoolExecutor.setRejectedExecutionHandler(new RejectionCounterWrapper(handler, name));
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> mfs = new ArrayList<>();

    List<String> metricLabels = Collections.singletonList("pool");
    GaugeMetricFamily poolSize =
        new GaugeMetricFamily(
            "nrt_thread_pool_size", "Current number of threads in the pool.", metricLabels);
    mfs.add(poolSize);
    GaugeMetricFamily poolMax =
        new GaugeMetricFamily(
            "nrt_thread_pool_max", "Maximum number of threads in the pool.", metricLabels);
    mfs.add(poolMax);
    GaugeMetricFamily poolActive =
        new GaugeMetricFamily(
            "nrt_thread_pool_active", "Number of active threads in the pool.", metricLabels);
    mfs.add(poolActive);
    GaugeMetricFamily poolTasks =
        new GaugeMetricFamily(
            "nrt_thread_pool_tasks", "Number of tasks ever scheduled for execution.", metricLabels);
    mfs.add(poolTasks);
    GaugeMetricFamily poolQueueSize =
        new GaugeMetricFamily(
            "nrt_thread_pool_queue_size", "Current size of pool task queue.", metricLabels);
    mfs.add(poolQueueSize);
    GaugeMetricFamily poolQueueRemaining =
        new GaugeMetricFamily(
            "nrt_thread_pool_queue_remaining", "Capacity left in pool task queue.", metricLabels);
    mfs.add(poolQueueRemaining);

    for (Map.Entry<String, ThreadPoolExecutor> entry : pools.entrySet()) {
      List<String> poolLabel = Collections.singletonList(entry.getKey());
      poolSize.addMetric(poolLabel, entry.getValue().getPoolSize());
      poolMax.addMetric(poolLabel, entry.getValue().getMaximumPoolSize());
      poolActive.addMetric(poolLabel, entry.getValue().getActiveCount());
      poolTasks.addMetric(poolLabel, entry.getValue().getTaskCount());
      poolQueueSize.addMetric(poolLabel, entry.getValue().getQueue().size());
      poolQueueRemaining.addMetric(poolLabel, entry.getValue().getQueue().remainingCapacity());
    }

    return mfs;
  }
}
