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
package com.yelp.nrtsearch.server.monitoring;

import com.google.common.annotations.VisibleForTesting;
import com.yelp.nrtsearch.server.state.GlobalState;
import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.file.PathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collector to produce metrics for total size of index directories. Unlike the value produced by
 * {@link IndexMetrics}, this includes files not in the current index version.
 */
public class DirSizeCollector implements MultiCollector {
  private static final Logger logger = LoggerFactory.getLogger(DirSizeCollector.class);

  @VisibleForTesting
  static final Gauge indexDirSize =
      Gauge.builder()
          .name("nrt_index_dir_size_bytes")
          .help("Total size of all files in index directory.")
          .labelNames("index")
          .build();

  private final GlobalState globalState;

  public DirSizeCollector(GlobalState globalState) {
    this.globalState = globalState;
  }

  @Override
  public MetricSnapshots collect() {
    List<MetricSnapshot> metrics = new ArrayList<>();

    try {
      Set<String> indexNames = globalState.getIndexNames();
      for (String indexName : indexNames) {
        Path indexDataPath = globalState.getIndexDir(indexName);
        if (Files.exists(indexDataPath)) {
          if (Files.isSymbolicLink(indexDataPath)) {
            indexDataPath = Files.readSymbolicLink(indexDataPath);
          }
          long dirSizeBytes = PathUtils.sizeOfDirectory(indexDataPath);
          indexDirSize.labelValues(indexName).set((double) dirSizeBytes);
        }
      }
      metrics.add(indexDirSize.collect());
    } catch (Exception e) {
      logger.warn("Error getting directory size metric: ", e);
    }
    return new MetricSnapshots(metrics);
  }
}
