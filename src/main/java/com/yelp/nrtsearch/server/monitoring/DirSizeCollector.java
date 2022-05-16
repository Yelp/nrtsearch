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

import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Collector to produce metrics for total size of index directories. Unlike the value produced by
 * {@link IndexMetrics}, this includes files not in the current index version.
 */
public class DirSizeCollector extends Collector {
  private static final Logger logger = LoggerFactory.getLogger(DirSizeCollector.class);
  private final GlobalState globalState;

  public DirSizeCollector(GlobalState globalState) {
    this.globalState = globalState;
  }

  @Override
  public List<MetricFamilySamples> collect() {
    List<MetricFamilySamples> mfs = new ArrayList<>();

    GaugeMetricFamily indexDirSize =
        new GaugeMetricFamily(
            "nrt_index_dir_size_bytes",
            "Total size of all files in index directory.",
            Collections.singletonList("index"));
    mfs.add(indexDirSize);

    try {
      Set<String> indexNames = globalState.getIndexNames();
      for (String indexName : indexNames) {
        Path indexDataPath = globalState.getIndexDir(indexName);
        File indexDataFile = indexDataPath.toFile();
        if (indexDataFile.exists()) {
          long dirSizeBytes = FileUtils.sizeOfDirectory(indexDataFile);
          indexDirSize.addMetric(Collections.singletonList(indexName), (double) dirSizeBytes);
        }
      }
    } catch (Exception e) {
      logger.warn("Error getting directory size metric: ", e);
    }
    return mfs;
  }
}
