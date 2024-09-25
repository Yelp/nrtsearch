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
package com.yelp.nrtsearch.server.monitoring;

import io.prometheus.metrics.core.metrics.Gauge;
import io.prometheus.metrics.model.registry.MultiCollector;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Collect stat metrics for this process from procfs. */
public class ProcStatCollector implements MultiCollector {
  private static final Logger logger = LoggerFactory.getLogger(ProcStatCollector.class);
  private static final String STAT_PATH_FORMAT = "/proc/%d/stat";
  private static final int MINOR_FAULT_INDEX = 9;
  private static final int CHILD_MINOR_FAULT_INDEX = 10;
  private static final int MAJOR_FAULT_INDEX = 11;
  private static final int CHILD_MAJOR_FAULT_INDEX = 12;
  private static final Gauge minorFaults =
      Gauge.builder()
          .name("nrt_stat_minor_faults")
          .help("Total number of minor page faults.")
          .build();
  private static final Gauge childMinorFaults =
      Gauge.builder()
          .name("nrt_stat_child_minor_faults")
          .help("Total number of minor page faults by waited-for children.")
          .build();
  private static final Gauge majorFaults =
      Gauge.builder()
          .name("nrt_stat_major_faults")
          .help("Total number of major page faults.")
          .build();
  private static final Gauge childMajorFaults =
      Gauge.builder()
          .name("nrt_stat_child_major_faults")
          .help("Total number of major page faults by waited-for children.")
          .build();

  private final File statFile;
  private boolean collectStat;

  public ProcStatCollector() {
    long pid = ProcessHandle.current().pid();
    statFile = new File(String.format(STAT_PATH_FORMAT, pid));
    collectStat = statFile.exists();
  }

  @Override
  public MetricSnapshots collect() {
    if (collectStat) {
      List<MetricSnapshot> metrics = new ArrayList<>();

      List<String> lines;
      try {
        lines = Files.readAllLines(statFile.toPath());
      } catch (IOException e) {
        logger.warn("Error reading stat info", e);
        collectStat = false;
        return new MetricSnapshots();
      }

      try {
        for (String line : lines) {
          String[] components = line.split("\\s+");
          if (components.length > CHILD_MAJOR_FAULT_INDEX) {
            minorFaults.set(Double.parseDouble(components[MINOR_FAULT_INDEX]));
            childMinorFaults.set(Double.parseDouble(components[CHILD_MINOR_FAULT_INDEX]));
            majorFaults.set(Double.parseDouble(components[MAJOR_FAULT_INDEX]));
            childMajorFaults.set(Double.parseDouble(components[CHILD_MAJOR_FAULT_INDEX]));
          }
        }
        metrics.add(minorFaults.collect());
        metrics.add(childMinorFaults.collect());
        metrics.add(majorFaults.collect());
        metrics.add(childMajorFaults.collect());
      } catch (Exception e) {
        logger.warn("Error parsing stat result", e);
        collectStat = false;
        return new MetricSnapshots();
      }

      return new MetricSnapshots(metrics);
    } else {
      return new MetricSnapshots();
    }
  }
}
