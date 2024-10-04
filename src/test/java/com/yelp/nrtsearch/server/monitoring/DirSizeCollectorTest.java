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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.state.GlobalState;
import io.prometheus.metrics.model.snapshots.DataPointSnapshot;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.Labels;
import io.prometheus.metrics.model.snapshots.MetricSnapshots;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DirSizeCollectorTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException {
    DirSizeCollector.indexDirSize.clear();
  }

  private GlobalState getMockState(Set<String> indexNames, File baseDir) {
    GlobalState mockGlobalState = mock(GlobalState.class);
    when(mockGlobalState.getIndexNames()).thenReturn(indexNames);
    when(mockGlobalState.getIndexDir(any(String.class)))
        .then(i -> Paths.get(baseDir.toString(), i.getArgument(0, String.class)));
    return mockGlobalState;
  }

  private void writeFile(File directory, String fileName, int size) throws IOException {
    Random random = new Random();
    File outfile = Paths.get(directory.toString(), fileName).toFile();
    byte[] data = new byte[size];
    random.nextBytes(data);
    try (FileOutputStream outputStream = new FileOutputStream(outfile)) {
      outputStream.write(data);
    }
  }

  @Test
  public void testNoIndices() {
    DirSizeCollector collector =
        new DirSizeCollector(getMockState(Collections.emptySet(), folder.getRoot()));
    MetricSnapshots metrics = collector.collect();
    assertEquals(1, metrics.size());
    assertEquals(0, metrics.get(0).getDataPoints().size());
  }

  @Test
  public void testIndexDirNotExists() {
    DirSizeCollector collector =
        new DirSizeCollector(getMockState(Collections.singleton("test_index"), folder.getRoot()));
    MetricSnapshots metrics = collector.collect();
    assertEquals(1, metrics.size());
    assertEquals(0, metrics.get(0).getDataPoints().size());
  }

  @Test
  public void testEmptyDir() throws IOException {
    DirSizeCollector collector =
        new DirSizeCollector(getMockState(Collections.singleton("test_index"), folder.getRoot()));

    folder.newFolder("test_index");

    MetricSnapshots metrics = collector.collect();
    assertEquals(1, metrics.size());
    assertEquals(1, metrics.get(0).getDataPoints().size());
    GaugeSnapshot.GaugeDataPointSnapshot sample =
        (GaugeSnapshot.GaugeDataPointSnapshot) metrics.get(0).getDataPoints().getFirst();
    assertEquals(0.0, sample.getValue(), 0.0);
    Labels labels = sample.getLabels();
    assertEquals(1, labels.size());
    assertEquals("test_index", labels.get("index"));
  }

  @Test
  public void testDirSize() throws IOException {
    DirSizeCollector collector =
        new DirSizeCollector(getMockState(Set.of("test_index", "test_index_2"), folder.getRoot()));

    File indexDir = folder.newFolder("test_index");
    writeFile(indexDir, "file1", 100);
    writeFile(indexDir, "file2", 200);
    indexDir = folder.newFolder("test_index_2");
    writeFile(indexDir, "file3", 500);
    writeFile(indexDir, "file4", 50);
    writeFile(indexDir, "file5", 400);

    MetricSnapshots metrics = collector.collect();
    assertEquals(1, metrics.size());
    assertEquals(2, metrics.get(0).getDataPoints().size());
    for (DataPointSnapshot dataPoint : metrics.get(0).getDataPoints()) {
      GaugeSnapshot.GaugeDataPointSnapshot sample =
          (GaugeSnapshot.GaugeDataPointSnapshot) dataPoint;
      Labels labels = sample.getLabels();
      assertEquals(1, labels.size());
      if (labels.get("index").equals("test_index")) {
        assertTrue(sample.getValue() >= 300.0);
      } else if (labels.get("index").equals("test_index_2")) {
        assertTrue(sample.getValue() >= 950.0);
      } else {
        fail("Unknown index: " + labels.get("index"));
      }
    }
  }

  @Test
  public void testDirSymlink() throws IOException {
    DirSizeCollector collector =
        new DirSizeCollector(getMockState(Set.of("test_index", "test_index_2"), folder.getRoot()));

    File indexDir = folder.newFolder("test_index");
    writeFile(indexDir, "file1", 100);
    writeFile(indexDir, "file2", 200);
    indexDir = folder.newFolder("not_test_index_2");
    writeFile(indexDir, "file3", 500);
    writeFile(indexDir, "file4", 50);
    writeFile(indexDir, "file5", 400);
    Files.createSymbolicLink(
        Paths.get(folder.getRoot().toString(), "test_index_2"), Paths.get(indexDir.toString()));

    MetricSnapshots metrics = collector.collect();
    assertEquals(1, metrics.size());
    assertEquals(2, metrics.get(0).getDataPoints().size());
    for (DataPointSnapshot dataPoint : metrics.get(0).getDataPoints()) {
      GaugeSnapshot.GaugeDataPointSnapshot sample =
          (GaugeSnapshot.GaugeDataPointSnapshot) dataPoint;
      Labels labels = sample.getLabels();
      assertEquals(1, labels.size());
      if (labels.getValue(0).equals("test_index")) {
        assertTrue(sample.getValue() >= 300.0);
      } else if (labels.getValue(0).equals("test_index_2")) {
        assertTrue(sample.getValue() >= 950.0);
      } else {
        fail("Unknown index: " + labels.getValue(0));
      }
    }
  }
}
