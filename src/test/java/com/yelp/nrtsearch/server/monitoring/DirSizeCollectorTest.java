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

import com.yelp.nrtsearch.server.luceneserver.GlobalState;
import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DirSizeCollectorTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

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
    List<MetricFamilySamples> mfs = collector.collect();
    assertEquals(1, mfs.size());
    assertEquals(0, mfs.get(0).samples.size());
  }

  @Test
  public void testIndexDirNotExists() {
    DirSizeCollector collector =
        new DirSizeCollector(getMockState(Collections.singleton("test_index"), folder.getRoot()));
    List<MetricFamilySamples> mfs = collector.collect();
    assertEquals(1, mfs.size());
    assertEquals(0, mfs.get(0).samples.size());
  }

  @Test
  public void testEmptyDir() throws IOException {
    DirSizeCollector collector =
        new DirSizeCollector(getMockState(Collections.singleton("test_index"), folder.getRoot()));

    folder.newFolder("test_index");

    List<MetricFamilySamples> mfs = collector.collect();
    assertEquals(1, mfs.size());
    assertEquals(1, mfs.get(0).samples.size());
    Sample sample = mfs.get(0).samples.get(0);
    assertEquals(0.0, sample.value, 0.0);
    assertEquals(Collections.singletonList("index"), sample.labelNames);
    assertEquals(Collections.singletonList("test_index"), sample.labelValues);
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

    List<MetricFamilySamples> mfs = collector.collect();
    assertEquals(1, mfs.size());
    assertEquals(2, mfs.get(0).samples.size());
    for (Sample sample : mfs.get(0).samples) {
      if (sample.labelValues.get(0).equals("test_index")) {
        assertTrue(sample.value >= 300.0);
      } else if (sample.labelValues.get(0).equals("test_index_2")) {
        assertTrue(sample.value >= 950.0);
      } else {
        fail("Unknown index: " + sample.labelValues.get(0));
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

    List<MetricFamilySamples> mfs = collector.collect();
    assertEquals(1, mfs.size());
    assertEquals(2, mfs.get(0).samples.size());
    for (Sample sample : mfs.get(0).samples) {
      if (sample.labelValues.get(0).equals("test_index")) {
        assertTrue(sample.value >= 300.0);
      } else if (sample.labelValues.get(0).equals("test_index_2")) {
        assertTrue(sample.value >= 950.0);
      } else {
        fail("Unknown index: " + sample.labelValues.get(0));
      }
    }
  }
}
