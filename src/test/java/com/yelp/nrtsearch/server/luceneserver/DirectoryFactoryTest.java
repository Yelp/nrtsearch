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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexPreloadConfig;
import com.yelp.nrtsearch.server.config.YamlConfigReader;
import com.yelp.nrtsearch.server.luceneserver.DirectoryFactory.SplitMMapDirectory;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Set;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DirectoryFactoryTest {

  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();

  private static final DirectoryFactory mmFactory = DirectoryFactory.get("MMapDirectory");

  @Test
  public void testAllPreset() throws IOException {
    try (Directory directory =
        mmFactory.open(folder.getRoot().toPath(), IndexPreloadConfig.PRELOAD_ALL)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(directory instanceof SplitMMapDirectory);
      assertTrue(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testDefault() throws IOException {
    String configFile = "nodeName: \"lucene_server_foo\"";
    IndexPreloadConfig config =
        IndexPreloadConfig.fromConfig(
            new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(directory instanceof SplitMMapDirectory);
      assertTrue(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testPreloadFalse() throws IOException {
    IndexPreloadConfig config = new IndexPreloadConfig(false, Collections.emptySet());
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(directory instanceof SplitMMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testPreloadFalseWithExtension() throws IOException {
    IndexPreloadConfig config =
        new IndexPreloadConfig(false, Collections.singleton(IndexPreloadConfig.ALL_EXTENSIONS));
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(directory instanceof SplitMMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testPreloadEmptyExtensions() throws IOException {
    IndexPreloadConfig config = new IndexPreloadConfig(true, Collections.emptySet());
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(directory instanceof SplitMMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testPreloadAll() throws IOException {
    IndexPreloadConfig config =
        new IndexPreloadConfig(true, Collections.singleton(IndexPreloadConfig.ALL_EXTENSIONS));
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(directory instanceof SplitMMapDirectory);
      assertTrue(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testPreloadExtensions() throws IOException {
    IndexPreloadConfig config = new IndexPreloadConfig(true, Set.of("doc", "dim"));
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertTrue(directory instanceof SplitMMapDirectory);
      SplitMMapDirectory splitMMapDirectory = (SplitMMapDirectory) directory;
      assertFalse(splitMMapDirectory.getPreload());
      assertTrue(splitMMapDirectory.getSplitToDirectory().getPreload());
      assertTrue(splitMMapDirectory.shouldSplit("file.doc"));
      assertTrue(splitMMapDirectory.shouldSplit("file.dim"));
      assertFalse(splitMMapDirectory.shouldSplit("file.nvd"));
      assertFalse(splitMMapDirectory.shouldSplit("file.tim"));
    }
  }

  @Test
  public void testSplitReading() throws IOException {
    IndexPreloadConfig config = new IndexPreloadConfig(true, Set.of("doc", "dim"));
    writeTestFiles();
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertTrue(directory instanceof SplitMMapDirectory);
      SplitMMapDirectory splitMMapDirectory = (SplitMMapDirectory) directory;
      assertFalse(splitMMapDirectory.getPreload());
      assertTrue(splitMMapDirectory.getSplitToDirectory().getPreload());
      assertTrue(splitMMapDirectory.shouldSplit("file.doc"));
      assertFalse(splitMMapDirectory.shouldSplit("file.tim"));
      assertFileContents("file.doc", directory, "DOC File Contents");
      assertFileContents("file.tim", directory, "TIM File Contents");
    }
  }

  private void writeTestFiles() throws IOException {
    writeTestFile("file.doc", "DOC File Contents");
    writeTestFile("file.tim", "TIM File Contents");
  }

  private void writeTestFile(String name, String contents) throws IOException {
    String fullName = Paths.get(folder.getRoot().getAbsolutePath(), name).toString();
    BufferedWriter writer = new BufferedWriter(new FileWriter(fullName));
    writer.write(contents);
    writer.close();
  }

  private void assertFileContents(String file, Directory directory, String expectedContents)
      throws IOException {
    try (IndexInput fileInput = directory.openInput(file, IOContext.DEFAULT)) {
      byte[] fileArray = new byte[(int) fileInput.length()];
      fileInput.readBytes(fileArray, 0, (int) fileInput.length());
      String fileString = new String(fileArray);
      assertEquals(expectedContents, fileString);
    }
  }
}
