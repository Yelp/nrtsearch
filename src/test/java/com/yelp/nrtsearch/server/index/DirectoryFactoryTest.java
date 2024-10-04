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
package com.yelp.nrtsearch.server.index;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.config.IndexPreloadConfig;
import com.yelp.nrtsearch.server.config.YamlConfigReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class DirectoryFactoryTest {

  @ClassRule public static final TemporaryFolder folder = new TemporaryFolder();

  private static final DirectoryFactory mmFactory = DirectoryFactory.get("MMapDirectory");
  private static final DirectoryFactory fsFactory = DirectoryFactory.get("FSDirectory");

  @Test
  public void testMMapDefault() throws IOException {
    String configFile = "nodeName: \"lucene_server_foo\"";
    IndexPreloadConfig config =
        IndexPreloadConfig.fromConfig(
            new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testMMapPreloadFalseWithExtension() throws IOException {
    IndexPreloadConfig config =
        new IndexPreloadConfig(false, Collections.singleton(IndexPreloadConfig.ALL_EXTENSIONS));
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testMMapPreloadEmptyExtensions() throws IOException {
    IndexPreloadConfig config = new IndexPreloadConfig(true, Collections.emptySet());
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testMMapPreloadAll() throws IOException {
    IndexPreloadConfig config =
        new IndexPreloadConfig(true, Collections.singleton(IndexPreloadConfig.ALL_EXTENSIONS));
    try (Directory directory = mmFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertTrue(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testFSDefault() throws IOException {
    String configFile = "nodeName: \"lucene_server_foo\"";
    IndexPreloadConfig config =
        IndexPreloadConfig.fromConfig(
            new YamlConfigReader(new ByteArrayInputStream(configFile.getBytes())));
    try (Directory directory = fsFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testFSPreloadFalseWithExtension() throws IOException {
    IndexPreloadConfig config =
        new IndexPreloadConfig(false, Collections.singleton(IndexPreloadConfig.ALL_EXTENSIONS));
    try (Directory directory = fsFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testFSPreloadEmptyExtensions() throws IOException {
    IndexPreloadConfig config = new IndexPreloadConfig(true, Collections.emptySet());
    try (Directory directory = fsFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertFalse(((MMapDirectory) directory).getPreload());
    }
  }

  @Test
  public void testFSPreloadAll() throws IOException {
    IndexPreloadConfig config =
        new IndexPreloadConfig(true, Collections.singleton(IndexPreloadConfig.ALL_EXTENSIONS));
    try (Directory directory = fsFactory.open(folder.getRoot().toPath(), config)) {
      assertTrue(directory instanceof MMapDirectory);
      assertTrue(((MMapDirectory) directory).getPreload());
    }
  }
}
