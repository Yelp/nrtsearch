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
package com.yelp.nrtsearch.server.utils;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.yelp.nrtsearch.server.config.PageCacheEvictionConfig;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.FSDirectory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;

public class PageCacheEvictionServiceTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  private PageCacheEvictor mockEvictor() {
    PageCacheEvictor evictor = mock(PageCacheEvictor.class);
    doReturn(true).when(evictor).isSupported();
    return evictor;
  }

  private Path createIndexWithCfs() throws IOException {
    Path indexPath = folder.newFolder("index").toPath();
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    config.setUseCompoundFile(true);
    try (FSDirectory dir = FSDirectory.open(indexPath);
        IndexWriter writer = new IndexWriter(dir, config)) {
      Document doc = new Document();
      doc.add(new TextField("body", "hello world", Field.Store.YES));
      writer.addDocument(doc);
      writer.commit();
    }
    return indexPath;
  }

  @Test
  public void testEvictColdData_disabled_noEviction() throws IOException {
    PageCacheEvictor evictor = mockEvictor();
    PageCacheEvictionConfig config = new PageCacheEvictionConfig(false, Set.of("vec"));
    PageCacheEvictionService service = new PageCacheEvictionService(config, evictor);

    service.evictColdData(folder.getRoot().toPath(), List.of("_0.vec", "_0.pos"));

    verify(evictor, never()).evictFile(any());
    verify(evictor, never()).evictFileRange(any(), anyLong(), anyLong());
  }

  @Test
  public void testEvictColdData_evictorNotSupported_noEviction() throws IOException {
    PageCacheEvictor evictor = mock(PageCacheEvictor.class);
    doReturn(false).when(evictor).isSupported();
    PageCacheEvictionConfig config = new PageCacheEvictionConfig(true, Set.of("vec"));
    PageCacheEvictionService service = new PageCacheEvictionService(config, evictor);

    service.evictColdData(folder.getRoot().toPath(), List.of("_0.vec"));

    verify(evictor, never()).evictFile(any());
  }

  @Test
  public void testEvictColdData_standaloneFiles_onlyColdEvicted() throws IOException {
    Path indexDir = folder.getRoot().toPath();
    // Create actual files so evictFile can run
    java.nio.file.Files.createFile(indexDir.resolve("_0.vec"));
    java.nio.file.Files.createFile(indexDir.resolve("_0.pos"));

    PageCacheEvictor evictor = mockEvictor();
    PageCacheEvictionConfig config = new PageCacheEvictionConfig(true, Set.of("vec"));
    PageCacheEvictionService service = new PageCacheEvictionService(config, evictor);

    service.evictColdData(indexDir, List.of("_0.vec", "_0.pos"));

    ArgumentCaptor<Path> captor = ArgumentCaptor.forClass(Path.class);
    verify(evictor, times(1)).evictFile(captor.capture());
    assertEquals(indexDir.resolve("_0.vec"), captor.getValue());
  }

  @Test
  public void testEvictColdData_wildcard_evictsAll() throws IOException {
    Path indexDir = folder.getRoot().toPath();
    java.nio.file.Files.createFile(indexDir.resolve("_0.vec"));
    java.nio.file.Files.createFile(indexDir.resolve("_0.pos"));
    java.nio.file.Files.createFile(indexDir.resolve("_0.dvd"));

    PageCacheEvictor evictor = mockEvictor();
    PageCacheEvictionConfig config =
        new PageCacheEvictionConfig(true, Set.of(PageCacheEvictionConfig.ALL_EXTENSIONS));
    PageCacheEvictionService service = new PageCacheEvictionService(config, evictor);

    service.evictColdData(indexDir, List.of("_0.vec", "_0.pos", "_0.dvd"));

    verify(evictor, times(3)).evictFile(any());
  }

  @Test
  public void testEvictColdData_cfeFilesNeverEvicted() throws IOException {
    Path indexDir = folder.getRoot().toPath();
    java.nio.file.Files.createFile(indexDir.resolve("_0.cfe"));

    PageCacheEvictor evictor = mockEvictor();
    PageCacheEvictionConfig config =
        new PageCacheEvictionConfig(true, Set.of(PageCacheEvictionConfig.ALL_EXTENSIONS));
    PageCacheEvictionService service = new PageCacheEvictionService(config, evictor);

    service.evictColdData(indexDir, List.of("_0.cfe"));

    verify(evictor, never()).evictFile(any());
    verify(evictor, never()).evictFileRange(any(), anyLong(), anyLong());
  }

  @Test
  public void testEvictColdDataForCfs_evictsColdSubFileRanges() throws IOException {
    Path indexPath = createIndexWithCfs();

    // Find the .cfs/.cfe files
    List<Path> cfsFiles =
        java.nio.file.Files.list(indexPath)
            .filter(p -> p.getFileName().toString().endsWith(".cfs"))
            .toList();
    assertEquals(1, cfsFiles.size());
    String cfsFileName = cfsFiles.get(0).getFileName().toString();

    // Get actual entries so we know what extensions are in the CFS
    Path cfeFile = indexPath.resolve(cfsFileName.replace(".cfs", ".cfe"));
    Map<String, CompoundFileEntriesReader.CfeEntry> entries =
        CompoundFileEntriesReader.readEntries(cfeFile);
    String firstExt = entries.values().iterator().next().name().replaceFirst(".*\\.", "");

    PageCacheEvictor evictor = mockEvictor();
    PageCacheEvictionConfig config = new PageCacheEvictionConfig(true, Set.of(firstExt));
    PageCacheEvictionService service = new PageCacheEvictionService(config, evictor);

    service.evictColdDataForCfs(indexPath, cfsFileName);

    // Should have called evictFileRange at least once for that extension
    verify(evictor, times(1)).evictFileRange(any(), anyLong(), anyLong());
  }

  @Test
  public void testEvictColdData_cfsTriggersRangeEviction() throws IOException {
    Path indexPath = createIndexWithCfs();
    List<Path> cfsFiles =
        java.nio.file.Files.list(indexPath)
            .filter(p -> p.getFileName().toString().endsWith(".cfs"))
            .toList();
    String cfsFileName = cfsFiles.get(0).getFileName().toString();
    Path cfeFile = indexPath.resolve(cfsFileName.replace(".cfs", ".cfe"));
    int numEntries = CompoundFileEntriesReader.readEntries(cfeFile).size();

    PageCacheEvictor evictor = mockEvictor();
    // Use wildcard to evict all sub-files in the CFS
    PageCacheEvictionConfig config =
        new PageCacheEvictionConfig(true, Set.of(PageCacheEvictionConfig.ALL_EXTENSIONS));
    PageCacheEvictionService service = new PageCacheEvictionService(config, evictor);

    service.evictColdData(indexPath, List.of(cfsFileName));

    // The CFS should trigger one range eviction per sub-file, not whole-file eviction
    verify(evictor, never()).evictFile(any());
    verify(evictor, times(numEntries)).evictFileRange(any(), anyLong(), anyLong());
  }
}
