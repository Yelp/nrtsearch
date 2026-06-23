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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
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

public class CompoundFileEntriesReaderTest {

  @Rule public TemporaryFolder folder = new TemporaryFolder();

  /** Creates a minimal Lucene index with CFS enabled and returns the index directory path. */
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
  public void testReadEntries_returnsSomeEntries() throws IOException {
    Path indexPath = createIndexWithCfs();
    // Find the .cfe file
    List<Path> cfeFiles =
        java.nio.file.Files.list(indexPath)
            .filter(p -> p.getFileName().toString().endsWith(".cfe"))
            .toList();
    assertEquals("Expected exactly one .cfe file", 1, cfeFiles.size());

    Map<String, CompoundFileEntriesReader.CfeEntry> entries =
        CompoundFileEntriesReader.readEntries(cfeFiles.get(0));
    assertNotNull(entries);
    assertTrue("Expected at least one sub-file entry", entries.size() > 0);
  }

  @Test
  public void testReadEntries_entriesHaveValidOffsets() throws IOException {
    Path indexPath = createIndexWithCfs();
    Path cfeFile =
        java.nio.file.Files.list(indexPath)
            .filter(p -> p.getFileName().toString().endsWith(".cfe"))
            .findFirst()
            .orElseThrow();
    // Find corresponding .cfs file size
    Path cfsFile =
        cfeFile.getParent().resolve(cfeFile.getFileName().toString().replace(".cfe", ".cfs"));
    long cfsSize = java.nio.file.Files.size(cfsFile);

    Map<String, CompoundFileEntriesReader.CfeEntry> entries =
        CompoundFileEntriesReader.readEntries(cfeFile);
    for (CompoundFileEntriesReader.CfeEntry entry : entries.values()) {
      assertTrue("Offset must be non-negative: " + entry.name(), entry.offset() >= 0);
      assertTrue("Length must be positive: " + entry.name(), entry.length() > 0);
      assertTrue(
          "Offset + length must fit within .cfs: " + entry.name(),
          entry.offset() + entry.length() <= cfsSize);
    }
  }

  @Test
  public void testReadEntries_entryNamesMatchExpectedExtensions() throws IOException {
    Path indexPath = createIndexWithCfs();
    Path cfeFile =
        java.nio.file.Files.list(indexPath)
            .filter(p -> p.getFileName().toString().endsWith(".cfe"))
            .findFirst()
            .orElseThrow();

    Map<String, CompoundFileEntriesReader.CfeEntry> entries =
        CompoundFileEntriesReader.readEntries(cfeFile);
    // All entry names should have a recognizable Lucene extension
    for (String name : entries.keySet()) {
      assertTrue("Entry name should contain a dot: " + name, name.contains("."));
    }
  }
}
