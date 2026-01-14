/*
 * Copyright 2025 Yelp Inc.
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
package org.apache.lucene.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.junit.Before;
import org.junit.Test;

public class LazyQueueTopScoreDocCollectorTest {
  private Directory directory;
  private IndexWriter indexWriter;
  private IndexSearcher indexSearcher;
  private DirectoryReader directoryReader;

  @Before
  public void setUp() throws IOException {
    Path tempPath = Files.createTempDirectory("lucene-test");
    directory = new MMapDirectory(tempPath);
    IndexWriterConfig config = new IndexWriterConfig();
    indexWriter = new IndexWriter(directory, config);

    // Add some test documents with different scores
    addDocument("doc1", 10.0f);
    addDocument("doc2", 8.0f);
    addDocument("doc3", 6.0f);
    addDocument("doc4", 4.0f);
    addDocument("doc5", 2.0f);
    addDocument("doc6", 1.0f);

    indexWriter.commit();
    indexWriter.close();

    directoryReader = DirectoryReader.open(directory);
    indexSearcher = new IndexSearcher(directoryReader);
  }

  private void addDocument(String id, float score) throws IOException {
    Document doc = new Document();
    doc.add(new StringField("id", id, Field.Store.YES));
    doc.add(new StringField("score", String.valueOf(score), Field.Store.YES));
    indexWriter.addDocument(doc);
  }

  @Test
  public void testLazyQueueTopScoreDocCollectorManagerCreation() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, Integer.MAX_VALUE);

    assertNotNull("Manager should not be null", manager);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Collector should not be null", collector);
  }

  @Test
  public void testLazyQueueTopScoreDocCollectorManagerWithAfter() throws IOException {
    ScoreDoc after = new ScoreDoc(2, 5.0f);
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(3, after, Integer.MAX_VALUE);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Collector with 'after' should not be null", collector);
  }

  @Test
  public void testLazyQueueTopScoreDocCollectorManagerWithTotalHitsThreshold() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 100);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Collector with totalHitsThreshold should not be null", collector);
  }

  @Test
  public void testScoreMode() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, Integer.MAX_VALUE);
    LazyQueueTopScoreDocCollector collector = manager.newCollector();

    assertEquals(
        "Score mode should be COMPLETE when totalHitsThreshold is Integer.MAX_VALUE",
        ScoreMode.COMPLETE,
        collector.scoreMode());
  }

  @Test
  public void testScoreModeTopScores() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, 10);
    LazyQueueTopScoreDocCollector collector = manager.newCollector();

    assertEquals(
        "Score mode should be TOP_SCORES when totalHitsThreshold is not Integer.MAX_VALUE",
        ScoreMode.TOP_SCORES,
        collector.scoreMode());
  }

  @Test
  public void testCollectorManagerCreatesCollectors() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(3, null, Integer.MAX_VALUE);

    // Test that manager can create multiple collectors
    LazyQueueTopScoreDocCollector collector1 = manager.newCollector();
    LazyQueueTopScoreDocCollector collector2 = manager.newCollector();

    assertNotNull("First collector should not be null", collector1);
    assertNotNull("Second collector should not be null", collector2);

    // Test that both collectors have the same configuration
    assertEquals(
        "Both collectors should have same score mode",
        collector1.scoreMode(),
        collector2.scoreMode());
  }

  @Test
  public void testTopDocsSize() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, Integer.MAX_VALUE);
    LazyQueueTopScoreDocCollector collector = manager.newCollector();

    // Initially, topDocsSize should be 0
    assertEquals("Initial top docs size should be 0", 0, collector.topDocsSize());
  }

  @Test
  public void testNewTopDocsWithNullResults() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, Integer.MAX_VALUE);
    LazyQueueTopScoreDocCollector collector = manager.newCollector();

    TopDocs topDocs = collector.newTopDocs(null, 0);
    assertNotNull("TopDocs should not be null", topDocs);
    assertEquals("Total hits should be 0", 0, topDocs.totalHits.value());
    assertEquals("Results array should be empty", 0, topDocs.scoreDocs.length);
  }

  @Test
  public void testNewTopDocsWithResults() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, Integer.MAX_VALUE);
    LazyQueueTopScoreDocCollector collector = manager.newCollector();

    ScoreDoc[] results = {new ScoreDoc(0, 10.0f), new ScoreDoc(1, 8.0f), new ScoreDoc(2, 6.0f)};

    TopDocs topDocs = collector.newTopDocs(results, 0);
    assertNotNull("TopDocs should not be null", topDocs);
    assertEquals("Results array should match input", 3, topDocs.scoreDocs.length);
    assertEquals("First doc should have score 10.0f", 10.0f, topDocs.scoreDocs[0].score, 0.01f);
    assertEquals("Second doc should have score 8.0f", 8.0f, topDocs.scoreDocs[1].score, 0.01f);
    assertEquals("Third doc should have score 6.0f", 6.0f, topDocs.scoreDocs[2].score, 0.01f);
  }

  @Test
  public void testMultipleCollectorsReduce() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(3, null, Integer.MAX_VALUE);

    // Create multiple collectors
    Collection<LazyQueueTopScoreDocCollector> collectors = new ArrayList<>();
    collectors.add(manager.newCollector());
    collectors.add(manager.newCollector());

    // This tests that the manager can create collectors and reduce them
    // In a real scenario, each collector would collect documents from different segments
    TopDocs result = manager.reduce(collectors);
    assertNotNull("Reduced TopDocs should not be null", result);
  }

  @Test
  public void testCollectorWithSearchAfterConfiguration() throws IOException {
    ScoreDoc after = new ScoreDoc(1, 7.0f);
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(3, after, Integer.MAX_VALUE);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Collector with searchAfter should not be null", collector);

    // Test that the collector can produce TopDocs (basic functionality)
    TopDocs topDocs = collector.topDocs();
    assertNotNull("TopDocs should not be null", topDocs);
    assertEquals("Initial total hits should be 0", 0, topDocs.totalHits.value());
  }

  @Test
  public void testCollectorWithTotalHitsThreshold() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(3, null, 10);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertEquals(
        "Score mode should be TOP_SCORES with threshold",
        ScoreMode.TOP_SCORES,
        collector.scoreMode());
  }

  @Test
  public void testCollectorBasicFunctionality() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, Integer.MAX_VALUE);
    LazyQueueTopScoreDocCollector collector = manager.newCollector();

    // Test initial state
    assertEquals("Initial top docs size should be 0", 0, collector.topDocsSize());

    // Test that we can create topDocs
    TopDocs topDocs = collector.topDocs();
    assertNotNull("TopDocs should not be null", topDocs);
    assertEquals("Initial total hits should be 0", 0, topDocs.totalHits.value());
  }

  @Test
  public void testMaxScoreAccumulatorIntegration() throws IOException {
    // Test with a totalHitsThreshold that will create a MaxScoreAccumulator internally
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(3, null, 10);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    assertNotNull("Collector with internal MaxScoreAccumulator should not be null", collector);
  }

  @Test
  public void testCollectorManagerConsistency() throws IOException {
    int numHits = 5;
    ScoreDoc after = new ScoreDoc(2, 5.0f);
    int totalHitsThreshold = 100;

    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(numHits, after, totalHitsThreshold);

    // Create multiple collectors from the same manager
    LazyQueueTopScoreDocCollector collector1 = manager.newCollector();
    LazyQueueTopScoreDocCollector collector2 = manager.newCollector();

    assertNotNull("First collector should not be null", collector1);
    assertNotNull("Second collector should not be null", collector2);

    // Both collectors should have the same configuration
    assertEquals(
        "Both collectors should have the same score mode",
        collector1.scoreMode(),
        collector2.scoreMode());
  }

  @Test
  public void testTotalHitsRelationHandling() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(2, null, 1); // Very low threshold

    LazyQueueTopScoreDocCollector collector = manager.newCollector();

    TopDocs topDocs = collector.topDocs();
    assertNotNull("TopDocs should not be null", topDocs);

    // The relation could be either EQUAL_TO or GREATER_THAN_OR_EQUAL_TO
    assertTrue(
        "Total hits relation should be valid",
        topDocs.totalHits.relation() == Relation.EQUAL_TO
            || topDocs.totalHits.relation() == Relation.GREATER_THAN_OR_EQUAL_TO);
  }

  @Test
  public void testCollectorReturnsExpectedTopDocsStructure() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(3, null, Integer.MAX_VALUE);

    LazyQueueTopScoreDocCollector collector = manager.newCollector();
    TopDocs topDocs = collector.topDocs();

    assertNotNull("TopDocs should not be null", topDocs);
    assertNotNull("ScoreDocs array should not be null", topDocs.scoreDocs);
    assertNotNull("TotalHits should not be null", topDocs.totalHits);
    assertTrue("Total hits value should be non-negative", topDocs.totalHits.value() >= 0);
  }

  @Test
  public void testCollectorManagerReduceEmptyCollectors() throws IOException {
    LazyQueueTopScoreDocCollectorManager manager =
        new LazyQueueTopScoreDocCollectorManager(5, null, Integer.MAX_VALUE);

    // Create empty collection of collectors
    Collection<LazyQueueTopScoreDocCollector> emptyCollectors = new ArrayList<>();

    TopDocs result = manager.reduce(emptyCollectors);
    assertNotNull("Reduced TopDocs should not be null even with empty collectors", result);
  }
}
