/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.rescore;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ScriptRescoreTest {

  /** DoubleValuesSource that returns a fixed value regardless of doc. */
  private static DoubleValuesSource constantSource(double value) {
    return new DoubleValuesSource() {
      @Override
      public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) {
        return new DoubleValues() {
          @Override
          public double doubleValue() {
            return value;
          }

          @Override
          public boolean advanceExact(int doc) {
            return true;
          }
        };
      }

      @Override
      public boolean needsScores() {
        return false;
      }

      @Override
      public DoubleValuesSource rewrite(IndexSearcher reader) {
        return this;
      }

      @Override
      public int hashCode() {
        return Double.hashCode(value);
      }

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }

      @Override
      public String toString() {
        return "constant(" + value + ")";
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }
    };
  }

  /**
   * DoubleValuesSource that returns {@code multiplier * previousScore} using the scores
   * DoubleValues passed by ScriptRescore (simulating get_score() in a script).
   */
  private static DoubleValuesSource multiplierOfPreviousScore(double multiplier) {
    return new DoubleValuesSource() {
      @Override
      public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) {
        return new DoubleValues() {
          @Override
          public double doubleValue() throws IOException {
            return scores.doubleValue() * multiplier;
          }

          @Override
          public boolean advanceExact(int doc) throws IOException {
            return scores.advanceExact(doc);
          }
        };
      }

      @Override
      public boolean needsScores() {
        return true;
      }

      @Override
      public DoubleValuesSource rewrite(IndexSearcher reader) {
        return this;
      }

      @Override
      public int hashCode() {
        return Double.hashCode(multiplier);
      }

      @Override
      public boolean equals(Object obj) {
        return this == obj;
      }

      @Override
      public String toString() {
        return "multiplier(" + multiplier + ")";
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }

  private RescoreContext buildContext(IndexSearcher searcher) {
    com.yelp.nrtsearch.server.search.SearchContext searchContext =
        mock(com.yelp.nrtsearch.server.search.SearchContext.class);
    org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy sat =
        mock(org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy.class);
    when(searchContext.getSearcherAndTaxonomy()).thenReturn(sat);
    when(sat.searcher()).thenReturn(searcher);
    return new RescoreContext(3, searchContext);
  }

  @Test
  public void testConstantScoreReplacesPreviousScore() throws Exception {
    // Build a tiny in-memory index with 3 docs spread across two segments.
    try (org.apache.lucene.store.ByteBuffersDirectory dir =
            new org.apache.lucene.store.ByteBuffersDirectory();
        org.apache.lucene.index.IndexWriter writer =
            new org.apache.lucene.index.IndexWriter(
                dir,
                new org.apache.lucene.index.IndexWriterConfig()
                    .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {

      writer.addDocument(Collections.emptyList());
      writer.addDocument(Collections.emptyList());
      writer.commit();
      writer.addDocument(Collections.emptyList());
      writer.commit();

      try (org.apache.lucene.index.DirectoryReader reader =
          org.apache.lucene.index.DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        ScoreDoc[] input = {new ScoreDoc(0, 3.0f), new ScoreDoc(1, 2.0f), new ScoreDoc(2, 1.0f)};
        TopDocs hits = new TopDocs(new TotalHits(3, Relation.EQUAL_TO), input);

        ScriptRescore rescorer = new ScriptRescore(constantSource(5.0));
        TopDocs result = rescorer.rescore(hits, buildContext(searcher));

        assertEquals(3, result.scoreDocs.length);
        // All docs get score 5.0 — order may vary but scores should all be 5.0.
        for (ScoreDoc sd : result.scoreDocs) {
          assertEquals(5.0f, sd.score, 0.0001f);
        }
      }
    }
  }

  @Test
  public void testScoreOrderAfterRescore() throws Exception {
    try (org.apache.lucene.store.ByteBuffersDirectory dir =
            new org.apache.lucene.store.ByteBuffersDirectory();
        org.apache.lucene.index.IndexWriter writer =
            new org.apache.lucene.index.IndexWriter(
                dir,
                new org.apache.lucene.index.IndexWriterConfig()
                    .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {

      // Three docs in one segment.
      writer.addDocument(Collections.emptyList()); // doc 0
      writer.addDocument(Collections.emptyList()); // doc 1
      writer.addDocument(Collections.emptyList()); // doc 2
      writer.commit();

      try (org.apache.lucene.index.DirectoryReader reader =
          org.apache.lucene.index.DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        // Input scores: doc0=1, doc1=2, doc2=3 (ascending).
        ScoreDoc[] input = {new ScoreDoc(2, 3.0f), new ScoreDoc(1, 2.0f), new ScoreDoc(0, 1.0f)};
        TopDocs hits = new TopDocs(new TotalHits(3, Relation.EQUAL_TO), input);

        // Script doubles the previous score.
        ScriptRescore rescorer = new ScriptRescore(multiplierOfPreviousScore(2.0));
        TopDocs result = rescorer.rescore(hits, buildContext(searcher));

        assertEquals(3, result.scoreDocs.length);
        // Scores doubled: doc2=6, doc1=4, doc0=2, still descending.
        assertEquals(6.0f, result.scoreDocs[0].score, 0.0001f);
        assertEquals(2, result.scoreDocs[0].doc);
        assertEquals(4.0f, result.scoreDocs[1].score, 0.0001f);
        assertEquals(1, result.scoreDocs[1].doc);
        assertEquals(2.0f, result.scoreDocs[2].score, 0.0001f);
        assertEquals(0, result.scoreDocs[2].doc);
      }
    }
  }

  @Test
  public void testRescoreAcrossMultipleSegments() throws Exception {
    try (org.apache.lucene.store.ByteBuffersDirectory dir =
            new org.apache.lucene.store.ByteBuffersDirectory();
        org.apache.lucene.index.IndexWriter writer =
            new org.apache.lucene.index.IndexWriter(
                dir,
                new org.apache.lucene.index.IndexWriterConfig()
                    .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {

      // Two docs in segment 0 (docBase=0), one doc in segment 1 (docBase=2).
      writer.addDocument(Collections.emptyList()); // global doc 0
      writer.addDocument(Collections.emptyList()); // global doc 1
      writer.commit();
      writer.addDocument(Collections.emptyList()); // global doc 2
      writer.commit();

      try (org.apache.lucene.index.DirectoryReader reader =
          org.apache.lucene.index.DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        ScoreDoc[] input = {new ScoreDoc(0, 10.0f), new ScoreDoc(1, 5.0f), new ScoreDoc(2, 1.0f)};
        TopDocs hits = new TopDocs(new TotalHits(3, Relation.EQUAL_TO), input);

        ScriptRescore rescorer = new ScriptRescore(multiplierOfPreviousScore(3.0));
        TopDocs result = rescorer.rescore(hits, buildContext(searcher));

        assertEquals(3, result.scoreDocs.length);
        assertEquals(30.0f, result.scoreDocs[0].score, 0.0001f);
        assertEquals(0, result.scoreDocs[0].doc);
        assertEquals(15.0f, result.scoreDocs[1].score, 0.0001f);
        assertEquals(1, result.scoreDocs[1].doc);
        assertEquals(3.0f, result.scoreDocs[2].score, 0.0001f);
        assertEquals(2, result.scoreDocs[2].doc);
      }
    }
  }

  @Test
  public void testRescoreInvertsOrderWhenScriptInvertsScores() throws Exception {
    try (org.apache.lucene.store.ByteBuffersDirectory dir =
            new org.apache.lucene.store.ByteBuffersDirectory();
        org.apache.lucene.index.IndexWriter writer =
            new org.apache.lucene.index.IndexWriter(
                dir,
                new org.apache.lucene.index.IndexWriterConfig()
                    .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {

      writer.addDocument(Collections.emptyList()); // doc 0
      writer.addDocument(Collections.emptyList()); // doc 1
      writer.addDocument(Collections.emptyList()); // doc 2
      writer.commit();

      try (org.apache.lucene.index.DirectoryReader reader =
          org.apache.lucene.index.DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        // Initial ranking: doc2 > doc1 > doc0 by score.
        ScoreDoc[] input = {new ScoreDoc(2, 3.0f), new ScoreDoc(1, 2.0f), new ScoreDoc(0, 1.0f)};
        TopDocs hits = new TopDocs(new TotalHits(3, Relation.EQUAL_TO), input);

        // Script replaces score with a constant low value for higher scores and high for lower —
        // simulated by assigning (4 - previousScore): doc2 gets 1, doc1 gets 2, doc0 gets 3.
        DoubleValuesSource invertingSource =
            new DoubleValuesSource() {
              @Override
              public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) {
                return new DoubleValues() {
                  @Override
                  public double doubleValue() throws IOException {
                    return 4.0 - scores.doubleValue();
                  }

                  @Override
                  public boolean advanceExact(int doc) throws IOException {
                    return scores.advanceExact(doc);
                  }
                };
              }

              @Override
              public boolean needsScores() {
                return true;
              }

              @Override
              public DoubleValuesSource rewrite(IndexSearcher r) {
                return this;
              }

              @Override
              public int hashCode() {
                return 42;
              }

              @Override
              public boolean equals(Object o) {
                return this == o;
              }

              @Override
              public String toString() {
                return "invert";
              }

              @Override
              public boolean isCacheable(LeafReaderContext ctx) {
                return false;
              }
            };

        ScriptRescore rescorer = new ScriptRescore(invertingSource);
        TopDocs result = rescorer.rescore(hits, buildContext(searcher));

        assertEquals(3, result.scoreDocs.length);
        // After inversion: doc0 should be first (score 3), doc1 second (2), doc2 third (1).
        assertEquals(0, result.scoreDocs[0].doc);
        assertEquals(3.0f, result.scoreDocs[0].score, 0.0001f);
        assertEquals(1, result.scoreDocs[1].doc);
        assertEquals(2.0f, result.scoreDocs[1].score, 0.0001f);
        assertEquals(2, result.scoreDocs[2].doc);
        assertEquals(1.0f, result.scoreDocs[2].score, 0.0001f);
      }
    }
  }

  @Test
  public void testTotalHitsPreserved() throws Exception {
    try (org.apache.lucene.store.ByteBuffersDirectory dir =
            new org.apache.lucene.store.ByteBuffersDirectory();
        org.apache.lucene.index.IndexWriter writer =
            new org.apache.lucene.index.IndexWriter(
                dir,
                new org.apache.lucene.index.IndexWriterConfig()
                    .setMergePolicy(org.apache.lucene.index.NoMergePolicy.INSTANCE))) {

      writer.addDocument(Collections.emptyList());
      writer.commit();

      try (org.apache.lucene.index.DirectoryReader reader =
          org.apache.lucene.index.DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs hits =
            new TopDocs(
                new TotalHits(999, Relation.GREATER_THAN_OR_EQUAL_TO),
                new ScoreDoc[] {new ScoreDoc(0, 1.0f)});

        ScriptRescore rescorer = new ScriptRescore(constantSource(2.0));
        TopDocs result = rescorer.rescore(hits, buildContext(searcher));

        assertEquals(999, result.totalHits.value());
        assertEquals(Relation.GREATER_THAN_OR_EQUAL_TO, result.totalHits.relation());
      }
    }
  }
}
