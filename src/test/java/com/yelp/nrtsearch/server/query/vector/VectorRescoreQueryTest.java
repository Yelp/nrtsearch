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
package com.yelp.nrtsearch.server.query.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.FullPrecisionFloatVectorSimilarityValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class VectorRescoreQueryTest {
  private static final String VECTOR_FIELD = "vector";
  private static final String RANK_FIELD = "rank";
  private static final int NUM_DOCS = 20;
  private static final float[] QUERY_VECTOR = new float[] {1.0f, 0.5f, 0.25f};

  private static Directory directory;
  private static DirectoryReader reader;
  private static IndexSearcher searcher;

  @BeforeClass
  public static void setup() throws IOException {
    directory = new ByteBuffersDirectory();
    try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
      for (int i = 0; i < NUM_DOCS; ++i) {
        Document document = new Document();
        document.add(
            new KnnFloatVectorField(
                VECTOR_FIELD, docVector(i), VectorSimilarityFunction.EUCLIDEAN));
        document.add(new NumericDocValuesField(RANK_FIELD, i));
        writer.addDocument(document);
      }
      writer.commit();
    }
    reader = DirectoryReader.open(directory);
    searcher = new IndexSearcher(reader);
  }

  @AfterClass
  public static void cleanup() throws IOException {
    reader.close();
    directory.close();
  }

  private static float[] docVector(int i) {
    return new float[] {i * 0.1f, i * 0.05f, 1.0f - i * 0.02f};
  }

  private static NrtKnnFloatVectorQuery knnQuery(int k, Query filter) {
    return new NrtKnnFloatVectorQuery(
        VECTOR_FIELD, QUERY_VECTOR, k, filter, k, new KnnSearchStrategy.Hnsw(0));
  }

  private static DoubleValuesSource fullPrecisionSource() {
    return new FullPrecisionFloatVectorSimilarityValuesSource(QUERY_VECTOR, VECTOR_FIELD);
  }

  @Test
  public void testInvalidN() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> new VectorRescoreQuery(knnQuery(10, null), fullPrecisionSource(), 0));
    assertEquals("n must be >= 1", e.getMessage());
  }

  @Test
  public void testNullArgs() {
    assertThrows(
        NullPointerException.class, () -> new VectorRescoreQuery(null, fullPrecisionSource(), 5));
    assertThrows(
        NullPointerException.class, () -> new VectorRescoreQuery(knnQuery(10, null), null, 5));
  }

  @Test
  public void testEqualsAndHashCode() {
    VectorRescoreQuery query = new VectorRescoreQuery(knnQuery(10, null), fullPrecisionSource(), 5);
    VectorRescoreQuery same = new VectorRescoreQuery(knnQuery(10, null), fullPrecisionSource(), 5);
    assertEquals(query, same);
    assertEquals(query.hashCode(), same.hashCode());

    assertNotEquals(query, new VectorRescoreQuery(knnQuery(10, null), fullPrecisionSource(), 4));
    assertNotEquals(query, new VectorRescoreQuery(knnQuery(11, null), fullPrecisionSource(), 5));
    assertNotEquals(
        query,
        new VectorRescoreQuery(
            knnQuery(10, null), DoubleValuesSource.fromLongField(RANK_FIELD), 5));
  }

  @Test
  public void testGetters() {
    NrtKnnFloatVectorQuery knnQuery = knnQuery(10, null);
    VectorRescoreQuery query = new VectorRescoreQuery(knnQuery, fullPrecisionSource(), 5);
    assertEquals(knnQuery, query.getKnnQuery());
    assertEquals(5, query.getN());
  }

  @Test
  public void testToString() {
    NrtKnnFloatVectorQuery knnQuery = knnQuery(10, null);
    VectorRescoreQuery query = new VectorRescoreQuery(knnQuery, fullPrecisionSource(), 5);
    String queryString = query.toString(VECTOR_FIELD);
    assertTrue(queryString, queryString.startsWith("VectorRescoreQuery:"));
    assertTrue(queryString, queryString.contains(knnQuery.toString(VECTOR_FIELD)));
    assertTrue(queryString, queryString.contains("FullPrecisionFloatVectorSimilarityValuesSource"));
    assertTrue(queryString, queryString.endsWith("[5]"));
  }

  @Test
  public void testTotalHitsNotVectorQuery() {
    VectorRescoreQuery query =
        new VectorRescoreQuery(new MatchAllDocsQuery(), fullPrecisionSource(), 5);
    assertNull(query.getTotalHits());
  }

  @Test
  public void testTotalHits() throws IOException {
    VectorRescoreQuery query = new VectorRescoreQuery(knnQuery(10, null), fullPrecisionSource(), 5);
    assertNull(query.getTotalHits());
    query.rewrite(searcher);
    assertNotNull(query.getTotalHits());
    assertTrue(query.getTotalHits().value() > 0);
  }

  @Test
  public void testRescoreFullPrecision() throws IOException {
    VectorRescoreQuery query =
        new VectorRescoreQuery(knnQuery(NUM_DOCS, null), fullPrecisionSource(), 5);
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(5, topDocs.scoreDocs.length);

    TopDocs knnTopDocs = searcher.search(knnQuery(5, null), 10);
    for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
      ScoreDoc scoreDoc = topDocs.scoreDocs[i];
      assertEquals(knnTopDocs.scoreDocs[i].doc, scoreDoc.doc);
      float expectedScore =
          VectorSimilarityFunction.EUCLIDEAN.compare(QUERY_VECTOR, docVector(scoreDoc.doc));
      assertEquals(expectedScore, scoreDoc.score, 0.00001);
    }
  }

  @Test
  public void testRescoreReorders() throws IOException {
    // Rescore the top 10 vector hits by rank, keeping the top 3. The rank of each doc is its id.
    TopDocs knnTopDocs = searcher.search(knnQuery(10, null), 10);
    int[] knnDocIds = new int[knnTopDocs.scoreDocs.length];
    for (int i = 0; i < knnDocIds.length; ++i) {
      knnDocIds[i] = knnTopDocs.scoreDocs[i].doc;
    }
    Arrays.sort(knnDocIds);

    VectorRescoreQuery query =
        new VectorRescoreQuery(knnQuery(10, null), DoubleValuesSource.fromLongField(RANK_FIELD), 3);
    TopDocs topDocs = searcher.search(query, 10);
    assertEquals(3, topDocs.scoreDocs.length);
    for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
      int expectedDocId = knnDocIds[knnDocIds.length - 1 - i];
      assertEquals(expectedDocId, topDocs.scoreDocs[i].doc);
      assertEquals(expectedDocId, topDocs.scoreDocs[i].score, 0.0);
    }
  }

  @Test
  public void testRewriteFilterNoMatch() throws IOException {
    VectorRescoreQuery query =
        new VectorRescoreQuery(knnQuery(10, new MatchNoDocsQuery()), fullPrecisionSource(), 5);
    Query rewritten = query.rewrite(searcher);
    assertTrue(rewritten.toString(), rewritten instanceof MatchNoDocsQuery);
    assertEquals(0, searcher.search(query, 10).scoreDocs.length);
  }
}
