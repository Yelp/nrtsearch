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
package com.yelp.nrtsearch.server.handler;

import static org.junit.Assert.*;

import com.yelp.nrtsearch.server.doc.DefaultSharedDocContext;
import com.yelp.nrtsearch.server.doc.SharedDocContext;
import com.yelp.nrtsearch.server.search.multiretriever.blender.score.BlendedScoreDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.junit.Test;

public class PopulateRetrieverScoresTest {

  /** Minimal BlendedScoreDoc subclass for testing. */
  private static BlendedScoreDoc blended(
      int docId, String retriever1, float score1, String retriever2, float score2) {
    BlendedScoreDoc b =
        new BlendedScoreDoc(retriever1, new ScoreDoc(docId, score1), score1) {
          @Override
          public void add(String retrieverName, int rank, float weight, ScoreDoc scoreDoc) {
            score = score + scoreDoc.score;
            scoreDocs.put(retrieverName, scoreDoc);
          }
        };
    b.add(retriever2, 1, 1.0f, new ScoreDoc(docId, score2));
    return b;
  }

  @Test
  public void testEmptyHitsNoWrites() {
    SharedDocContext ctx = new DefaultSharedDocContext();
    TopDocs hits = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]);
    SearchHandler.populateRetrieverScores(hits, ctx);
    // No documents were visited; nothing to assert, just verify no exception.
  }

  @Test
  public void testPlainScoreDocsNoWrites() {
    SharedDocContext ctx = new DefaultSharedDocContext();
    ScoreDoc[] docs = {new ScoreDoc(0, 3.0f), new ScoreDoc(1, 2.0f)};
    TopDocs hits = new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), docs);
    SearchHandler.populateRetrieverScores(hits, ctx);
    // Plain ScoreDocs carry no retriever data — context should be empty for those docs.
    assertTrue(ctx.getContext(0).isEmpty());
    assertTrue(ctx.getContext(1).isEmpty());
  }

  @Test
  public void testRetrieverScoresStoredWithPrefix() {
    SharedDocContext ctx = new DefaultSharedDocContext();
    BlendedScoreDoc hit = blended(5, "text", 1.5f, "knn", 0.8f);
    TopDocs hits = new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {hit});
    SearchHandler.populateRetrieverScores(hits, ctx);

    assertEquals(1.5, (Double) ctx.getContext(5).get("retriever_text"), 0.0001);
    assertEquals(0.8, (Double) ctx.getContext(5).get("retriever_knn"), 0.001);
    // Raw retriever name without prefix is not present.
    assertNull(ctx.getContext(5).get("text"));
  }

  @Test
  public void testMultipleDocsStoredUnderSameKey() {
    SharedDocContext ctx = new DefaultSharedDocContext();
    BlendedScoreDoc hit0 = blended(0, "text", 2.0f, "knn", 1.0f);
    BlendedScoreDoc hit1 = blended(1, "text", 3.0f, "knn", 0.5f);
    TopDocs hits =
        new TopDocs(new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {hit0, hit1});
    SearchHandler.populateRetrieverScores(hits, ctx);

    assertEquals(2.0, (Double) ctx.getContext(0).get("retriever_text"), 0.0001);
    assertEquals(3.0, (Double) ctx.getContext(1).get("retriever_text"), 0.0001);
    assertEquals(1.0, (Double) ctx.getContext(0).get("retriever_knn"), 0.001);
    assertEquals(0.5, (Double) ctx.getContext(1).get("retriever_knn"), 0.001);
  }

  @Test
  public void testMixedPlainAndBlendedHits() {
    SharedDocContext ctx = new DefaultSharedDocContext();
    BlendedScoreDoc blendedHit = blended(0, "r1", 5.0f, "r2", 3.0f);
    ScoreDoc plainHit = new ScoreDoc(1, 2.0f);
    TopDocs hits =
        new TopDocs(
            new TotalHits(2, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {blendedHit, plainHit});
    SearchHandler.populateRetrieverScores(hits, ctx);

    assertEquals(5.0, (Double) ctx.getContext(0).get("retriever_r1"), 0.0001);
    // Plain ScoreDoc at doc 1 should have no retriever entries.
    assertNull(ctx.getContext(1).get("retriever_r1"));
  }
}
