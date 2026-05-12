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

import com.yelp.nrtsearch.server.query.QueryUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * {@link RescoreOperation} that re-scores each hit by evaluating a {@link DoubleValuesSource}
 * (typically compiled from a {@link com.yelp.nrtsearch.server.script.ScoreScript}). The
 * previous-pass score is passed as the {@link DoubleValues} {@code scores} argument to {@link
 * DoubleValuesSource#getValues}. How that score is surfaced to the script author depends on the
 * script engine:
 *
 * <ul>
 *   <li><b>JS (Lucene expression)</b> – accessible as the {@code _score} variable, resolved via
 *       {@link com.yelp.nrtsearch.server.field.FieldDefBindings} to {@link
 *       DoubleValuesSource#SCORES}.
 *   <li><b>Custom {@link com.yelp.nrtsearch.server.script.ScoreScript} subclasses</b> – accessible
 *       via {@link com.yelp.nrtsearch.server.script.ScoreScript#get_score()}.
 * </ul>
 */
public class ScriptRescore implements RescoreOperation {

  // Ascending by doc id then shard index — used to walk segment leaves in one forward pass.
  // Mirrors the ordering of TopDocs' private BY_DOC_ID / DEFAULT_TIE_BREAKER.
  private static final Comparator<ScoreDoc> BY_DOC_ID =
      Comparator.comparingInt((ScoreDoc d) -> d.doc).thenComparingInt(d -> d.shardIndex);
  // Descending by score; ties broken by doc id then shard index (consistent with TopDocs.merge).
  private static final Comparator<ScoreDoc> BY_SCORE_DESC =
      Comparator.<ScoreDoc>comparingDouble(d -> -d.score).thenComparing(BY_DOC_ID);

  private final DoubleValuesSource scriptSource;

  public ScriptRescore(DoubleValuesSource scriptSource) {
    this.scriptSource = scriptSource;
  }

  @Override
  public TopDocs rescore(TopDocs hits, RescoreContext context) throws IOException {
    IndexSearcher searcher = context.getSearchContext().getSearcherAndTaxonomy().searcher();

    // Sort hits by doc id so we can walk leaves in one forward pass.
    ScoreDoc[] scoreDocs = hits.scoreDocs.clone();
    Arrays.sort(scoreDocs, BY_DOC_ID);

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    int leafIndex = 0;
    DoubleValues scriptValues = null;
    QueryUtils.SettableDoubleValues scoreValues = new QueryUtils.SettableDoubleValues();

    for (ScoreDoc scoreDoc : scoreDocs) {
      // Advance to the leaf that contains this global doc id.
      while (leafIndex < leaves.size() - 1 && leaves.get(leafIndex + 1).docBase <= scoreDoc.doc) {
        leafIndex++;
        scriptValues = null; // entering a new segment
      }

      if (scriptValues == null) {
        LeafReaderContext leaf = leaves.get(leafIndex);
        scriptValues = scriptSource.getValues(leaf, scoreValues);
      }

      scoreValues.setValue(scoreDoc.score);
      int segmentDocId = scoreDoc.doc - leaves.get(leafIndex).docBase;
      scriptValues.advanceExact(segmentDocId);
      scoreDoc.score = (float) scriptValues.doubleValue();
    }

    // Re-sort by new score descending.
    Arrays.sort(scoreDocs, BY_SCORE_DESC);

    // Trim to windowSize, mirroring Lucene QueryRescorer behaviour.
    int windowSize = context.getWindowSize();
    if (windowSize < scoreDocs.length) {
      ScoreDoc[] trimmed = new ScoreDoc[windowSize];
      System.arraycopy(scoreDocs, 0, trimmed, 0, windowSize);
      scoreDocs = trimmed;
    }

    return new TopDocs(new TotalHits(hits.totalHits.value(), hits.totalHits.relation()), scoreDocs);
  }
}
