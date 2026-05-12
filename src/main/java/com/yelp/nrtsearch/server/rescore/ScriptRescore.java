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

import java.io.IOException;
import java.util.Arrays;
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
 * (typically compiled from a {@link com.yelp.nrtsearch.server.script.ScoreScript}). The script
 * receives the previous-pass score via the {@code _score} expression variable and doc values via
 * {@code doc}.
 */
public class ScriptRescore implements RescoreOperation {

  private final DoubleValuesSource scriptSource;

  public ScriptRescore(DoubleValuesSource scriptSource) {
    this.scriptSource = scriptSource;
  }

  @Override
  public TopDocs rescore(TopDocs hits, RescoreContext context) throws IOException {
    IndexSearcher searcher = context.getSearchContext().getSearcherAndTaxonomy().searcher();

    // Sort hits by doc id so we can walk leaves in one forward pass.
    ScoreDoc[] scoreDocs = hits.scoreDocs.clone();
    Arrays.sort(scoreDocs, (a, b) -> Integer.compare(a.doc, b.doc));

    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();

    int leafIndex = 0;
    DoubleValues scriptValues = null;
    TopDocsScoreValues scoreValues = null;

    for (ScoreDoc scoreDoc : scoreDocs) {
      // Advance to the leaf that contains this global doc id.
      while (leafIndex < leaves.size() - 1 && leaves.get(leafIndex + 1).docBase <= scoreDoc.doc) {
        leafIndex++;
        scriptValues = null; // entering a new segment
      }

      if (scriptValues == null) {
        LeafReaderContext leaf = leaves.get(leafIndex);
        // TopDocsScoreValues exposes the previous-pass score as _score to the script.
        scoreValues = new TopDocsScoreValues(scoreDocs, leaf.docBase);
        scriptValues = scriptSource.getValues(leaf, scoreValues);
      }

      int segmentDocId = scoreDoc.doc - leaves.get(leafIndex).docBase;
      scriptValues.advanceExact(segmentDocId);
      scoreDoc.score = (float) scriptValues.doubleValue();
    }

    // Re-sort by new score descending.
    Arrays.sort(scoreDocs, (a, b) -> Float.compare(b.score, a.score));

    // Trim to windowSize, mirroring Lucene QueryRescorer behaviour.
    int windowSize = context.getWindowSize();
    if (windowSize < scoreDocs.length) {
      ScoreDoc[] trimmed = new ScoreDoc[windowSize];
      System.arraycopy(scoreDocs, 0, trimmed, 0, windowSize);
      scoreDocs = trimmed;
    }

    return new TopDocs(new TotalHits(hits.totalHits.value(), hits.totalHits.relation()), scoreDocs);
  }

  /**
   * Exposes the previous-pass score from a sorted {@link ScoreDoc} array as a {@link DoubleValues},
   * so the script engine can read it as the {@code _score} expression variable. Looks up the score
   * by converting the segment-local doc id back to a global doc id using {@code docBase}.
   */
  private static class TopDocsScoreValues extends DoubleValues {
    private final ScoreDoc[] scoreDocs;
    private final int docBase;
    private double score;

    TopDocsScoreValues(ScoreDoc[] scoreDocs, int docBase) {
      this.scoreDocs = scoreDocs;
      this.docBase = docBase;
    }

    @Override
    public double doubleValue() {
      return score;
    }

    @Override
    public boolean advanceExact(int segmentDocId) {
      int globalDocId = docBase + segmentDocId;
      for (ScoreDoc sd : scoreDocs) {
        if (sd.doc == globalDocId) {
          score = sd.score;
          return true;
        }
      }
      return false;
    }
  }
}
