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

import com.yelp.nrtsearch.server.doc.DocLookup;
import com.yelp.nrtsearch.server.query.QueryUtils;
import com.yelp.nrtsearch.server.script.ScoreScript;
import com.yelp.nrtsearch.server.script.ScriptFactoryContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * {@link RescoreOperation} that re-scores each hit by evaluating a compiled script. At rescore
 * time, the factory is called with the current {@link
 * com.yelp.nrtsearch.server.doc.SharedDocContext} from the search context, allowing the script to
 * read per-retriever scores (and any other values contributed by earlier pipeline steps) in
 * addition to the standard inputs.
 *
 * <p>Inputs available to scripts:
 *
 * <ul>
 *   <li><b>{@code _score}</b> — the previous-pass blended score (JS: {@code _score} variable;
 *       {@link com.yelp.nrtsearch.server.script.ScoreScript} subclasses: {@code get_score()}).
 *   <li><b>Doc values</b> — index field values via {@code doc['field'].value}.
 *   <li><b>Shared doc context values</b> — per-document values contributed by earlier pipeline
 *       steps. For multi-retriever queries, each retriever's raw score is available under the key
 *       {@code retriever_<name>} (JS: {@code _shared_retriever_<name>}; {@link
 *       com.yelp.nrtsearch.server.script.ScoreScript} subclasses: {@code
 *       get_shared_double("retriever_<name>", defaultValue)}).
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

  private final ScoreScript.Factory factory;
  private final Map<String, Object> params;
  private final DocLookup docLookup;

  public ScriptRescore(
      ScoreScript.Factory factory, Map<String, Object> params, DocLookup docLookup) {
    this.factory = factory;
    this.params = params;
    this.docLookup = docLookup;
  }

  @Override
  public TopDocs rescore(TopDocs hits, RescoreContext context) throws IOException {
    ScriptFactoryContext factoryContext =
        ScriptFactoryContext.builder(params, docLookup)
            .sharedDocContext(context.getSearchContext().getSharedDocContext())
            .build();
    DoubleValuesSource scriptSource = factory.newFactory(factoryContext);

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
