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
package com.yelp.nrtsearch.server.search.multiretriever.blender.score;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.lucene.search.ScoreDoc;

/**
 * Abstract {@link ScoreDoc} that merges multiple retriever hits for the same document into a single
 * ranked entry. The {@link #score} field (inherited from {@link ScoreDoc}) is the live blended
 * score and must be kept up to date by each {@link #add} implementation. It drives result ordering
 * directly — no separate sorting-score indirection.
 *
 * <p>The original per-retriever hits are preserved in {@link #scoreDocs}, keyed by retriever name
 * in insertion order, for diagnostics and downstream inspection.
 *
 * <p>Subclasses define their own merging semantics by implementing {@link #add(String, int, float,
 * ScoreDoc)}, which is called for every retriever hit after the first. The first hit is supplied at
 * construction time.
 */
public abstract class BlendedScoreDoc extends ScoreDoc {

  /**
   * Per-retriever hits merged into this entry, keyed by retriever name in insertion order. The
   * first entry is always the hit supplied at construction time.
   */
  protected final LinkedHashMap<String, ScoreDoc> scoreDocs;

  /**
   * @param firstRetrieverName name of the first retriever contributing a hit
   * @param baseDoc the first retriever hit; its {@code doc} and {@code shardIndex} are inherited,
   *     and it is prepopulated into {@link #scoreDocs}
   * @param blendedScore initial blended score derived from {@code baseDoc} by the subclass
   */
  protected BlendedScoreDoc(String firstRetrieverName, ScoreDoc baseDoc, float blendedScore) {
    super(baseDoc.doc, blendedScore, baseDoc.shardIndex);
    this.scoreDocs = new LinkedHashMap<>();
    this.scoreDocs.put(firstRetrieverName, baseDoc);
  }

  /** Returns the per-retriever hits merged into this entry, keyed by retriever name. */
  public Map<String, ScoreDoc> getScoreDocs() {
    return scoreDocs;
  }

  /**
   * Merge a new retriever hit into this entry. Implementations must update {@link #score} to
   * reflect the new blended value and insert {@code scoreDoc} into {@link #scoreDocs} under {@code
   * retrieverName}.
   *
   * @param retrieverName name of the retriever contributing this hit
   * @param rank 1-based rank of the document in the new retriever's result list
   * @param weight per-retriever weight (e.g. boost)
   * @param scoreDoc raw hit from the new retriever
   */
  public abstract void add(String retrieverName, int rank, float weight, ScoreDoc scoreDoc);
}
