/*
 * Copyright 2023 Yelp Inc.
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

import com.yelp.nrtsearch.server.grpc.KnnQuery;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VectorFieldDef;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.search.AbstractKnnVectorQuery.DocAndScoreQuery;
import org.apache.lucene.search.IndexSearcher.LeafSlice;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;

/**
 * Collector modeled off the lucene {@link KnnFloatVectorQuery} that uses an {@link ExecutorService}
 * to do parallel collection by index slice. A numCandidates value may be provided that is greater
 * than k. This is the per segment documents considered for approximate vector search.
 */
public class NrtsearchKnnCollector {
  private static final TopDocs NO_RESULTS = TopDocsCollector.EMPTY_TOPDOCS;
  static final int NUM_CANDIDATES_LIMIT = 10000;
  private final String field;
  private final float[] queryVector;
  private final int k;
  private final int numCandidates;
  private final float boost;
  private final Weight filterWeight;
  private TopDocs result = null;

  /**
   * Constructor.
   *
   * @param grpcQuery vector query definition from request
   * @param indexState index state
   * @param searcher index searcher
   */
  public NrtsearchKnnCollector(KnnQuery grpcQuery, IndexState indexState, IndexSearcher searcher) {
    this.field = grpcQuery.getField();
    FieldDef fieldDef = indexState.getField(field);
    if (!(fieldDef instanceof VectorFieldDef)) {
      throw new IllegalArgumentException("Field is not a vector: " + field);
    }
    VectorFieldDef vectorFieldDef = (VectorFieldDef) fieldDef;
    if (!vectorFieldDef.isSearchable()) {
      throw new IllegalArgumentException("Vector field is not searchable: " + field);
    }

    if (grpcQuery.getQueryVectorCount() != vectorFieldDef.getVectorDimensions()) {
      throw new IllegalArgumentException(
          "Invalid query vector size, expected: "
              + vectorFieldDef.getVectorDimensions()
              + ", found: "
              + grpcQuery.getQueryVectorCount());
    }
    this.queryVector = new float[grpcQuery.getQueryVectorCount()];
    for (int i = 0; i < grpcQuery.getQueryVectorCount(); ++i) {
      this.queryVector[i] = grpcQuery.getQueryVector(i);
    }
    vectorFieldDef.validateVectorForSearch(queryVector);

    this.k = grpcQuery.getK();
    this.numCandidates = grpcQuery.getNumCandidates();
    if (k < 1) {
      throw new IllegalArgumentException("Vector search k must be >= 1");
    }
    if (numCandidates < k) {
      throw new IllegalArgumentException("Vector search numCandidates must be >= k");
    }
    if (numCandidates > NUM_CANDIDATES_LIMIT) {
      throw new IllegalArgumentException("Vector search numCandidates > " + NUM_CANDIDATES_LIMIT);
    }

    this.boost = grpcQuery.getBoost() > 0 ? grpcQuery.getBoost() : 1.0f;
    if (grpcQuery.hasFilter()) {
      this.filterWeight = createFilterWeight(grpcQuery.getFilter(), indexState, searcher);
    } else {
      this.filterWeight = null;
    }
  }

  private Weight createFilterWeight(Query query, IndexState indexState, IndexSearcher searcher) {
    org.apache.lucene.search.Query filterQuery =
        QueryNodeMapper.getInstance().getQuery(query, indexState);
    BooleanQuery booleanQuery =
        new BooleanQuery.Builder()
            .add(filterQuery, BooleanClause.Occur.FILTER)
            .add(new FieldExistsQuery(field), BooleanClause.Occur.FILTER)
            .build();
    try {
      org.apache.lucene.search.Query rewritten = searcher.rewrite(booleanQuery);
      return searcher.createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1f);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Get the top k nearest documents based on vector search.
   *
   * @param searcher index searcher
   * @param executor parallel search executor
   * @return vector search result as top docs
   */
  public TopDocs getResult(IndexSearcher searcher, ExecutorService executor) {
    if (result == null) {
      LeafSlice[] indexSlices = searcher.getSlices();
      List<Future<TopDocs>> futures = new ArrayList<>();
      for (LeafSlice slice : indexSlices) {
        futures.add(executor.submit(() -> processSlice(slice)));
      }
      TopDocs[] perSliceTopDocs = new TopDocs[futures.size()];
      for (int i = 0; i < futures.size(); ++i) {
        try {
          perSliceTopDocs[i] = futures.get(i).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new RuntimeException(e);
        }
      }
      result = applyBoost(TopDocs.merge(k, perSliceTopDocs));
    }
    return result;
  }

  /**
   * Get vector search results as a lucene Query. This Query recalls the top k documents, with the
   * scores being the boosted similarity scores.
   *
   * @param searcher index searcher
   * @param executor parallel search executor
   * @return vector search result as Query
   */
  public org.apache.lucene.search.Query getResultQuery(
      IndexSearcher searcher, ExecutorService executor) {
    IndexReader reader = searcher.getIndexReader();
    TopDocs topK = getResult(searcher, executor);
    int len = topK.scoreDocs.length;
    Arrays.sort(topK.scoreDocs, Comparator.comparingInt(a -> a.doc));
    int[] docs = new int[len];
    float[] scores = new float[len];
    for (int i = 0; i < len; i++) {
      docs[i] = topK.scoreDocs[i].doc;
      scores[i] = topK.scoreDocs[i].score;
    }
    int[] segmentStarts = findSegmentStarts(reader, docs);
    return new DocAndScoreQuery(k, docs, scores, segmentStarts, reader.getContext().id());
  }

  private int[] findSegmentStarts(IndexReader reader, int[] docs) {
    int[] starts = new int[reader.leaves().size() + 1];
    starts[starts.length - 1] = docs.length;
    if (starts.length == 2) {
      return starts;
    }
    int resultIndex = 0;
    for (int i = 1; i < starts.length - 1; i++) {
      int upper = reader.leaves().get(i).docBase;
      resultIndex = Arrays.binarySearch(docs, resultIndex, docs.length, upper);
      if (resultIndex < 0) {
        resultIndex = -1 - resultIndex;
      }
      starts[i] = resultIndex;
    }
    return starts;
  }

  private TopDocs applyBoost(TopDocs topDocs) {
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      scoreDoc.score *= boost;
    }
    return topDocs;
  }

  private TopDocs processSlice(LeafSlice slice) throws IOException {
    TopDocs[] perLeafTopDocs = new TopDocs[slice.leaves.length];

    for (int i = 0; i < slice.leaves.length; ++i) {
      LeafReaderContext ctx = slice.leaves[i];
      TopDocs results = searchLeaf(ctx, filterWeight);
      if (ctx.docBase > 0) {
        for (ScoreDoc scoreDoc : results.scoreDocs) {
          scoreDoc.doc += ctx.docBase;
        }
      }
      perLeafTopDocs[i] = results;
    }
    // Merge sort the results
    return TopDocs.merge(k, perLeafTopDocs);
  }

  protected TopDocs approximateSearch(LeafReaderContext context, Bits acceptDocs, int visitedLimit)
      throws IOException {
    TopDocs results =
        context
            .reader()
            .searchNearestVectors(field, queryVector, numCandidates, acceptDocs, visitedLimit);
    return results != null ? results : NO_RESULTS;
  }

  VectorScorer createVectorScorer(LeafReaderContext context, FieldInfo fi) throws IOException {
    if (fi.getVectorEncoding() != VectorEncoding.FLOAT32) {
      return null;
    }
    return VectorScorer.create(context, fi, queryVector);
  }

  private TopDocs searchLeaf(LeafReaderContext ctx, Weight filterWeight) throws IOException {
    Bits liveDocs = ctx.reader().getLiveDocs();
    int maxDoc = ctx.reader().maxDoc();

    if (filterWeight == null) {
      return approximateSearch(ctx, liveDocs, Integer.MAX_VALUE);
    }

    Scorer scorer = filterWeight.scorer(ctx);
    if (scorer == null) {
      return NO_RESULTS;
    }

    BitSet acceptDocs = createBitSet(scorer.iterator(), liveDocs, maxDoc);
    int cost = acceptDocs.cardinality();

    if (cost <= numCandidates) {
      // If there are <= k possible matches, short-circuit and perform exact search, since HNSW
      // must always visit at least k documents
      return exactSearch(ctx, new BitSetIterator(acceptDocs, cost));
    }

    // Perform the approximate kNN search
    TopDocs results = approximateSearch(ctx, acceptDocs, cost);
    if (results.totalHits.relation == TotalHits.Relation.EQUAL_TO) {
      return results;
    } else {
      // We stopped the kNN search because it visited too many nodes, so fall back to exact search
      return exactSearch(ctx, new BitSetIterator(acceptDocs, cost));
    }
  }

  private BitSet createBitSet(DocIdSetIterator iterator, Bits liveDocs, int maxDoc)
      throws IOException {
    if (liveDocs == null && iterator instanceof BitSetIterator) {
      // If we already have a BitSet and no deletions, reuse the BitSet
      return ((BitSetIterator) iterator).getBitSet();
    } else {
      // Create a new BitSet from matching and live docs
      FilteredDocIdSetIterator filterIterator =
          new FilteredDocIdSetIterator(iterator) {
            @Override
            protected boolean match(int doc) {
              return liveDocs == null || liveDocs.get(doc);
            }
          };
      return BitSet.of(filterIterator, maxDoc);
    }
  }

  // We allow this to be overridden so that tests can check what search strategy is used
  protected TopDocs exactSearch(LeafReaderContext context, DocIdSetIterator acceptIterator)
      throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null || fi.getVectorDimension() == 0) {
      // The field does not exist or does not index vectors
      return NO_RESULTS;
    }

    VectorScorer vectorScorer = createVectorScorer(context, fi);
    HitQueue queue = new HitQueue(k, true);
    ScoreDoc topDoc = queue.top();
    int doc;
    while ((doc = acceptIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      boolean advanced = vectorScorer.advanceExact(doc);
      assert advanced;

      float score = vectorScorer.score();
      if (score > topDoc.score) {
        topDoc.score = score;
        topDoc.doc = doc;
        topDoc = queue.updateTop();
      }
    }

    // Remove any remaining sentinel values
    while (queue.size() > 0 && queue.top().score < 0) {
      queue.pop();
    }

    ScoreDoc[] topScoreDocs = new ScoreDoc[queue.size()];
    for (int i = topScoreDocs.length - 1; i >= 0; i--) {
      topScoreDocs[i] = queue.pop();
    }

    TotalHits totalHits = new TotalHits(acceptIterator.cost(), TotalHits.Relation.EQUAL_TO);
    return new TopDocs(totalHits, topScoreDocs);
  }
}
