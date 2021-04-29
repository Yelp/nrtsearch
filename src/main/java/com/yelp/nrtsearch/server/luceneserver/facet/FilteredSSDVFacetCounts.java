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
package com.yelp.nrtsearch.server.luceneserver.facet;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;

/**
 * Facet implementation based off the {@link
 * org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts}. Computes facet counts based on
 * the sorted set doc value ordinals for a provided list of values, acting as an inclusion filter.
 * Only applies to a single dimension.
 */
public class FilteredSSDVFacetCounts extends Facets {

  final SortedSetDocValuesReaderState state;
  final SortedSetDocValues dv;
  final String field;
  final Map<Long, Integer> globalOrdinalToCountIndex;
  final List<String> values;
  final int[] counts;

  /**
   * Facet to count based on sorted set doc values, but only considering the provided values.
   *
   * @param values values to count
   * @param dim facet dimension
   * @param state reader state
   * @param hits hits to facet over
   * @throws IOException
   */
  public FilteredSSDVFacetCounts(
      List<String> values, String dim, SortedSetDocValuesReaderState state, FacetsCollector hits)
      throws IOException {
    this.state = state;
    this.field = state.getField();
    this.values = values;
    dv = state.getDocValues();
    counts = new int[values.size()];

    // find mapping to go from global ordinal to the value count index
    globalOrdinalToCountIndex = new HashMap<>();
    for (int i = 0; i < values.size(); ++i) {
      String[] fullPath = new String[2];
      fullPath[0] = dim;
      fullPath[1] = values.get(i);
      long gOrd = dv.lookupTerm(new BytesRef(FacetsConfig.pathToString(fullPath)));
      if (gOrd >= 0) {
        globalOrdinalToCountIndex.put(gOrd, i);
      }
    }

    count(hits.getMatchingDocs());
  }

  /** Does all the "real work" of tallying up the counts. */
  private void count(List<MatchingDocs> matchingDocs) throws IOException {
    OrdinalMap ordinalMap;

    // TODO: is this right?  really, we need a way to
    // verify that this ordinalMap "matches" the leaves in
    // matchingDocs...
    if (dv instanceof MultiDocValues.MultiSortedSetDocValues && matchingDocs.size() > 1) {
      ordinalMap = ((MultiSortedSetDocValues) dv).mapping;
    } else {
      ordinalMap = null;
    }
    IndexReader reader = state.getReader();

    for (MatchingDocs hits : matchingDocs) {
      // LUCENE-5090: make sure the provided reader context "matches"
      // the top-level reader passed to the
      // SortedSetDocValuesReaderState, else cryptic
      // AIOOBE can happen:
      if (ReaderUtil.getTopLevelContext(hits.context).reader() != reader) {
        throw new IllegalStateException(
            "the SortedSetDocValuesReaderState provided to this class does not match the reader being searched; you must create a new SortedSetDocValuesReaderState every time you open a new IndexReader");
      }

      countOneSegment(ordinalMap, hits.context.reader(), hits.context.ord, hits);
    }
  }

  private void countOneSegment(
      OrdinalMap ordinalMap, LeafReader reader, int segOrd, MatchingDocs hits) throws IOException {
    SortedSetDocValues segValues = reader.getSortedSetDocValues(field);
    if (segValues == null) {
      // nothing to count
      return;
    }

    DocIdSetIterator it;
    if (hits == null) {
      it = segValues;
    } else {
      it = ConjunctionDISI.intersectIterators(Arrays.asList(hits.bits.iterator(), segValues));
    }

    // TODO: yet another option is to count all segs
    // first, only in seg-ord space, and then do a
    // merge-sort-PQ in the end to only "resolve to
    // global" those seg ords that can compete, if we know
    // we just want top K?  ie, this is the same algo
    // that'd be used for merging facets across shards
    // (distributed faceting).  but this has much higher
    // temp ram req'ts (sum of number of ords across all
    // segs)
    if (ordinalMap != null) {
      final LongValues ordMap = ordinalMap.getGlobalOrds(segOrd);

      int numSegOrds = (int) segValues.getValueCount();

      if (hits != null && hits.totalHits < numSegOrds / 10) {
        // Remap every ord to global ord as we iterate:
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          int term = (int) segValues.nextOrd();
          while (term != SortedSetDocValues.NO_MORE_ORDS) {
            Integer countIndex = globalOrdinalToCountIndex.get(ordMap.get(term));
            if (countIndex != null) {
              counts[countIndex]++;
            }
            term = (int) segValues.nextOrd();
          }
        }
      } else {
        // First count in seg-ord space:
        final int[] segCounts = new int[numSegOrds];
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          int term = (int) segValues.nextOrd();
          while (term != SortedSetDocValues.NO_MORE_ORDS) {
            segCounts[term]++;
            term = (int) segValues.nextOrd();
          }
        }

        // Then, migrate to global ords:
        for (int ord = 0; ord < numSegOrds; ord++) {
          int count = segCounts[ord];
          if (count != 0) {
            Integer countIndex = globalOrdinalToCountIndex.get(ordMap.get(ord));
            if (countIndex != null) {
              counts[countIndex]++;
            }
          }
        }
      }
    } else {
      // No ord mapping (e.g., single segment index):
      // just aggregate directly into counts:
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
        long term = segValues.nextOrd();
        while (term != SortedSetDocValues.NO_MORE_ORDS) {
          Integer countIndex = globalOrdinalToCountIndex.get(term);
          if (countIndex != null) {
            counts[countIndex]++;
          }
          term = segValues.nextOrd();
        }
      }
    }
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    if (topN <= 0) {
      throw new IllegalArgumentException("topN must be > 0 (got: " + topN + ")");
    }
    if (path.length > 0) {
      throw new IllegalArgumentException("path should be 0 length");
    }
    return getDim(dim, topN);
  }

  private FacetResult getDim(String dim, int topN) throws IOException {
    TopOrdAndIntQueue q = null;

    int bottomCount = 0;
    int dimCount = 0;
    int childCount = 0;

    TopOrdAndIntQueue.OrdAndValue reuse = null;
    for (int ord = 0; ord < counts.length; ord++) {
      if (counts[ord] > 0) {
        dimCount += counts[ord];
        childCount++;
        if (counts[ord] > bottomCount) {
          if (reuse == null) {
            reuse = new TopOrdAndIntQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = counts[ord];
          if (q == null) {
            // Lazy init, so we don't create this for the
            // sparse case unnecessarily
            q = new TopOrdAndIntQueue(topN);
          }
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomCount = q.top().value;
          }
        }
      }
    }

    if (q == null) {
      return null;
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for (int i = labelValues.length - 1; i >= 0; i--) {
      TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
      labelValues[i] = new LabelAndValue(values.get(ordAndValue.ord), ordAndValue.value);
    }
    return new FacetResult(dim, new String[0], dimCount, labelValues, childCount);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    if (path.length != 1) {
      throw new IllegalArgumentException("path must be length=1");
    }
    long ord = dv.lookupTerm(new BytesRef(FacetsConfig.pathToString(dim, path)));
    if (ord < 0) {
      return -1;
    }

    Integer countIndex = globalOrdinalToCountIndex.get(ord);
    if (countIndex == null) {
      return -1;
    }

    return counts[countIndex];
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    throw new UnsupportedOperationException();
  }
}
