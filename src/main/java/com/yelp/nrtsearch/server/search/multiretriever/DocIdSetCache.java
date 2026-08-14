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
package com.yelp.nrtsearch.server.search.multiretriever;

import com.yelp.nrtsearch.server.search.SearcherResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.SparseFixedBitSet;

/**
 * Captures document IDs from multiple retriever searches into per-retriever, per-segment {@link
 * SparseFixedBitSet}s, then merges them (OR) into a union view for the aggregation pass.
 *
 * <p>Uses {@link SparseFixedBitSet} for collection. Each retriever tracks its own hit count and
 * stops recording once its per-retriever threshold is breached (plain int, no cross-thread
 * contention). After all retrievers complete, run {@link #merge} ORs the per-retriever bitsets
 * together into the union view.
 */
public class DocIdSetCache {

  private final int perRetrieverMaxHits;
  private final AtomicBoolean exceeded = new AtomicBoolean(false);
  private final List<Map<Integer, SparseFixedBitSet>> retrieverBitSets;
  private Map<Integer, BitSet> mergedBitSets;

  /**
   * @param perRetrieverMaxHits per-retriever threshold — if any single retriever collects more than
   *     this many docs, the cache is invalidated immediately during collection
   */
  public DocIdSetCache(int perRetrieverMaxHits) {
    this.perRetrieverMaxHits = perRetrieverMaxHits;
    this.retrieverBitSets = new ArrayList<>();
  }

  /**
   * Wraps the given collector manager with caching. Each call creates a new per-retriever bitset
   * collection and returns a wrapping manager that captures doc IDs during collection.
   */
  public synchronized CollectorManager<? extends Collector, SearcherResult> wrapWithCaching(
      CollectorManager<? extends Collector, SearcherResult> delegate) {
    return new DocIdSetCacheCollectorManager(delegate, newRetrieverBitSets());
  }

  /** Creates a new per-retriever bitset collection for direct use in tests. */
  synchronized RetrieverBitSets newRetrieverBitSets() {
    ConcurrentHashMap<Integer, SparseFixedBitSet> map = new ConcurrentHashMap<>();
    retrieverBitSets.add(map);
    return new RetrieverBitSets(map, perRetrieverMaxHits, exceeded);
  }

  /**
   * True once any retriever has tripped the threshold. Collectors check this to skip bitset writes.
   */
  public boolean isExceeded() {
    return exceeded.get();
  }

  /**
   * Merges all per-retriever bitsets via OR into the union view. Must be called after all
   * retrievers have completed (single-threaded). Idempotent — subsequent calls are no-ops.
   *
   * <p>If the exceeded flag was already set during collection, the merge is skipped (bitsets are
   * incomplete and must not be used).
   */
  public void merge() {
    if (mergedBitSets != null) {
      return;
    }
    if (exceeded.get()) {
      retrieverBitSets.clear();
      return;
    }

    Map<Integer, BitSet> tempMergedBitSets = new HashMap<>();

    for (Map<Integer, SparseFixedBitSet> retrieverMap : retrieverBitSets) {
      for (Map.Entry<Integer, SparseFixedBitSet> entry : retrieverMap.entrySet()) {
        int segmentOrd = entry.getKey();
        SparseFixedBitSet bits = entry.getValue();
        tempMergedBitSets.merge(
            segmentOrd,
            bits,
            (existing, incoming) -> {
              try {
                // Using iterator for OR as the underlying bitSet are not guaranteed to be
                // fixedBitSet
                // that supports bitwise operations
                existing.or(new BitSetIterator(incoming, incoming.approximateCardinality()));
              } catch (IOException e) {
                throw new RuntimeException("Failed to merge bitsets for segment " + segmentOrd, e);
              }
              return existing;
            });
      }
    }

    retrieverBitSets.clear();
    // Set the mergedBitSets only after the merge is complete.
    this.mergedBitSets = tempMergedBitSets;
  }

  /** Whether {@link #merge} has been called successfully. */
  public boolean isMerged() {
    return mergedBitSets != null;
  }

  /** Gets the merged bitset for a given segment. Only valid after {@link #merge}. */
  public BitSet getBitSet(int segmentOrd) {
    return mergedBitSets != null ? mergedBitSets.get(segmentOrd) : null;
  }

  /**
   * Per-retriever handle for recording hits. Each instance is owned by a single retriever.
   *
   * <p>The underlying map is a {@link ConcurrentHashMap} so that multiple search slices (each
   * handling disjoint segments) can safely create entries concurrently. Each {@link
   * SparseFixedBitSet} is only written by the single slice thread that owns that segment.
   *
   * <p>Tracks a per-retriever hit count (plain int, incremented only by the slice thread that owns
   * the segment). Once the count exceeds the threshold, sets the cache-wide exceeded flag and stops
   * recording. No atomic operations on the hot path.
   */
  public static class RetrieverBitSets {
    private final ConcurrentHashMap<Integer, SparseFixedBitSet> segmentBitSets;
    private final int maxHits;
    private final AtomicBoolean exceeded;
    private int hitCount;

    RetrieverBitSets(
        ConcurrentHashMap<Integer, SparseFixedBitSet> segmentBitSets,
        int maxHits,
        AtomicBoolean exceeded) {
      this.segmentBitSets = segmentBitSets;
      this.maxHits = maxHits;
      this.exceeded = exceeded;
    }

    /**
     * Gets or creates the bitset for the given segment.
     *
     * @param segmentOrd segment ordinal from LeafReaderContext.ord
     * @param maxDoc the segment's maxDoc (sizes the bitset)
     */
    public SparseFixedBitSet getOrCreateBitSet(int segmentOrd, int maxDoc) {
      return segmentBitSets.computeIfAbsent(segmentOrd, k -> new SparseFixedBitSet(maxDoc));
    }

    /**
     * Records a hit. Call from the collector after setting a bit. Once the per-retriever count
     * exceeds the threshold, sets the shared exceeded flag.
     */
    public void recordHit() {
      if (++hitCount > maxHits) {
        exceeded.set(true);
      }
    }

    /** Whether any retriever has exceeded the threshold. */
    public boolean isExceeded() {
      return exceeded.get();
    }
  }
}
