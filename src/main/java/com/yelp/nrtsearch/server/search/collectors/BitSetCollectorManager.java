/*
 * Copyright 2025 Yelp Inc.
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
package com.yelp.nrtsearch.server.search.collectors;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.util.FixedBitSet;

/** CollectorManager for creating BitSet collectors that can run in parallel */
public class BitSetCollectorManager
    implements CollectorManager<BitSetCollectorManager.BitSetCollector, FixedBitSet> {

  private final int maxDoc;
  private final String collectorName;

  public BitSetCollectorManager(int maxDoc, String collectorName) {
    this.maxDoc = maxDoc;
    this.collectorName = collectorName;
  }

  public BitSetCollectorManager(int maxDoc) {
    this(maxDoc, "BitSetCollector");
  }

  @Override
  public BitSetCollector newCollector() throws IOException {
    return new BitSetCollector(maxDoc, collectorName);
  }

  @Override
  public FixedBitSet reduce(Collection<BitSetCollector> collectors) throws IOException {
    FixedBitSet result = new FixedBitSet(maxDoc);

    // Union all BitSets from parallel collectors
    for (BitSetCollector collector : collectors) {
      result.or(collector.getBitSet());
    }

    return result;
  }

  /** Individual collector that creates a BitSet of matching documents */
  public static class BitSetCollector extends org.apache.lucene.search.SimpleCollector {
    private final FixedBitSet bitSet;
    private final AtomicInteger totalMatches;
    private final String collectorName;
    private int docBase;

    public BitSetCollector(int maxDoc, String collectorName) {
      this.bitSet = new FixedBitSet(maxDoc);
      this.totalMatches = new AtomicInteger(0);
      this.collectorName = collectorName;
    }

    @Override
    protected void doSetNextReader(org.apache.lucene.index.LeafReaderContext context)
        throws IOException {
      this.docBase = context.docBase;
    }

    @Override
    public void collect(int doc) throws IOException {
      int globalDoc = docBase + doc;
      bitSet.set(globalDoc);
      totalMatches.incrementAndGet();
    }

    @Override
    public org.apache.lucene.search.ScoreMode scoreMode() {
      return org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
    }

    public FixedBitSet getBitSet() {
      return bitSet;
    }

    public int getTotalMatches() {
      return totalMatches.get();
    }

    public String getCollectorName() {
      return collectorName;
    }
  }
}
