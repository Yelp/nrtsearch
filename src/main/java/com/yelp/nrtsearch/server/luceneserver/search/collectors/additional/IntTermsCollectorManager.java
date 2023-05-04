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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import com.yelp.nrtsearch.server.collectors.BucketOrder;
import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.NestedCollectorManagers.NestedCollectors;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.NestedCollectorManagers.NestedCollectors.NestedLeafCollectors;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMaps;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/** Collector manager that aggregates terms from Integer doc values into buckets. */
public class IntTermsCollectorManager extends TermsCollectorManager {
  private final IndexableFieldDef fieldDef;

  /**
   * Constructor.
   *
   * @param name Collection name from request
   * @param grpcTermsCollector Collector parameters from request
   * @param context context info for collector building
   * @param indexableFieldDef field def
   * @param nestedCollectorSuppliers suppliers to create nested collector managers
   * @param bucketOrder ordering for results buckets
   */
  public IntTermsCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.TermsCollector grpcTermsCollector,
      CollectorCreatorContext context,
      IndexableFieldDef indexableFieldDef,
      Map<String, Supplier<AdditionalCollectorManager<? extends Collector, CollectorResult>>>
          nestedCollectorSuppliers,
      BucketOrder bucketOrder) {
    super(name, grpcTermsCollector.getSize(), nestedCollectorSuppliers, bucketOrder);
    this.fieldDef = indexableFieldDef;
  }

  @Override
  public TermsCollector newCollector() throws IOException {
    return new IntTermsCollector();
  }

  @Override
  public CollectorResult reduce(Collection<TermsCollector> collectors) throws IOException {
    Int2IntMap combinedCounts = combineCounts(collectors);
    BucketResult.Builder bucketBuilder = BucketResult.newBuilder();
    Collection<NestedCollectors> nestedCollectors;
    if (hasNestedCollectors()) {
      nestedCollectors =
          collectors.stream().map(TermsCollector::getNestedCollectors).collect(Collectors.toList());
    } else {
      nestedCollectors = Collections.emptyList();
    }
    fillBucketResult(bucketBuilder, combinedCounts, nestedCollectors);
    return CollectorResult.newBuilder().setBucketResult(bucketBuilder.build()).build();
  }

  /** Combine term counts from each parallel collector into a single map */
  private Int2IntMap combineCounts(Collection<TermsCollector> collectors) {
    if (collectors.isEmpty()) {
      return Int2IntMaps.EMPTY_MAP;
    }
    Iterator<TermsCollector> iterator = collectors.iterator();
    IntTermsCollector termsCollector = (IntTermsCollector) iterator.next();
    Int2IntOpenHashMap totalCountsMap = termsCollector.countsMap;
    while (iterator.hasNext()) {
      termsCollector = (IntTermsCollector) iterator.next();
      termsCollector
          .countsMap
          .int2IntEntrySet()
          .fastForEach(e -> totalCountsMap.addTo(e.getIntKey(), e.getIntValue()));
    }
    return totalCountsMap;
  }

  /** Collector implementation to record term counts from Integer {@link LoadedDocValues}. */
  public class IntTermsCollector extends TermsCollector {

    Int2IntOpenHashMap countsMap = new Int2IntOpenHashMap();

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new TermsLeafCollector(context);
    }

    @Override
    public ScoreMode implementationScoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    /** Leaf Collector implementation to record term counts from Integer {@link LoadedDocValues}. */
    public class TermsLeafCollector implements LeafCollector {
      final LoadedDocValues<Integer> docValues;
      final NestedLeafCollectors nestedLeafCollectors;

      public TermsLeafCollector(LeafReaderContext leafContext) throws IOException {
        docValues = (LoadedDocValues<Integer>) fieldDef.getDocValues(leafContext);
        NestedCollectors nestedCollectors = getNestedCollectors();
        if (nestedCollectors != null) {
          nestedLeafCollectors = nestedCollectors.getLeafCollector(leafContext);
        } else {
          nestedLeafCollectors = null;
        }
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        if (nestedLeafCollectors != null) {
          nestedLeafCollectors.setScorer(scorer);
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        docValues.setDocId(doc);
        for (int i = 0; i < docValues.size(); ++i) {
          Integer value = docValues.get(i);
          countsMap.addTo(value, 1);
          if (nestedLeafCollectors != null) {
            nestedLeafCollectors.collect(value, doc);
          }
        }
      }
    }
  }
}
