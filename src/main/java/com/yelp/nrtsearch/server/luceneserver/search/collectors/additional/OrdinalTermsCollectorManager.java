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

import static org.apache.lucene.index.SortedSetDocValues.NO_MORE_ORDS;

import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.GlobalOrdinalable;
import com.yelp.nrtsearch.server.luceneserver.search.GlobalOrdinalLookup;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.NestedCollectorManagers.NestedCollectors;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.NestedCollectorManagers.NestedCollectors.NestedLeafCollectors;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2IntMaps;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.LongValues;

/** Collector manager that aggregates terms using global ordinals into buckets. */
public class OrdinalTermsCollectorManager extends TermsCollectorManager {

  private final IndexableFieldDef fieldDef;
  private final GlobalOrdinalLookup globalOrdinalLookup;

  /**
   * Constructor.
   *
   * @param name Collection name from request
   * @param grpcTermsCollector Collector parameters from request
   * @param context context info for collector building
   * @param indexableFieldDef field def
   * @param globalOrdinalable provider of global ordinal lookup
   * @param nestedCollectorSuppliers suppliers to create nested collector managers
   */
  public OrdinalTermsCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.TermsCollector grpcTermsCollector,
      CollectorCreatorContext context,
      IndexableFieldDef indexableFieldDef,
      GlobalOrdinalable globalOrdinalable,
      Map<String, Supplier<AdditionalCollectorManager<? extends Collector, CollectorResult>>>
          nestedCollectorSuppliers) {
    super(name, grpcTermsCollector.getSize(), nestedCollectorSuppliers);
    fieldDef = indexableFieldDef;
    try {
      globalOrdinalLookup =
          globalOrdinalable.getOrdinalLookup(
              context.getSearcherAndTaxonomy().searcher.getIndexReader());
    } catch (IOException e) {
      throw new RuntimeException("Error getting ordinal map");
    }
  }

  @Override
  public TermsCollector newCollector() throws IOException {
    return new OrdinalTermsCollector();
  }

  @Override
  public CollectorResult reduce(Collection<TermsCollector> collectors) throws IOException {
    Long2IntMap combinedCounts = combineCounts(collectors);
    BucketResult.Builder bucketBuilder = BucketResult.newBuilder();
    Collection<NestedCollectors> nestedCollectors;
    if (hasNestedCollectors()) {
      nestedCollectors =
          collectors.stream().map(TermsCollector::getNestedCollectors).collect(Collectors.toList());
    } else {
      nestedCollectors = Collections.emptyList();
    }
    fillBucketResult(bucketBuilder, combinedCounts, globalOrdinalLookup, nestedCollectors);

    return CollectorResult.newBuilder().setBucketResult(bucketBuilder.build()).build();
  }

  /** Combine term counts from each parallel collector into a single map */
  private Long2IntMap combineCounts(Collection<TermsCollector> collectors) {
    if (collectors.isEmpty()) {
      return Long2IntMaps.EMPTY_MAP;
    }
    Iterator<TermsCollector> iterator = collectors.iterator();
    OrdinalTermsCollector termsCollector = (OrdinalTermsCollector) iterator.next();
    Long2IntOpenHashMap totalCountsMap = termsCollector.countsMap;
    while (iterator.hasNext()) {
      termsCollector = (OrdinalTermsCollector) iterator.next();
      termsCollector
          .countsMap
          .long2IntEntrySet()
          .fastForEach(e -> totalCountsMap.addTo(e.getLongKey(), e.getIntValue()));
    }
    return totalCountsMap;
  }

  /** Collector implementation to record term counts based on global ordinals. */
  public class OrdinalTermsCollector extends TermsCollector {

    Long2IntOpenHashMap countsMap = new Long2IntOpenHashMap();

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      if (fieldDef.getDocValuesType() == DocValuesType.SORTED) {
        return new SortedLeafCollector(context);
      } else {
        return new SortedSetLeafCollector(context);
      }
    }

    @Override
    public ScoreMode implementationScoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    /**
     * Leaf collector implementation to record term counts based on global ordinals from {@link
     * SortedDocValues}.
     */
    public class SortedLeafCollector implements LeafCollector {
      final SortedDocValues docValues;
      final LongValues segmentOrdsMapping;
      final NestedLeafCollectors nestedLeafCollectors;

      public SortedLeafCollector(LeafReaderContext leafContext) throws IOException {
        docValues = DocValues.getSorted(leafContext.reader(), fieldDef.getName());
        segmentOrdsMapping = globalOrdinalLookup.getSegmentMapping(leafContext.ord);
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
        if (docValues.advanceExact(doc)) {
          long globalOrd = segmentOrdsMapping.get(docValues.ordValue());
          countsMap.addTo(globalOrd, 1);
          if (nestedLeafCollectors != null) {
            nestedLeafCollectors.collect(globalOrd, doc);
          }
        }
      }
    }

    /**
     * Leaf collector implementation to record term counts based on global ordinals from {@link
     * SortedSetDocValues}.
     */
    public class SortedSetLeafCollector implements LeafCollector {
      final SortedSetDocValues docValues;
      final LongValues segmentOrdsMapping;
      final NestedLeafCollectors nestedLeafCollectors;

      public SortedSetLeafCollector(LeafReaderContext leafContext) throws IOException {
        docValues = DocValues.getSortedSet(leafContext.reader(), fieldDef.getName());
        segmentOrdsMapping = globalOrdinalLookup.getSegmentMapping(leafContext.ord);
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
        if (docValues.advanceExact(doc)) {
          long ord = docValues.nextOrd();
          while (ord != NO_MORE_ORDS) {
            long globalOrd = segmentOrdsMapping.get(ord);
            countsMap.addTo(globalOrd, 1);
            if (nestedLeafCollectors != null) {
              nestedLeafCollectors.collect(globalOrd, doc);
            }
            ord = docValues.nextOrd();
          }
        }
      }
    }
  }
}
