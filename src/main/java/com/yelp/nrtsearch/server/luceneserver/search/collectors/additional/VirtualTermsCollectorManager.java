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

import com.yelp.nrtsearch.server.grpc.BucketResult;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import it.unimi.dsi.fastutil.doubles.Double2IntMap;
import it.unimi.dsi.fastutil.doubles.Double2IntMaps;
import it.unimi.dsi.fastutil.doubles.Double2IntOpenHashMap;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/** Collector manager that aggregates terms from {@link VirtualFieldDef} into buckets. */
public class VirtualTermsCollectorManager extends TermsCollectorManager {

  private final DoubleValuesSource valuesSource;

  /**
   * Constructor.
   *
   * @param name Collection name from request
   * @param grpcTermsCollector Collector parameters from request
   * @param context context info for collector building
   * @param virtualFieldDef field def
   */
  public VirtualTermsCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.TermsCollector grpcTermsCollector,
      CollectorCreatorContext context,
      VirtualFieldDef virtualFieldDef) {
    super(name, grpcTermsCollector.getSize());
    valuesSource = virtualFieldDef.getValuesSource();
  }

  @Override
  public TermsCollector newCollector() throws IOException {
    return new DoubleTermsCollector();
  }

  @Override
  public CollectorResult reduce(Collection<TermsCollector> collectors) throws IOException {
    Double2IntMap combinedCounts = combineCounts(collectors);
    BucketResult.Builder bucketBuilder = BucketResult.newBuilder();
    fillBucketResult(bucketBuilder, combinedCounts);

    return CollectorResult.newBuilder().setBucketResult(bucketBuilder.build()).build();
  }

  /** Combine term counts from each parallel collector into a single map */
  private Double2IntMap combineCounts(Collection<TermsCollector> collectors) {
    if (collectors.isEmpty()) {
      return Double2IntMaps.EMPTY_MAP;
    }
    Iterator<TermsCollector> iterator = collectors.iterator();
    DoubleTermsCollector termsCollector = (DoubleTermsCollector) iterator.next();
    Double2IntOpenHashMap totalCountsMap = termsCollector.countsMap;
    while (iterator.hasNext()) {
      termsCollector = (DoubleTermsCollector) iterator.next();
      termsCollector
          .countsMap
          .double2IntEntrySet()
          .fastForEach(e -> totalCountsMap.addTo(e.getDoubleKey(), e.getIntValue()));
    }
    return totalCountsMap;
  }

  /** Collector implementation to record term counts from a {@link DoubleValuesSource}. */
  public class DoubleTermsCollector extends TermsCollector {

    Double2IntOpenHashMap countsMap = new Double2IntOpenHashMap();

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new TermsLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
      if (valuesSource.needsScores()) {
        return ScoreMode.COMPLETE;
      } else {
        return ScoreMode.COMPLETE_NO_SCORES;
      }
    }

    /** Leaf collector implementation to record term counts from a {@link DoubleValuesSource}. */
    public class TermsLeafCollector implements LeafCollector {

      final ScoreValues scoreValues;
      final DoubleValues values;

      public TermsLeafCollector(LeafReaderContext leafContext) throws IOException {
        scoreValues = new ScoreValues();
        values = valuesSource.getValues(leafContext, scoreValues);
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        scoreValues.scorable = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        values.advanceExact(doc);
        countsMap.addTo(values.doubleValue(), 1);
      }
    }
  }

  /** Mutable value source to provide doc score based on a {@link Scorable}. */
  public static class ScoreValues extends DoubleValues {
    Scorable scorable;

    @Override
    public double doubleValue() throws IOException {
      return scorable.score();
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      return true;
    }
  }
}
