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
import com.yelp.nrtsearch.server.grpc.TermsCollector.TermsSourceCase;
import com.yelp.nrtsearch.server.luceneserver.script.FacetScript;
import com.yelp.nrtsearch.server.luceneserver.script.FacetScript.Factory;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.NestedCollectorManagers.NestedCollectors;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.NestedCollectorManagers.NestedCollectors.NestedLeafCollectors;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntMaps;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
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

/** Collector manager that aggregates terms from a {@link FacetScript} into buckets. */
public class ScriptTermsCollectorManager extends TermsCollectorManager {
  private final FacetScript.SegmentFactory scriptFactory;
  /**
   * Constructor.
   *
   * @param name Collection name from request
   * @param grpcTermsCollector Collector parameters from request
   * @param context context info for collector building
   * @param nestedCollectorSuppliers suppliers to create nested collector managers
   * @param bucketOrder ordering for results buckets
   */
  public ScriptTermsCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.TermsCollector grpcTermsCollector,
      CollectorCreatorContext context,
      Map<String, Supplier<AdditionalCollectorManager<? extends Collector, CollectorResult>>>
          nestedCollectorSuppliers,
      BucketOrder bucketOrder) {
    super(name, grpcTermsCollector.getSize(), nestedCollectorSuppliers, bucketOrder);

    if (grpcTermsCollector.getTermsSourceCase() == TermsSourceCase.SCRIPT) {
      Factory factory =
          ScriptService.getInstance().compile(grpcTermsCollector.getScript(), FacetScript.CONTEXT);
      scriptFactory =
          factory.newFactory(
              ScriptParamsUtils.decodeParams(grpcTermsCollector.getScript().getParamsMap()),
              context.getIndexState().docLookup);
    } else {
      throw new IllegalArgumentException(
          "ScriptTermsCollectorManager requires a script definition");
    }
  }

  @Override
  public TermsCollector newCollector() throws IOException {
    return new ScriptTermsCollector();
  }

  @Override
  public CollectorResult reduce(Collection<TermsCollectorManager.TermsCollector> collectors)
      throws IOException {
    Object2IntMap<Object> combinedCounts = combineCounts(collectors);
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
  private Object2IntMap<Object> combineCounts(Collection<TermsCollector> collectors) {
    if (collectors.isEmpty()) {
      return Object2IntMaps.emptyMap();
    }
    Iterator<TermsCollector> iterator = collectors.iterator();
    ScriptTermsCollector termsCollector = (ScriptTermsCollector) iterator.next();
    Object2IntOpenHashMap<Object> totalCountsMap = termsCollector.countsMap;
    while (iterator.hasNext()) {
      termsCollector = (ScriptTermsCollector) iterator.next();
      termsCollector
          .countsMap
          .object2IntEntrySet()
          .fastForEach(e -> totalCountsMap.addTo(e.getKey(), e.getIntValue()));
    }
    return totalCountsMap;
  }

  /** Collector implementation to record term counts generated by a {@link FacetScript}. */
  public class ScriptTermsCollector extends TermsCollector {

    Object2IntOpenHashMap<Object> countsMap = new Object2IntOpenHashMap<>();

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new TermsLeafCollector(context);
    }

    @Override
    public ScoreMode implementationScoreMode() {
      // Script cannot currently use score
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    /** Leaf Collector implementation to record term counts generated by a {@link FacetScript}. */
    public class TermsLeafCollector implements LeafCollector {
      final FacetScript facetScript;
      final NestedLeafCollectors nestedLeafCollectors;

      public TermsLeafCollector(LeafReaderContext leafContext) throws IOException {
        facetScript = scriptFactory.newInstance(leafContext);
        NestedCollectors nestedCollectors = getNestedCollectors();
        if (nestedCollectors != null) {
          nestedLeafCollectors = nestedCollectors.getLeafCollector(leafContext);
        } else {
          nestedLeafCollectors = null;
        }
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        // TODO make score available to script
        if (nestedLeafCollectors != null) {
          nestedLeafCollectors.setScorer(scorer);
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        facetScript.setDocId(doc);
        Object scriptResult = facetScript.execute();
        if (scriptResult != null) {
          processScriptResult(scriptResult, doc);
        }
      }

      private void processScriptResult(Object scriptResult, int doc) throws IOException {
        if (scriptResult instanceof Iterable) {
          ((Iterable<?>) scriptResult)
              .forEach(
                  v -> {
                    if (v != null) {
                      countsMap.addTo(v, 1);
                      if (nestedLeafCollectors != null) {
                        try {
                          nestedLeafCollectors.collect(v, doc);
                        } catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                      }
                    }
                  });
        } else {
          countsMap.addTo(scriptResult, 1);
          if (nestedLeafCollectors != null) {
            nestedLeafCollectors.collect(scriptResult, doc);
          }
        }
      }
    }
  }
}
