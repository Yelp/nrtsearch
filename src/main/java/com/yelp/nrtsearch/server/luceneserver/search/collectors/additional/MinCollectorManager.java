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
package com.yelp.nrtsearch.server.luceneserver.search.collectors.additional;

import com.google.protobuf.DoubleValue;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * Collector to find a min double value of a collection of documents. Currently, supports values
 * based on a {@link ScoreScript} definition.
 */
public class MinCollectorManager
    implements AdditionalCollectorManager<MinCollectorManager.MinCollector, CollectorResult> {
  static final double UNSET_VALUE = Double.MAX_VALUE;

  private final String name;
  private final ValueProvider valueProvider;

  /**
   * Constructor.
   *
   * @param name collector name
   * @param grpcMinCollector min collector definition message
   * @param context collector creation context
   */
  public MinCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.MinCollector grpcMinCollector,
      CollectorCreatorContext context) {
    this.name = name;

    switch (grpcMinCollector.getValueSourceCase()) {
      case SCRIPT:
        valueProvider =
            new ScriptValueProvider(
                grpcMinCollector.getScript(), context.getIndexState().docLookup, UNSET_VALUE);
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown value source: " + grpcMinCollector.getValueSourceCase());
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public MinCollector newCollector() throws IOException {
    return new MinCollector();
  }

  @Override
  public CollectorResult reduce(Collection<MinCollector> collectors) throws IOException {
    CollectorResult.Builder resultBuilder = CollectorResult.newBuilder();
    double minValue = UNSET_VALUE;
    for (MinCollector collector : collectors) {
      if (collector.minValue < minValue) {
        minValue = collector.minValue;
      }
    }
    resultBuilder.setDoubleResult(DoubleValue.newBuilder().setValue(minValue).build());

    return resultBuilder.build();
  }

  public class MinCollector implements Collector {
    double minValue = UNSET_VALUE;

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafContext) throws IOException {
      return new MinLeafCollector(leafContext);
    }

    @Override
    public ScoreMode scoreMode() {
      return valueProvider.needsScore() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    public class MinLeafCollector implements LeafCollector {
      final LeafValueProvider leafValueProvider;

      public MinLeafCollector(LeafReaderContext leafContext) throws IOException {
        leafValueProvider = valueProvider.getLeafValueProvider(leafContext);
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        leafValueProvider.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        double value = leafValueProvider.getValue(doc);
        if (value < minValue) {
          minValue = value;
        }
      }
    }
  }
}
