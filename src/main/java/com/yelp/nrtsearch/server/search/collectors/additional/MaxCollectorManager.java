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
package com.yelp.nrtsearch.server.search.collectors.additional;

import com.google.protobuf.DoubleValue;
import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.script.ScoreScript;
import com.yelp.nrtsearch.server.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.search.collectors.additional.MaxCollectorManager.MaxCollector;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * Collector to find a max double value of a collection of documents. Currently, supports values
 * based on a {@link ScoreScript} definition.
 */
public class MaxCollectorManager
    implements AdditionalCollectorManager<MaxCollector, CollectorResult> {
  static final double UNSET_VALUE = -Double.MAX_VALUE;

  private final String name;
  private final ValueProvider valueProvider;

  /**
   * Constructor.
   *
   * @param name collector name
   * @param grpcMaxCollector max collector definition message
   * @param context collector creation context
   */
  public MaxCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.MaxCollector grpcMaxCollector,
      CollectorCreatorContext context) {
    this.name = name;

    switch (grpcMaxCollector.getValueSourceCase()) {
      case SCRIPT:
        valueProvider =
            new ScriptValueProvider(
                grpcMaxCollector.getScript(), context.getIndexState().docLookup, UNSET_VALUE);
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown value source: " + grpcMaxCollector.getValueSourceCase());
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public MaxCollector newCollector() throws IOException {
    return new MaxCollector();
  }

  @Override
  public CollectorResult reduce(Collection<MaxCollector> collectors) throws IOException {
    CollectorResult.Builder resultBuilder = CollectorResult.newBuilder();
    double maxValue = UNSET_VALUE;
    for (MaxCollector collector : collectors) {
      if (collector.maxValue > maxValue) {
        maxValue = collector.maxValue;
      }
    }
    resultBuilder.setDoubleResult(DoubleValue.newBuilder().setValue(maxValue).build());

    return resultBuilder.build();
  }

  public class MaxCollector implements Collector {
    double maxValue = UNSET_VALUE;

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafContext) throws IOException {
      return new MaxLeafCollector(leafContext);
    }

    @Override
    public ScoreMode scoreMode() {
      return valueProvider.needsScore() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    public class MaxLeafCollector implements LeafCollector {
      final LeafValueProvider leafValueProvider;

      public MaxLeafCollector(LeafReaderContext leafContext) throws IOException {
        leafValueProvider = valueProvider.getLeafValueProvider(leafContext);
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        leafValueProvider.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        double value = leafValueProvider.getValue(doc);
        if (value > maxValue) {
          maxValue = value;
        }
      }
    }
  }
}
