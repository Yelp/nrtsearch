/*
 * Copyright 2024 Yelp Inc.
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
import com.yelp.nrtsearch.server.search.collectors.additional.SumCollectorManager.SumCollector;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;

/**
 * Collector to find a sum of double values of a collection of documents. Currently, supports values
 * based on a {@link ScoreScript} definition.
 */
public class SumCollectorManager
    implements AdditionalCollectorManager<SumCollector, CollectorResult> {
  static final double INITIAL_VALUE = 0.0;

  private final String name;
  private final ValueProvider valueProvider;

  /**
   * Constructor.
   *
   * @param name collector name
   * @param grpcSumCollector sum collector definition message
   * @param context collector creation context
   */
  public SumCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.SumCollector grpcSumCollector,
      CollectorCreatorContext context) {
    this.name = name;

    if (grpcSumCollector.getValueSourceCase()
        == com.yelp.nrtsearch.server.grpc.SumCollector.ValueSourceCase.SCRIPT) {
      valueProvider =
          new ScriptValueProvider(
              grpcSumCollector.getScript(), context.getIndexState().docLookup, 0.0);
    } else {
      throw new IllegalArgumentException(
          "Unknown value source: " + grpcSumCollector.getValueSourceCase());
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public SumCollector newCollector() throws IOException {
    return new SumCollector();
  }

  @Override
  public CollectorResult reduce(Collection<SumCollector> collectors) throws IOException {
    CollectorResult.Builder resultBuilder = CollectorResult.newBuilder();
    double sumValue = INITIAL_VALUE;
    for (SumCollector collector : collectors) {
      sumValue += collector.sumValue;
    }
    resultBuilder.setDoubleResult(DoubleValue.newBuilder().setValue(sumValue).build());

    return resultBuilder.build();
  }

  public class SumCollector implements Collector {
    double sumValue = INITIAL_VALUE;

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext leafContext) throws IOException {
      return new SumLeafCollector(leafContext);
    }

    @Override
    public ScoreMode scoreMode() {
      return valueProvider.needsScore() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    public class SumLeafCollector implements LeafCollector {
      final LeafValueProvider leafValueProvider;

      public SumLeafCollector(LeafReaderContext leafContext) throws IOException {
        leafValueProvider = valueProvider.getLeafValueProvider(leafContext);
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        leafValueProvider.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        double value = leafValueProvider.getValue(doc);
        sumValue += value;
      }
    }
  }
}
