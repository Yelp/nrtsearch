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
import com.yelp.nrtsearch.server.grpc.Script;
import com.yelp.nrtsearch.server.luceneserver.doc.DocLookup;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScoreScript.Factory;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.additional.MaxCollectorManager.MaxCollector;
import com.yelp.nrtsearch.server.luceneserver.search.query.QueryUtils.ScorableDoubleValues;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
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

  /** Interface for providing double values to aggregate. */
  interface ValueProvider {

    /**
     * Get leaf value provider for a given lucene segment.
     *
     * @param leafContext segment context
     * @return leaf value provider
     * @throws IOException
     */
    LeafValueProvider getLeafValueProvider(LeafReaderContext leafContext) throws IOException;

    /** Get if this provider uses the relevance score. */
    boolean needsScore();
  }

  /** Interface for providing a double value for a segment document. */
  interface LeafValueProvider {

    /**
     * Set scorer for current doc id.
     *
     * @param scorer scorer
     * @throws IOException
     */
    void setScorer(Scorable scorer) throws IOException;

    /**
     * Get a double value for the given doc id.
     *
     * @param doc doc id
     * @throws IOException
     */
    double getValue(int doc) throws IOException;
  }

  /** Value provider that uses a {@link ScoreScript}. */
  static class ScriptValueProvider implements ValueProvider {
    final DoubleValuesSource valuesSource;

    /**
     * Constructor.
     *
     * @param script script definition
     * @param lookup doc value lookup
     */
    ScriptValueProvider(Script script, DocLookup lookup) {
      Factory factory = ScriptService.getInstance().compile(script, ScoreScript.CONTEXT);
      valuesSource =
          factory.newFactory(ScriptParamsUtils.decodeParams(script.getParamsMap()), lookup);
    }

    @Override
    public LeafValueProvider getLeafValueProvider(LeafReaderContext leafContext)
        throws IOException {
      return new ScriptLeafValueProvider(leafContext);
    }

    @Override
    public boolean needsScore() {
      return valuesSource.needsScores();
    }

    /** Leaf value provider that uses a {@link ScoreScript}. */
    class ScriptLeafValueProvider implements LeafValueProvider {
      final DoubleValues values;
      final ScorableDoubleValues scores;

      /**
       * Constructor.
       *
       * @param leafContext segment context
       * @throws IOException
       */
      ScriptLeafValueProvider(LeafReaderContext leafContext) throws IOException {
        scores = new ScorableDoubleValues();
        values = valuesSource.getValues(leafContext, scores);
      }

      @Override
      public void setScorer(Scorable scorer) {
        scores.setScorer(scorer);
      }

      @Override
      public double getValue(int doc) throws IOException {
        if (values.advanceExact(doc)) {
          return values.doubleValue();
        } else {
          return UNSET_VALUE;
        }
      }
    }
  }

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
                grpcMaxCollector.getScript(), context.getIndexState().docLookup);
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
