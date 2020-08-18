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
package com.yelp.nrtsearch.server.luceneserver.search.collectors;

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.MaxCollector;
import com.yelp.nrtsearch.server.grpc.MaxCollectorResult;
import com.yelp.nrtsearch.server.grpc.Selector;
import com.yelp.nrtsearch.server.grpc.SortType;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.properties.Sortable;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import java.io.IOException;
import java.util.Collection;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SortField;

/**
 * Collector for finding the maximum value of a given field. The field must be {@link Sortable}.
 * TODO: add handling for virtual fields
 */
public class MaxValueCollectorManager
    implements CustomCollectorManager<MaxValueCollectorManager.MaxValueCollector, CollectorResult> {

  private final String name;
  private final SortField sortField;

  public MaxValueCollectorManager(SearchContext context, String name, MaxCollector maxCollector) {
    this.name = name;
    FieldDef fieldDef = context.queryFields().get(maxCollector.getField());
    if (fieldDef == null) {
      throw new IllegalArgumentException("Field does not exist: " + maxCollector.getField());
    }

    if (!(fieldDef instanceof Sortable)) {
      throw new IllegalArgumentException("Field must be Sortable for max value collection");
    }
    sortField =
        ((Sortable) fieldDef)
            .getSortField(
                SortType.newBuilder()
                    .setFieldName(fieldDef.getName())
                    .setSelector(Selector.MAX)
                    .setMissingLat(true)
                    .setReverse(true)
                    .build());
  }

  @Override
  public MaxValueCollector newCollector() throws IOException {
    return new MaxValueCollector();
  }

  @Override
  public CollectorResult reduce(Collection<MaxValueCollector> collectors) throws IOException {
    Object globalMax = null;
    for (MaxValueCollector maxValueCollector : collectors) {
      Object collectorMax = maxValueCollector.fieldComparator.value(0);

      // Cast comparator to concrete Object type. This will be safe since the input values
      // will either be null or the result from another Collector.
      @SuppressWarnings("unchecked")
      int comp =
          ((FieldComparator<Object>) maxValueCollector.fieldComparator)
              .compareValues(globalMax, collectorMax);
      if (comp < 0) {
        globalMax = collectorMax;
      }
    }
    MaxCollectorResult.Builder maxResultBuilder = MaxCollectorResult.newBuilder();
    fillValue(globalMax, maxResultBuilder);
    return CollectorResult.newBuilder().setMax(maxResultBuilder).build();
  }

  private void fillValue(Object value, MaxCollectorResult.Builder result) {
    if (value == null) {
      // how should we handle a collection with no hits?
      return;
    }
    switch (sortField.getType()) {
      case INT:
        result.setIntValue(((Number) value).intValue());
        break;
      case LONG:
        result.setLongValue(((Number) value).longValue());
        break;
      case FLOAT:
        result.setFloatValue(((Number) value).floatValue());
        break;
      case DOUBLE:
        result.setDoubleValue(((Number) value).doubleValue());
        break;
      default:
        throw new IllegalArgumentException("Cannot get max with sort type: " + sortField.getType());
    }
  }

  @Override
  public String getName() {
    return name;
  }

  public class MaxValueCollector implements Collector {
    final FieldComparator<?> fieldComparator;

    private MaxValueCollector() {
      fieldComparator = sortField.getComparator(1, 0);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new MaxValueLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
      if (sortField.needsScores()) {
        return ScoreMode.COMPLETE;
      } else {
        return ScoreMode.COMPLETE_NO_SCORES;
      }
    }

    public class MaxValueLeafCollector implements LeafCollector {
      private final LeafFieldComparator leafFieldComparator;

      private MaxValueLeafCollector(LeafReaderContext context) throws IOException {
        leafFieldComparator = fieldComparator.getLeafComparator(context);
        leafFieldComparator.setBottom(0);
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        leafFieldComparator.setScorer(scorer);
      }

      @Override
      public void collect(int doc) throws IOException {
        if (leafFieldComparator.compareBottom(doc) < 0) {
          leafFieldComparator.copy(0, doc);
          leafFieldComparator.setBottom(0);
        }
      }
    }
  }
}
