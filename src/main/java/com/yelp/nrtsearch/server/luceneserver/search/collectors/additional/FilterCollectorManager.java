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

import com.yelp.nrtsearch.server.grpc.CollectorResult;
import com.yelp.nrtsearch.server.grpc.FilterResult;
import com.yelp.nrtsearch.server.grpc.Query;
import com.yelp.nrtsearch.server.grpc.TermInSetQuery;
import com.yelp.nrtsearch.server.luceneserver.QueryNodeMapper;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.search.SearchContext;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.AdditionalCollectorManager;
import com.yelp.nrtsearch.server.luceneserver.search.collectors.CollectorCreatorContext;
import com.yelp.nrtsearch.server.luceneserver.search.query.QueryUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;

public class FilterCollectorManager
    implements AdditionalCollectorManager<FilterCollectorManager.FilterCollector, CollectorResult> {
  private final String name;
  private final Map<String, AdditionalCollectorManager<Collector, CollectorResult>>
      nestedCollectorManagers;
  final Filter filter;

  interface Filter {
    LeafFilter getLeafFilter(LeafReaderContext context) throws IOException;
  }

  interface LeafFilter {
    boolean accepts(int docId) throws IOException;
  }

  private static class QueryFilter implements Filter {
    final Weight filterWeight;

    QueryFilter(Query grpcQuery, CollectorCreatorContext context) {
      org.apache.lucene.search.Query query =
          QueryNodeMapper.getInstance().getQuery(grpcQuery, context.getIndexState());
      try {
        org.apache.lucene.search.Query rewritten =
            context.getSearcherAndTaxonomy().searcher.rewrite(query);
        filterWeight =
            context
                .getSearcherAndTaxonomy()
                .searcher
                .createWeight(rewritten, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
      } catch (IOException e) {
        throw new RuntimeException("Error creating filter query weight", e);
      }
    }

    @Override
    public LeafFilter getLeafFilter(LeafReaderContext context) throws IOException {
      return new QueryLeafFilter(context);
    }

    private class QueryLeafFilter implements LeafFilter {
      final Bits filterDocSet;

      QueryLeafFilter(LeafReaderContext context) throws IOException {
        filterDocSet =
            QueryUtils.asSequentialAccessBits(
                context.reader().maxDoc(), filterWeight.scorerSupplier(context));
      }

      @Override
      public boolean accepts(int docId) throws IOException {
        return filterDocSet.get(docId);
      }
    }
  }

  private static class SetQueryFilter implements Filter {
    final IndexableFieldDef filterField;
    final Set<Object> filterSet = new HashSet<>();

    SetQueryFilter(TermInSetQuery grpcTermInSetQuery, CollectorCreatorContext context) {
      FieldDef fieldDef = context.getQueryFields().get(grpcTermInSetQuery.getField());
      if (fieldDef == null) {
        throw new IllegalArgumentException(
            "Unknown filter field: " + grpcTermInSetQuery.getField());
      }
      if (!(fieldDef instanceof IndexableFieldDef)) {
        throw new IllegalArgumentException(
            "Filter field is not indexable: " + grpcTermInSetQuery.getField());
      }
      filterField = (IndexableFieldDef) fieldDef;
      if (!filterField.hasDocValues()) {
        throw new IllegalArgumentException(
            "Filter field must have doc values enabled: " + grpcTermInSetQuery.getField());
      }

      switch (grpcTermInSetQuery.getTermTypesCase()) {
        case INTTERMS:
          filterSet.addAll(grpcTermInSetQuery.getIntTerms().getTermsList());
          break;
        case LONGTERMS:
          filterSet.addAll(grpcTermInSetQuery.getLongTerms().getTermsList());
          break;
        case FLOATTERMS:
          filterSet.addAll(grpcTermInSetQuery.getFloatTerms().getTermsList());
          break;
        case DOUBLETERMS:
          filterSet.addAll(grpcTermInSetQuery.getDoubleTerms().getTermsList());
          break;
        case TEXTTERMS:
          filterSet.addAll(grpcTermInSetQuery.getTextTerms().getTermsList());
          break;
        default:
          throw new IllegalArgumentException(
              "Unknown filter term type: " + grpcTermInSetQuery.getTermTypesCase());
      }
    }

    @Override
    public LeafFilter getLeafFilter(LeafReaderContext context) throws IOException {
      return new SetQueryLeafFilter(context);
    }

    private class SetQueryLeafFilter implements LeafFilter {
      final LoadedDocValues<?> filterDocValues;

      SetQueryLeafFilter(LeafReaderContext context) throws IOException {
        filterDocValues = filterField.getDocValues(context);
      }

      @Override
      public boolean accepts(int docId) throws IOException {
        filterDocValues.setDocId(docId);
        for (int i = 0; i < filterDocValues.size(); ++i) {
          if (filterSet.contains(filterDocValues.get(i))) {
            return true;
          }
        }
        return false;
      }
    }
  }

  public FilterCollectorManager(
      String name,
      com.yelp.nrtsearch.server.grpc.FilterCollector grpcFilterCollector,
      CollectorCreatorContext context,
      Map<String, Supplier<AdditionalCollectorManager<? extends Collector, CollectorResult>>>
          nestedCollectorSuppliers) {
    this.name = name;
    if (nestedCollectorSuppliers.isEmpty()) {
      throw new IllegalArgumentException(
          "Filter collector \"" + name + "\" must have nested collectors");
    }
    nestedCollectorManagers =
        nestedCollectorSuppliers.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Entry::getKey,
                    e ->
                        (AdditionalCollectorManager<Collector, CollectorResult>)
                            e.getValue().get()));

    switch (grpcFilterCollector.getFilterCase()) {
      case QUERY:
        filter = new QueryFilter(grpcFilterCollector.getQuery(), context);
        break;
      case SETQUERY:
        filter = new SetQueryFilter(grpcFilterCollector.getSetQuery(), context);
        break;
      default:
        throw new IllegalArgumentException(
            "Unknown filter type: " + grpcFilterCollector.getFilterCase());
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setSearchContext(SearchContext searchContext) {
    nestedCollectorManagers.forEach((k, v) -> v.setSearchContext(searchContext));
  }

  @Override
  public FilterCollector newCollector() throws IOException {
    return new FilterCollector();
  }

  @Override
  public CollectorResult reduce(Collection<FilterCollector> collectors) throws IOException {
    CollectorResult.Builder resultBuilder = CollectorResult.newBuilder();
    FilterResult.Builder filterResultBuilder = FilterResult.newBuilder();

    int totalDocCount = 0;
    for (FilterCollector filterCollector : collectors) {
      totalDocCount += filterCollector.docCount;
    }

    List<Collector> nestedCollectors = new ArrayList<>(collectors.size());
    for (Map.Entry<String, AdditionalCollectorManager<Collector, CollectorResult>> entry :
        nestedCollectorManagers.entrySet()) {
      nestedCollectors.clear();
      for (FilterCollector filterCollector : collectors) {
        nestedCollectors.add(filterCollector.nestedCollectors.get(entry.getKey()));
      }
      filterResultBuilder.putNestedCollectorResults(
          entry.getKey(), entry.getValue().reduce(nestedCollectors));
    }

    filterResultBuilder.setDocCount(totalDocCount);
    resultBuilder.setFilterResult(filterResultBuilder);
    return resultBuilder.build();
  }

  public class FilterCollector implements Collector {
    final Map<String, Collector> nestedCollectors;
    int docCount;

    public FilterCollector() throws IOException {
      nestedCollectors = new HashMap<>();
      for (Map.Entry<String, AdditionalCollectorManager<Collector, CollectorResult>> entry :
          nestedCollectorManagers.entrySet()) {
        nestedCollectors.put(entry.getKey(), entry.getValue().newCollector());
      }
      docCount = 0;
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
      return new FilterLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
      for (Map.Entry<String, Collector> entry : nestedCollectors.entrySet()) {
        if (entry.getValue().scoreMode() != ScoreMode.COMPLETE_NO_SCORES) {
          return ScoreMode.COMPLETE;
        }
      }
      return ScoreMode.COMPLETE_NO_SCORES;
    }

    public class FilterLeafCollector implements LeafCollector {
      final List<LeafCollector> nestedLeafCollectors;
      final LeafFilter leafFilter;

      public FilterLeafCollector(LeafReaderContext context) throws IOException {
        nestedLeafCollectors = new ArrayList<>(nestedCollectors.size());
        for (Map.Entry<String, Collector> entry : nestedCollectors.entrySet()) {
          nestedLeafCollectors.add(entry.getValue().getLeafCollector(context));
        }
        leafFilter = filter.getLeafFilter(context);
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        for (LeafCollector leafCollector : nestedLeafCollectors) {
          leafCollector.setScorer(scorer);
        }
      }

      @Override
      public void collect(int doc) throws IOException {
        if (!leafFilter.accepts(doc)) {
          return;
        }
        docCount++;
        for (LeafCollector leafCollector : nestedLeafCollectors) {
          leafCollector.collect(doc);
        }
      }
    }
  }
}
