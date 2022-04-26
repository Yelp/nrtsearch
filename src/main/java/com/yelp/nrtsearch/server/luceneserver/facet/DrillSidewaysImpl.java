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
package com.yelp.nrtsearch.server.luceneserver.facet;

import com.google.protobuf.ProtocolStringList;
import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.NumericRangeType;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.DoubleFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FloatFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IntFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.LongFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.luceneserver.script.FacetScript;
import com.yelp.nrtsearch.server.luceneserver.script.ScriptService;
import com.yelp.nrtsearch.server.utils.ScriptParamsUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.range.DoubleRange;
import org.apache.lucene.facet.range.DoubleRangeFacetCounts;
import org.apache.lucene.facet.range.LongRange;
import org.apache.lucene.facet.range.LongRangeFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;

public class DrillSidewaysImpl extends DrillSideways {
  private final List<Facet> grpcFacets;
  private final SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomyManager;
  private final IndexState indexState;
  private final ShardState shardState;
  private final Map<String, FieldDef> dynamicFields;
  private final List<com.yelp.nrtsearch.server.grpc.FacetResult> grpcFacetResults;
  private final Diagnostics.Builder diagnostics;

  /**
   * @param searcher
   * @param config
   * @param taxoReader
   * @param grpcFacets
   * @param searcherAndTaxonomyManager
   * @param shardState
   * @param dynamicFields
   * @param diagnostics diagnostics builder for storing facet timing
   */
  public DrillSidewaysImpl(
      IndexSearcher searcher,
      FacetsConfig config,
      TaxonomyReader taxoReader,
      List<Facet> grpcFacets,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomyManager,
      IndexState indexState,
      ShardState shardState,
      Map<String, FieldDef> dynamicFields,
      List<com.yelp.nrtsearch.server.grpc.FacetResult> grpcFacetResults,
      ExecutorService executorService,
      Diagnostics.Builder diagnostics) {
    super(searcher, config, taxoReader, null, executorService);
    this.grpcFacets = grpcFacets;
    this.searcherAndTaxonomyManager = searcherAndTaxonomyManager;
    this.indexState = indexState;
    this.shardState = shardState;
    this.dynamicFields = dynamicFields;
    this.grpcFacetResults = grpcFacetResults;
    this.diagnostics = diagnostics;
  }

  protected Facets buildFacetsResult(
      FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims)
      throws IOException {
    fillFacetResults(
        drillDowns,
        drillSideways,
        drillSidewaysDims,
        indexState,
        shardState,
        grpcFacets,
        dynamicFields,
        searcherAndTaxonomyManager,
        grpcFacetResults,
        diagnostics);
    return null;
  }

  static void fillFacetResults(
      FacetsCollector drillDowns,
      FacetsCollector[] drillSideways,
      String[] drillSidewaysDims,
      IndexState indexState,
      ShardState shardState,
      List<Facet> grpcFacets,
      Map<String, FieldDef> dynamicFields,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomyManager,
      List<com.yelp.nrtsearch.server.grpc.FacetResult> grpcFacetResults,
      Diagnostics.Builder diagnostics)
      throws IOException {

    Map<String, FacetsCollector> dsDimMap = new HashMap<String, FacetsCollector>();
    if (drillSidewaysDims != null) {
      for (int i = 0; i < drillSidewaysDims.length; i++) {
        dsDimMap.put(drillSidewaysDims[i], drillSideways[i]);
      }
    }

    // Holds already computed Facets, since more
    // than one dimension can share a single
    // index field name.  We need one map for "normal" and
    // another for SSDV facets because an app can index both
    // into the same Lucene field (this is the default):
    Map<String, Facets> indexFieldNameToFacets = new HashMap<String, Facets>();
    Map<String, Facets> indexFieldNameToSSDVFacets = new HashMap<String, Facets>();

    for (Facet facet : grpcFacets) {
      // these facets will be created from the top docs
      if (facet.getSampleTopDocs() != 0) {
        continue;
      }

      long startNS = System.nanoTime();

      com.yelp.nrtsearch.server.grpc.FacetResult facetResult;
      if (facet.hasScript()) {
        // this facet is a FacetScript, run script against all matching documents
        facetResult = getScriptFacetResult(facet, drillDowns, indexState);
      } else {
        facetResult =
            getFieldFacetResult(
                drillDowns,
                dsDimMap,
                indexState,
                shardState,
                facet,
                dynamicFields,
                searcherAndTaxonomyManager,
                indexFieldNameToFacets);
      }
      if (facetResult != null) {
        grpcFacetResults.add(facetResult);
      }
      long endNS = System.nanoTime();
      diagnostics.putFacetTimeMs(facet.getName(), (endNS - startNS) / 1000000.0);
    }
  }

  private static com.yelp.nrtsearch.server.grpc.FacetResult getScriptFacetResult(
      Facet facet, FacetsCollector drillDowns, IndexState indexState) throws IOException {

    FacetScript.Factory factory =
        ScriptService.getInstance().compile(facet.getScript(), FacetScript.CONTEXT);
    FacetScript.SegmentFactory segmentFactory =
        factory.newFactory(
            ScriptParamsUtils.decodeParams(facet.getScript().getParamsMap()), indexState.docLookup);

    Map<Object, Integer> countsMap = new HashMap<>();
    int totalDocs = 0;
    // run script against all match docs, and aggregate counts
    for (MatchingDocs matchingDocs : drillDowns.getMatchingDocs()) {
      FacetScript script = segmentFactory.newInstance(matchingDocs.context);
      DocIdSetIterator iterator = matchingDocs.bits.iterator();
      if (iterator == null) {
        continue;
      }
      int docId = iterator.nextDoc();
      while (docId != DocIdSetIterator.NO_MORE_DOCS) {
        script.setDocId(docId);
        Object scriptResult = script.execute();
        if (scriptResult != null) {
          processScriptResult(scriptResult, countsMap);
        }
        totalDocs++;
        docId = iterator.nextDoc();
      }
    }
    return buildFacetResultFromCountsGrpc(countsMap, facet, totalDocs);
  }

  private static com.yelp.nrtsearch.server.grpc.FacetResult getDocValuesFacetResult(
      Facet facet, FacetsCollector drillDowns, IndexableFieldDef fieldDef) throws IOException {
    Map<Object, Integer> countsMap = new HashMap<>();
    int totalDocs = 0;
    // get doc values for all match docs, and aggregate counts
    for (MatchingDocs matchingDocs : drillDowns.getMatchingDocs()) {
      LoadedDocValues<?> docValues = fieldDef.getDocValues(matchingDocs.context);
      DocIdSetIterator iterator = matchingDocs.bits.iterator();
      if (iterator == null) {
        continue;
      }
      int docId = iterator.nextDoc();
      while (docId != DocIdSetIterator.NO_MORE_DOCS) {
        docValues.setDocId(docId);
        if (!docValues.isEmpty()) {
          for (Object value : docValues) {
            countsMap.merge(value, 1, Integer::sum);
          }
          totalDocs++;
        }
        docId = iterator.nextDoc();
      }
    }
    return buildFacetResultFromCountsGrpc(countsMap, facet, totalDocs);
  }

  private static void processScriptResult(Object scriptResult, Map<Object, Integer> countsMap) {
    if (scriptResult instanceof Iterable) {
      ((Iterable<?>) scriptResult)
          .forEach(
              v -> {
                if (v != null) {
                  countsMap.merge(v, 1, Integer::sum);
                }
              });
    } else {
      countsMap.merge(scriptResult, 1, Integer::sum);
    }
  }

  /**
   * Build a {@link com.yelp.nrtsearch.server.grpc.FacetResult} given a computed aggregation in the
   * form of a count map.
   *
   * @param countsMap map containing all aggregated values to the number of times they were observed
   * @param facet facet definition grpc message
   * @param totalDocs total number of docs aggregated
   * @return facet result grpc message
   */
  public static com.yelp.nrtsearch.server.grpc.FacetResult buildFacetResultFromCountsGrpc(
      Map<Object, Integer> countsMap, Facet facet, int totalDocs) {
    com.yelp.nrtsearch.server.grpc.FacetResult.Builder builder =
        com.yelp.nrtsearch.server.grpc.FacetResult.newBuilder();
    builder.setName(facet.getName());
    builder.setDim(facet.getDim());
    builder.addAllPath(facet.getPathsList());
    builder.setValue(totalDocs);
    builder.setChildCount(countsMap.size());

    if (countsMap.size() > 0 && facet.getTopN() > 0) {
      // add all map entries into a priority queue, keeping only the top N
      PriorityQueue<Map.Entry<Object, Integer>> priorityQueue =
          new PriorityQueue<>(
              Math.min(countsMap.size(), facet.getTopN()),
              Map.Entry.comparingByValue(Integer::compare));

      int minimumCount = -1;
      for (Map.Entry<Object, Integer> entry : countsMap.entrySet()) {
        if (priorityQueue.size() < facet.getTopN()) {
          priorityQueue.offer(entry);
          minimumCount = priorityQueue.peek().getValue();
        } else if (entry.getValue() > minimumCount) {
          priorityQueue.poll();
          priorityQueue.offer(entry);
          minimumCount = priorityQueue.peek().getValue();
        }
      }

      // the priority queue is a min heap, use a linked list to reverse the order
      LinkedList<com.yelp.nrtsearch.server.grpc.LabelAndValue> labelAndValues = new LinkedList<>();
      while (!priorityQueue.isEmpty()) {
        Map.Entry<Object, Integer> entry = priorityQueue.poll();
        labelAndValues.addFirst(
            com.yelp.nrtsearch.server.grpc.LabelAndValue.newBuilder()
                // the key will never be null, since we check before adding
                .setLabel(entry.getKey().toString())
                .setValue(entry.getValue())
                .build());
      }
      builder.addAllLabelValues(labelAndValues);
    }
    return builder.build();
  }

  private static com.yelp.nrtsearch.server.grpc.FacetResult getFieldFacetResult(
      FacetsCollector drillDowns,
      Map<String, FacetsCollector> dsDimMap,
      IndexState indexState,
      ShardState shardState,
      Facet facet,
      Map<String, FieldDef> dynamicFields,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomyManager,
      Map<String, Facets> indexFieldNameToFacets)
      throws IOException {

    String fieldName = facet.getDim();
    FieldDef fieldDef = dynamicFields.get(fieldName);
    if (fieldDef == null) {
      throw new IllegalArgumentException(
          String.format(
              "field %s was not registered and was not specified as a dynamic field ", fieldName));
    }

    FacetResult facetResult;
    if (!(fieldDef instanceof IndexableFieldDef) && !(fieldDef instanceof VirtualFieldDef)) {
      throw new IllegalArgumentException(
          String.format(
              "field %s is neither a virtual field nor registered as an indexable field. Facets are supported only for these types",
              fieldName));
    }
    if (!facet.getNumericRangeList().isEmpty()) {

      if (fieldDef.getFacetValueType() != IndexableFieldDef.FacetValueType.NUMERIC_RANGE) {
        throw new IllegalArgumentException(
            String.format(
                "field %s was not registered with facet=numericRange", fieldDef.getName()));
      }
      if (fieldDef instanceof IntFieldDef || fieldDef instanceof LongFieldDef) {
        List<NumericRangeType> rangeList = facet.getNumericRangeList();
        LongRange[] ranges = new LongRange[rangeList.size()];
        for (int i = 0; i < ranges.length; i++) {
          NumericRangeType numericRangeType = rangeList.get(i);
          ranges[i] =
              new LongRange(
                  numericRangeType.getLabel(),
                  numericRangeType.getMin(),
                  numericRangeType.getMinInclusive(),
                  numericRangeType.getMax(),
                  numericRangeType.getMaxInclusive());
        }

        FacetsCollector c = dsDimMap.get(fieldDef.getName());
        if (c == null) {
          c = drillDowns;
        }
        LongRangeFacetCounts longRangeFacetCounts =
            new LongRangeFacetCounts(fieldDef.getName(), c, ranges);

        facetResult =
            longRangeFacetCounts.getTopChildren(
                0,
                fieldDef.getName(),
                facet.getPathsList().toArray(new String[facet.getPathsCount()]));
      } else if (fieldDef instanceof FloatFieldDef) {
        throw new IllegalArgumentException(
            String.format(
                "field %s is of type float with FloatFieldDocValues which do not support numeric_range faceting",
                fieldDef.getName()));

      } else if (fieldDef instanceof DoubleFieldDef || fieldDef instanceof VirtualFieldDef) {
        List<NumericRangeType> rangeList = facet.getNumericRangeList();
        DoubleRange[] ranges = new DoubleRange[rangeList.size()];
        for (int i = 0; i < ranges.length; i++) {
          NumericRangeType numericRangeType = rangeList.get(i);
          ranges[i] =
              new DoubleRange(
                  numericRangeType.getLabel(),
                  numericRangeType.getMin(),
                  numericRangeType.getMinInclusive(),
                  numericRangeType.getMax(),
                  numericRangeType.getMaxInclusive());
        }

        FacetsCollector c = dsDimMap.get(fieldDef.getName());
        if (c == null) {
          c = drillDowns;
        }
        DoubleRangeFacetCounts doubleRangeFacetCounts;
        if (fieldDef instanceof VirtualFieldDef) {
          VirtualFieldDef virtualFieldDef = (VirtualFieldDef) fieldDef;
          doubleRangeFacetCounts =
              new DoubleRangeFacetCounts(
                  virtualFieldDef.getName(), virtualFieldDef.getValuesSource(), c, ranges);

        } else {
          doubleRangeFacetCounts = new DoubleRangeFacetCounts(fieldDef.getName(), c, ranges);
        }

        facetResult =
            doubleRangeFacetCounts.getTopChildren(
                0,
                fieldDef.getName(),
                facet.getPathsList().toArray(new String[facet.getPathsCount()]));
      } else {
        throw new IllegalArgumentException(
            String.format(
                "numericRanges must be provided only on field type numeric e.g. int, double, flat"));
      }
    } else if (fieldDef.getFacetValueType()
        == IndexableFieldDef.FacetValueType.SORTED_SET_DOC_VALUES) {
      FacetsCollector c = dsDimMap.get(fieldDef.getName());
      if (c == null) {
        c = drillDowns;
      }
      if (facet.getLabelsCount() > 0) {
        // filter facet if a label list is provided
        FilteredSSDVFacetCounts filteredSSDVFacetCounts =
            new FilteredSSDVFacetCounts(
                facet.getLabelsList(),
                fieldDef.getName(),
                shardState.getSSDVState(indexState, searcherAndTaxonomyManager, fieldDef),
                c);
        facetResult = filteredSSDVFacetCounts.getTopChildren(facet.getTopN(), fieldDef.getName());
      } else {
        SortedSetDocValuesFacetCounts sortedSetDocValuesFacetCounts =
            new SortedSetDocValuesFacetCounts(
                shardState.getSSDVState(indexState, searcherAndTaxonomyManager, fieldDef), c);
        facetResult =
            sortedSetDocValuesFacetCounts.getTopChildren(facet.getTopN(), fieldDef.getName());
      }
    } else if (fieldDef.getFacetValueType() != IndexableFieldDef.FacetValueType.NO_FACETS) {

      // Taxonomy  facets
      if (fieldDef.getFacetValueType() == IndexableFieldDef.FacetValueType.NUMERIC_RANGE) {
        throw new IllegalArgumentException(
            String.format(
                "%s was registered with facet = numericRange; must pass numericRanges in the request",
                fieldDef.getName()));
      }

      String[] path;
      if (!facet.getPathsList().isEmpty()) {
        ProtocolStringList pathList = facet.getPathsList();
        path = new String[facet.getPathsList().size()];
        for (int idx = 0; idx < path.length; idx++) {
          path[idx] = pathList.get(idx);
        }
      } else {
        path = new String[0];
      }

      FacetsCollector c = dsDimMap.get(fieldDef.getName());
      boolean useCachedOrds = facet.getUseOrdsCache();

      Facets luceneFacets;
      if (c != null) {
        // This dimension was used in
        // drill-down; compute its facet counts from the
        // drill-sideways collector:
        String indexFieldName =
            indexState.getFacetsConfig().getDimConfig(fieldDef.getName()).indexFieldName;
        if (useCachedOrds) {
          luceneFacets =
              new TaxonomyFacetCounts(
                  shardState.getOrdsCache(indexFieldName),
                  searcherAndTaxonomyManager.taxonomyReader,
                  indexState.getFacetsConfig(),
                  c);
        } else {
          luceneFacets =
              new FastTaxonomyFacetCounts(
                  indexFieldName,
                  searcherAndTaxonomyManager.taxonomyReader,
                  indexState.getFacetsConfig(),
                  c);
        }
      } else {

        // nocommit test both normal & ssdv facets in same index

        // See if we already computed facet
        // counts for this indexFieldName:
        String indexFieldName =
            indexState.getFacetsConfig().getDimConfig(fieldDef.getName()).indexFieldName;
        Map<String, Facets> facetsMap = indexFieldNameToFacets;
        luceneFacets = facetsMap.get(indexFieldName);
        if (luceneFacets == null) {
          if (useCachedOrds) {
            luceneFacets =
                new TaxonomyFacetCounts(
                    shardState.getOrdsCache(indexFieldName),
                    searcherAndTaxonomyManager.taxonomyReader,
                    indexState.getFacetsConfig(),
                    drillDowns);
          } else {
            luceneFacets =
                new FastTaxonomyFacetCounts(
                    indexFieldName,
                    searcherAndTaxonomyManager.taxonomyReader,
                    indexState.getFacetsConfig(),
                    drillDowns);
          }
          facetsMap.put(indexFieldName, luceneFacets);
        }
      }
      if (facet.getTopN() != 0) {
        facetResult = luceneFacets.getTopChildren(facet.getTopN(), fieldDef.getName(), path);
      } else if (!facet.getLabelsList().isEmpty()) {
        List<LabelAndValue> results = new ArrayList<LabelAndValue>();
        for (String label : facet.getLabelsList()) {
          results.add(
              new LabelAndValue(label, luceneFacets.getSpecificValue(fieldDef.getName(), label)));
        }
        facetResult =
            new FacetResult(
                fieldDef.getName(),
                path,
                -1,
                results.toArray(new LabelAndValue[results.size()]),
                -1);
      } else {
        throw new IllegalArgumentException(
            String.format("each facet request must have either topN or labels"));
      }
    } else {
      // if no facet type is enabled on the field, try using the field doc values
      if (!(fieldDef instanceof IndexableFieldDef)) {
        throw new IllegalArgumentException(
            "Doc values facet requires an indexable field : " + fieldName);
      }
      IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fieldDef;
      if (!indexableFieldDef.hasDocValues()) {
        throw new IllegalArgumentException(
            "Doc values facet requires doc values enabled : " + fieldName);
      }
      return getDocValuesFacetResult(facet, drillDowns, indexableFieldDef);
    }
    return buildFacetResultGrpc(facetResult, facet.getName());
  }

  private static com.yelp.nrtsearch.server.grpc.FacetResult buildFacetResultGrpc(
      FacetResult facetResult, String name) {
    var builder = com.yelp.nrtsearch.server.grpc.FacetResult.newBuilder();
    builder.setName(name);
    if (facetResult != null) {
      builder.setDim(facetResult.dim);
      builder.addAllPath(Arrays.asList(facetResult.path));
      builder.setValue(facetResult.value.doubleValue());
      builder.setChildCount(facetResult.childCount);
      List<com.yelp.nrtsearch.server.grpc.LabelAndValue> labelAndValues = new ArrayList<>();
      for (LabelAndValue labelValue : facetResult.labelValues) {
        var labelAndValue =
            com.yelp.nrtsearch.server.grpc.LabelAndValue.newBuilder()
                .setLabel(labelValue.label)
                .setValue(labelValue.value.doubleValue())
                .build();
        labelAndValues.add(labelAndValue);
      }
      builder.addAllLabelValues(labelAndValues);
    }
    return builder.build();
  }
}
