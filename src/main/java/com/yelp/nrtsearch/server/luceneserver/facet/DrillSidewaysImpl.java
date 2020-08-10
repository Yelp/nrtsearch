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
import com.yelp.nrtsearch.server.luceneserver.IndexState;
import com.yelp.nrtsearch.server.luceneserver.ShardState;
import com.yelp.nrtsearch.server.luceneserver.field.DoubleFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.FloatFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IntFieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.LongFieldDef;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
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
import org.apache.lucene.search.IndexSearcher;

public class DrillSidewaysImpl extends DrillSideways {
  private final List<Facet> grpcFacets;
  private final SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomyManager;
  private final ShardState shardState;
  private final Map<String, FieldDef> dynamicFields;
  private final List<com.yelp.nrtsearch.server.grpc.FacetResult> grpcFacetResults;

  /**
   * @param searcher
   * @param config
   * @param taxoReader
   * @param grpcFacets
   * @param searcherAndTaxonomyManager
   * @param shardState
   * @param dynamicFields
   */
  public DrillSidewaysImpl(
      IndexSearcher searcher,
      FacetsConfig config,
      TaxonomyReader taxoReader,
      List<Facet> grpcFacets,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomyManager,
      ShardState shardState,
      Map<String, FieldDef> dynamicFields,
      List<com.yelp.nrtsearch.server.grpc.FacetResult> grpcFacetResults,
      ExecutorService executorService) {
    super(searcher, config, taxoReader, null, executorService);
    this.grpcFacets = grpcFacets;
    this.searcherAndTaxonomyManager = searcherAndTaxonomyManager;
    this.shardState = shardState;
    this.dynamicFields = dynamicFields;
    this.grpcFacetResults = grpcFacetResults;
  }

  protected Facets buildFacetsResult(
      FacetsCollector drillDowns, FacetsCollector[] drillSideways, String[] drillSidewaysDims)
      throws IOException {
    fillFacetResults(
        drillDowns,
        drillSideways,
        drillSidewaysDims,
        shardState,
        grpcFacets,
        dynamicFields,
        searcherAndTaxonomyManager,
        grpcFacetResults);
    return null;
  }

  static void fillFacetResults(
      FacetsCollector drillDowns,
      FacetsCollector[] drillSideways,
      String[] drillSidewaysDims,
      ShardState shardState,
      List<Facet> grpcFacets,
      Map<String, FieldDef> dynamicFields,
      SearcherTaxonomyManager.SearcherAndTaxonomy searcherAndTaxonomyManager,
      List<com.yelp.nrtsearch.server.grpc.FacetResult> grpcFacetResults)
      throws IOException {

    IndexState indexState = shardState.indexState;

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
      String fieldName = facet.getDim();
      FieldDef fd = dynamicFields.get(fieldName);
      if (fd == null) {
        throw new IllegalArgumentException(
            String.format(
                "field %s was not registered and was not specified as a dynamic field ",
                fieldName));
      }

      FacetResult facetResult;
      if (!(fd instanceof IndexableFieldDef)) { // TODO: Also enable facet support in Virtual Fields
        throw new IllegalArgumentException(
            String.format(
                "field % not registered as an indexable field. Facets applicable only to indexable fields",
                fieldName));
      }
      IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fd;
      if (!facet.getNumericRangeList().isEmpty()) {

        if (indexableFieldDef.getFacetValueType()
            != IndexableFieldDef.FacetValueType.NUMERIC_RANGE) {
          throw new IllegalArgumentException(
              String.format(
                  "field %s was not registered with facet=numericRange",
                  indexableFieldDef.getName()));
        }
        if (indexableFieldDef instanceof IntFieldDef || indexableFieldDef instanceof LongFieldDef) {
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

          FacetsCollector c = dsDimMap.get(indexableFieldDef.getName());
          if (c == null) {
            c = drillDowns;
          }

          LongRangeFacetCounts longRangeFacetCounts =
              new LongRangeFacetCounts(indexableFieldDef.getName(), c, ranges);
          facetResult =
              longRangeFacetCounts.getTopChildren(
                  0,
                  indexableFieldDef.getName(),
                  facet.getPathsList().toArray(new String[facet.getPathsCount()]));
        } else if (indexableFieldDef instanceof FloatFieldDef
            || indexableFieldDef
                instanceof DoubleFieldDef) { // TODO  this should allow VirtualFieldDef to
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

          FacetsCollector c = dsDimMap.get(indexableFieldDef.getName());
          if (c == null) {
            c = drillDowns;
          }

          DoubleRangeFacetCounts doubleRangeFacetCounts =
              new DoubleRangeFacetCounts(
                  indexableFieldDef.getName(),
                  c, // TODO: for VirtualFieldDef pass valueSource here
                  ranges);
          facetResult =
              doubleRangeFacetCounts.getTopChildren(
                  0,
                  indexableFieldDef.getName(),
                  facet.getPathsList().toArray(new String[facet.getPathsCount()]));
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "numericRanges must be provided only on field type numeric e.g. int, double, flat"));
        }
      } else if (indexableFieldDef.getFacetValueType()
          == IndexableFieldDef.FacetValueType.SORTED_SET_DOC_VALUES) {
        FacetsCollector c = dsDimMap.get(indexableFieldDef.getName());
        if (c == null) {
          c = drillDowns;
        }
        SortedSetDocValuesFacetCounts sortedSetDocValuesFacetCounts =
            new SortedSetDocValuesFacetCounts(
                shardState.getSSDVState(searcherAndTaxonomyManager, indexableFieldDef), c);
        facetResult =
            sortedSetDocValuesFacetCounts.getTopChildren(
                facet.getTopN(), indexableFieldDef.getName(), new String[0]);
      } else {

        // Taxonomy  facets
        if (indexableFieldDef.getFacetValueType() == IndexableFieldDef.FacetValueType.NO_FACETS) {
          throw new IllegalArgumentException(
              String.format(
                  "%s was not registered with facet enabled", indexableFieldDef.getName()));
        } else if (indexableFieldDef.getFacetValueType()
            == IndexableFieldDef.FacetValueType.NUMERIC_RANGE) {
          throw new IllegalArgumentException(
              String.format(
                  "%s was registered with facet = numericRange; must pass numericRanges in the request",
                  indexableFieldDef.getName()));
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

        FacetsCollector c = dsDimMap.get(indexableFieldDef.getName());
        boolean useCachedOrds = facet.getUseOrdsCache();

        Facets luceneFacets;
        if (c != null) {
          // This dimension was used in
          // drill-down; compute its facet counts from the
          // drill-sideways collector:
          String indexFieldName =
              indexState.facetsConfig.getDimConfig(indexableFieldDef.getName()).indexFieldName;
          if (useCachedOrds) {
            luceneFacets =
                new TaxonomyFacetCounts(
                    shardState.getOrdsCache(indexFieldName),
                    searcherAndTaxonomyManager.taxonomyReader,
                    indexState.facetsConfig,
                    c);
          } else {
            luceneFacets =
                new FastTaxonomyFacetCounts(
                    indexFieldName,
                    searcherAndTaxonomyManager.taxonomyReader,
                    indexState.facetsConfig,
                    c);
          }
        } else {

          // nocommit test both normal & ssdv facets in same index

          // See if we already computed facet
          // counts for this indexFieldName:
          String indexFieldName =
              indexState.facetsConfig.getDimConfig(indexableFieldDef.getName()).indexFieldName;
          Map<String, Facets> facetsMap = indexFieldNameToFacets;
          luceneFacets = facetsMap.get(indexFieldName);
          if (luceneFacets == null) {
            if (useCachedOrds) {
              luceneFacets =
                  new TaxonomyFacetCounts(
                      shardState.getOrdsCache(indexFieldName),
                      searcherAndTaxonomyManager.taxonomyReader,
                      indexState.facetsConfig,
                      drillDowns);
            } else {
              luceneFacets =
                  new FastTaxonomyFacetCounts(
                      indexFieldName,
                      searcherAndTaxonomyManager.taxonomyReader,
                      indexState.facetsConfig,
                      drillDowns);
            }
            facetsMap.put(indexFieldName, luceneFacets);
          }
        }
        if (facet.getTopN() != 0) {
          facetResult =
              luceneFacets.getTopChildren(facet.getTopN(), indexableFieldDef.getName(), path);
        } else if (!facet.getLabelsList().isEmpty()) {
          List<LabelAndValue> results = new ArrayList<LabelAndValue>();
          for (String label : facet.getLabelsList()) {
            results.add(
                new LabelAndValue(
                    label, luceneFacets.getSpecificValue(indexableFieldDef.getName(), label)));
          }
          facetResult =
              new FacetResult(
                  indexableFieldDef.getName(),
                  path,
                  -1,
                  results.toArray(new LabelAndValue[results.size()]),
                  -1);
        } else {
          throw new IllegalArgumentException(
              String.format("each facet request must have either topN or labels"));
        }
      }
      if (facetResult != null) {
        grpcFacetResults.add(buildFacetResultGrpc(facetResult));
      }
    }
  }

  private static com.yelp.nrtsearch.server.grpc.FacetResult buildFacetResultGrpc(
      FacetResult facetResult) {
    var builder = com.yelp.nrtsearch.server.grpc.FacetResult.newBuilder();
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
    return builder.build();
  }
}
