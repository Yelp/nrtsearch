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

import com.yelp.nrtsearch.server.grpc.Facet;
import com.yelp.nrtsearch.server.grpc.FacetResult;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Diagnostics;
import com.yelp.nrtsearch.server.luceneserver.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.luceneserver.field.FieldDef;
import com.yelp.nrtsearch.server.luceneserver.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.luceneserver.index.IndexState;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;

/**
 * Class that manages collecting of Facet aggregations based on the top ranked documents. Collection
 * is based on the doc values of the field specified as the facet dimension.
 */
public class FacetTopDocs {

  private FacetTopDocs() {}

  /**
   * Compute facet aggregations based on a sample of the query's top ranked documents. Only
   * processes facets that have sampleTopDocs set to a value greater than zero.
   *
   * @param topDocs top documents from query
   * @param facets facet definition grpc messages
   * @param indexState state for index
   * @param searcher searcher for query
   * @param diagnostics diagnostics builder for storing facet timing
   * @return results for facets over top docs
   * @throws IOException if error reading doc values
   * @throws IllegalArgumentException if top docs facet field is not indexable or does not have doc
   *     values enabled
   */
  public static Iterable<FacetResult> facetTopDocsSample(
      TopDocs topDocs,
      List<Facet> facets,
      IndexState indexState,
      IndexSearcher searcher,
      Diagnostics.Builder diagnostics)
      throws IOException {
    List<Facet> sampleFacets =
        facets.stream().filter(facet -> facet.getSampleTopDocs() > 0).collect(Collectors.toList());
    if (sampleFacets.isEmpty()) {
      return Collections.emptyList();
    }

    List<FacetResult> facetResults = new ArrayList<>(sampleFacets.size());
    for (Facet facet : sampleFacets) {
      long startNS = System.nanoTime();
      facetResults.add(facetFromTopDocs(topDocs, facet, indexState, searcher));
      long endNS = System.nanoTime();
      diagnostics.putFacetTimeMs(facet.getName(), (endNS - startNS) / 1000000.0);
    }
    return facetResults;
  }

  private static FacetResult facetFromTopDocs(
      TopDocs topDocs, Facet facet, IndexState indexState, IndexSearcher searcher)
      throws IOException {
    FieldDef fieldDef = indexState.getField(facet.getDim());
    if (!(fieldDef instanceof IndexableFieldDef)) {
      throw new IllegalArgumentException(
          "Sampling facet field must be indexable: " + facet.getDim());
    }
    IndexableFieldDef indexableFieldDef = (IndexableFieldDef) fieldDef;
    if (!indexableFieldDef.hasDocValues()) {
      throw new IllegalArgumentException(
          "Sampling facet field must have doc values enabled: " + facet.getDim());
    }
    Map<Object, Integer> countsMap = new HashMap<>();
    int totalDocs = 0;
    int maxDoc = Math.min(topDocs.scoreDocs.length, facet.getSampleTopDocs());
    List<LeafReaderContext> leaves = searcher.getIndexReader().leaves();
    for (int i = 0; i < maxDoc; ++i) {
      LeafReaderContext context = leaves.get(ReaderUtil.subIndex(topDocs.scoreDocs[i].doc, leaves));

      LoadedDocValues<?> docValues = indexableFieldDef.getDocValues(context);
      docValues.setDocId(topDocs.scoreDocs[i].doc - context.docBase);
      if (docValues.isEmpty()) {
        continue;
      }
      for (Object value : docValues) {
        countsMap.merge(value, 1, Integer::sum);
      }
      totalDocs++;
    }
    return DrillSidewaysImpl.buildFacetResultFromCountsGrpc(countsMap, facet, totalDocs);
  }
}
