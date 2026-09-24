/*
 * Copyright 2026 Yelp Inc.
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
package com.yelp.nrtsearch.server.search.crossindex;

import com.yelp.nrtsearch.server.doc.LoadedDocValues;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.grpc.CrossIndexLookup;
import com.yelp.nrtsearch.server.grpc.CrossIndexResults;
import com.yelp.nrtsearch.server.grpc.SearchResponse;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.CompositeFieldValue;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.grpc.SecondaryHit;
import com.yelp.nrtsearch.server.search.FetchTasks;
import com.yelp.nrtsearch.server.search.SearchContext;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;

/**
 * FetchTask that writes materialized cross-index lookup results to each primary hit's
 * crossIndexResults field in the response.
 */
public class CrossIndexLookupFetchTask implements FetchTasks.FetchTask {

  private final CrossIndexLookupManager manager;
  private final List<CrossIndexLookup> lookups;

  public CrossIndexLookupFetchTask(
      CrossIndexLookupManager manager, List<CrossIndexLookup> lookups) {
    this.manager = manager;
    this.lookups = lookups;
  }

  @Override
  public void processHit(
      SearchContext searchContext,
      LeafReaderContext hitLeaf,
      SearchResponse.Hit.Builder hit)
      throws IOException {

    for (CrossIndexLookup lookup : lookups) {
      if (lookup.getRetrieveFieldsList().isEmpty()) {
        continue;
      }

      String indexName = lookup.getIndex();
      String primaryField = lookup.getPrimaryField();

      // Read join key from primary doc values
      String joinKey = readJoinKey(searchContext, hitLeaf, hit, primaryField);
      if (joinKey == null) {
        continue;
      }

      // Look up materialized results
      List<Map<String, CompositeFieldValue>> secondaryHits =
          manager.getResults(indexName, joinKey);
      if (secondaryHits == null || secondaryHits.isEmpty()) {
        continue;
      }

      // Build CrossIndexResults proto
      CrossIndexResults.Builder resultsBuilder = CrossIndexResults.newBuilder();
      for (Map<String, CompositeFieldValue> hitFields : secondaryHits) {
        SecondaryHit.Builder secHitBuilder = SecondaryHit.newBuilder();
        // Only include retrieve_fields (not expose_to_scripts-only fields)
        for (String fieldName : lookup.getRetrieveFieldsList()) {
          CompositeFieldValue cfv = hitFields.get(fieldName);
          if (cfv != null) {
            secHitBuilder.putFields(fieldName, cfv);
          }
        }
        resultsBuilder.addHits(secHitBuilder);
      }

      hit.putCrossIndexResults(indexName, resultsBuilder.build());
    }
  }

  private String readJoinKey(
      SearchContext searchContext,
      LeafReaderContext hitLeaf,
      SearchResponse.Hit.Builder hit,
      String primaryField)
      throws IOException {
    // Look up the primary field def from the search context
    var fieldDef = searchContext.getQueryFields().get(primaryField);
    if (!(fieldDef instanceof IndexableFieldDef<?> indexableDef) || !indexableDef.hasDocValues()) {
      return null;
    }

    int segmentDocId = hit.getLuceneDocId() - hitLeaf.docBase;
    LoadedDocValues<?> docValues = indexableDef.getDocValues(hitLeaf);
    docValues.setDocId(segmentDocId);

    if (docValues.size() == 0) {
      return null;
    }

    FieldValue fv = docValues.toFieldValue(0);
    if (fv.hasTextValue() && !fv.getTextValue().isEmpty()) {
      return fv.getTextValue();
    } else if (fv.hasIntValue()) {
      return String.valueOf(fv.getIntValue());
    } else if (fv.hasLongValue()) {
      return String.valueOf(fv.getLongValue());
    }
    return null;
  }
}
