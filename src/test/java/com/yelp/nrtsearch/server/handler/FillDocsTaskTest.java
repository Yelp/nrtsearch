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
package com.yelp.nrtsearch.server.handler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit;
import com.yelp.nrtsearch.server.grpc.SearchResponse.Hit.FieldValue;
import com.yelp.nrtsearch.server.handler.SearchHandler.FillDocsTask;
import com.yelp.nrtsearch.server.search.FetchTasks;
import com.yelp.nrtsearch.server.search.FieldFetchContext;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;

public class FillDocsTaskTest {

  private static IndexableFieldDef<?> field(boolean stored, boolean docValues) {
    IndexableFieldDef<?> fieldDef = mock(IndexableFieldDef.class);
    when(fieldDef.isStored()).thenReturn(stored);
    when(fieldDef.hasDocValues()).thenReturn(docValues);
    when(fieldDef.getStoredFieldValue(any(StoredValue.class)))
        .thenAnswer(
            invocation -> {
              StoredValue value = invocation.getArgument(0);
              return FieldValue.newBuilder().setTextValue(value.getStringValue()).build();
            });
    return fieldDef;
  }

  private static FieldFetchContext fetchContext(
      Map<String, FieldDef> retrieveFields, StoredFields storedFields) throws IOException {
    IndexSearcher searcher = mock(IndexSearcher.class);
    when(searcher.storedFields()).thenReturn(storedFields);
    FieldFetchContext context = mock(FieldFetchContext.class);
    when(context.getRetrieveFields()).thenReturn(retrieveFields);
    when(context.getSearcherAndTaxonomy()).thenReturn(new SearcherAndTaxonomy(searcher, null));
    when(context.getFetchTasks()).thenReturn(mock(FetchTasks.class));
    return context;
  }

  private static List<String> textValues(Hit.Builder hit, String field) {
    return hit.getFieldsOrThrow(field).getFieldValueList().stream()
        .map(FieldValue::getTextValue)
        .toList();
  }

  @Test
  public void testStoredFieldsAreReadFromStoredFields() throws IOException {
    StoredFields storedFields = mock(StoredFields.class);
    Map<String, FieldDef> retrieveFields = new LinkedHashMap<>();
    retrieveFields.put("stored_only", field(true, false));
    retrieveFields.put("stored_and_doc_values", field(true, true));
    retrieveFields.put("doc_values_only", field(false, true));
    retrieveFields.put("virtual", mock(VirtualFieldDef.class));

    FillDocsTask.StoredFieldFetchContext storedContext =
        FillDocsTask.getStoredFieldFetchContext(fetchContext(retrieveFields, storedFields));

    assertSame(storedFields, storedContext.storedFields());
    assertEquals(Set.of("stored_only", "stored_and_doc_values"), storedContext.fieldNames());
    assertEquals(
        List.of("stored_only", "stored_and_doc_values"),
        storedContext.nameAndFieldDefs().stream().map(FillDocsTask.NameAndFieldDef::name).toList());
  }

  @Test
  public void testStoredFieldWithDocValuesIsOnlyReadFromStoredFields() throws IOException {
    // stored values keep their original order, so they take priority over doc values, and the
    // doc values are not read at all
    IndexableFieldDef<?> storedAndDocValues = field(true, true);
    Map<String, FieldDef> retrieveFields = Map.of("stored_and_doc_values", storedAndDocValues);

    Document document = new Document();
    document.add(new StoredField("stored_and_doc_values", "b"));
    document.add(new StoredField("stored_and_doc_values", "a"));
    document.add(new StoredField("stored_and_doc_values", "b"));
    StoredFields storedFields = mock(StoredFields.class);
    when(storedFields.document(eq(7), anySet())).thenReturn(document);

    FieldFetchContext context = fetchContext(retrieveFields, storedFields);
    Hit.Builder hit = Hit.newBuilder().setLuceneDocId(7);
    FillDocsTask.fetchSlice(
        context, null, List.of(hit), null, FillDocsTask.getStoredFieldFetchContext(context));

    assertEquals(List.of("b", "a", "b"), textValues(hit, "stored_and_doc_values"));
    verify(storedAndDocValues, never()).getDocValues(any());
  }
}
