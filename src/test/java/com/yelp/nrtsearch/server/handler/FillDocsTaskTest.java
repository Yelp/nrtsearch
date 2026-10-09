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
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.VirtualFieldDef;
import com.yelp.nrtsearch.server.handler.SearchHandler.FillDocsTask;
import com.yelp.nrtsearch.server.search.FieldFetchContext;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.facet.taxonomy.SearcherTaxonomyManager.SearcherAndTaxonomy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.IndexSearcher;
import org.junit.Test;

public class FillDocsTaskTest {

  private static IndexableFieldDef<?> field(boolean stored, boolean docValues) {
    IndexableFieldDef<?> fieldDef = mock(IndexableFieldDef.class);
    when(fieldDef.isStored()).thenReturn(stored);
    when(fieldDef.hasDocValues()).thenReturn(docValues);
    return fieldDef;
  }

  private static FieldFetchContext fetchContext(
      Map<String, FieldDef> retrieveFields, StoredFields storedFields) throws IOException {
    IndexSearcher searcher = mock(IndexSearcher.class);
    when(searcher.storedFields()).thenReturn(storedFields);
    FieldFetchContext context = mock(FieldFetchContext.class);
    when(context.getRetrieveFields()).thenReturn(retrieveFields);
    when(context.getSearcherAndTaxonomy()).thenReturn(new SearcherAndTaxonomy(searcher, null));
    return context;
  }

  @Test
  public void testStoredOnlyFieldIsReadFromStoredFields() throws IOException {
    StoredFields storedFields = mock(StoredFields.class);
    Map<String, FieldDef> retrieveFields = Map.of("stored_only", field(true, false));

    FillDocsTask.StoredFieldFetchContext storedContext =
        FillDocsTask.getStoredFieldFetchContext(fetchContext(retrieveFields, storedFields));

    assertSame(storedFields, storedContext.storedFields());
    assertEquals(Set.of("stored_only"), storedContext.fieldNames());
    assertEquals(1, storedContext.nameAndFieldDefs().size());
    assertEquals("stored_only", storedContext.nameAndFieldDefs().get(0).name());
  }

  @Test
  public void testStoredFieldWithDocValuesIsNotReadFromStoredFields() throws IOException {
    // fetchSlice already fills these from doc values; reading the stored document as well
    // would decompress a stored fields block per hit only to overwrite the same value
    Map<String, FieldDef> retrieveFields =
        Map.of("stored_and_doc_values", field(true, true), "doc_values_only", field(false, true));

    FillDocsTask.StoredFieldFetchContext storedContext =
        FillDocsTask.getStoredFieldFetchContext(
            fetchContext(retrieveFields, mock(StoredFields.class)));

    assertTrue(storedContext.fieldNames().isEmpty());
    assertTrue(storedContext.nameAndFieldDefs().isEmpty());
  }

  @Test
  public void testOnlyStoredOnlyFieldsAreReadFromStoredFields() throws IOException {
    Map<String, FieldDef> retrieveFields = new LinkedHashMap<>();
    retrieveFields.put("stored_only", field(true, false));
    retrieveFields.put("stored_and_doc_values", field(true, true));
    retrieveFields.put("doc_values_only", field(false, true));
    retrieveFields.put("virtual", mock(VirtualFieldDef.class));

    FillDocsTask.StoredFieldFetchContext storedContext =
        FillDocsTask.getStoredFieldFetchContext(
            fetchContext(retrieveFields, mock(StoredFields.class)));

    assertEquals(Set.of("stored_only"), storedContext.fieldNames());
    assertEquals(1, storedContext.nameAndFieldDefs().size());
    assertEquals("stored_only", storedContext.nameAndFieldDefs().get(0).name());
  }
}
