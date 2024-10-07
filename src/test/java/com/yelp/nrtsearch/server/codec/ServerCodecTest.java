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
package com.yelp.nrtsearch.server.codec;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.field.FieldDef;
import com.yelp.nrtsearch.server.field.IndexableFieldDef;
import com.yelp.nrtsearch.server.field.VectorFieldDef;
import com.yelp.nrtsearch.server.index.IndexState;
import com.yelp.nrtsearch.server.index.IndexStateManager;
import com.yelp.nrtsearch.server.similarity.SimilarityCreator;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Set;
import org.apache.lucene.backward_codecs.lucene80.Lucene80DocValuesFormat;
import org.apache.lucene.backward_codecs.lucene90.Lucene90HnswVectorsFormat;
import org.apache.lucene.backward_codecs.lucene90.Lucene90PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene912.Lucene912PostingsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.junit.BeforeClass;
import org.junit.Test;

public class ServerCodecTest {

  @BeforeClass
  public static void beforeClass() {
    SimilarityCreator.initialize(
        new NrtsearchConfig(new ByteArrayInputStream("name: node1".getBytes())), List.of());
  }

  private IndexStateManager getManager(FieldDef fieldDef) {
    IndexStateManager mockStateManager = mock(IndexStateManager.class);
    IndexState mockIndexState = mock(IndexState.class);
    when(mockStateManager.getCurrent()).thenReturn(mockIndexState);
    when(mockIndexState.getField("field")).thenReturn(fieldDef);
    when(mockIndexState.getInternalFacetFieldNames()).thenReturn(Set.of("internal_field"));
    return mockStateManager;
  }

  @Test
  public void testPostingFormat_default() {
    IndexableFieldDef mockFieldDef = mock(IndexableFieldDef.class);
    when(mockFieldDef.getPostingsFormat()).thenReturn(null);
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(serverCodec.getPostingsFormatForField("field") instanceof Lucene912PostingsFormat);
    verify(mockFieldDef, times(1)).getPostingsFormat();
    verifyNoMoreInteractions(mockFieldDef);
  }

  @Test
  public void testPostingFormat_provided() {
    IndexableFieldDef mockFieldDef = mock(IndexableFieldDef.class);
    when(mockFieldDef.getPostingsFormat()).thenReturn(new Lucene90PostingsFormat());
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(serverCodec.getPostingsFormatForField("field") instanceof Lucene90PostingsFormat);
    verify(mockFieldDef, times(1)).getPostingsFormat();
    verifyNoMoreInteractions(mockFieldDef);
  }

  @Test
  public void testPostingFormat_notIndexable() {
    FieldDef mockFieldDef = mock(FieldDef.class);
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    try {
      serverCodec.getPostingsFormatForField("field");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Field \"field\" is not indexable"));
    }
    verifyNoInteractions(mockFieldDef);
  }

  @Test
  public void testPostingFormat_internalField() {
    FieldDef mockFieldDef = mock(FieldDef.class);
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(
        serverCodec.getPostingsFormatForField("internal_field") instanceof Lucene912PostingsFormat);
    verifyNoInteractions(mockFieldDef);
  }

  @Test
  public void testDocValuesFormat_default() {
    IndexableFieldDef mockFieldDef = mock(IndexableFieldDef.class);
    when(mockFieldDef.getDocValuesFormat()).thenReturn(null);
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(serverCodec.getDocValuesFormatForField("field") instanceof Lucene90DocValuesFormat);
    verify(mockFieldDef, times(1)).getDocValuesFormat();
    verifyNoMoreInteractions(mockFieldDef);
  }

  @Test
  public void testDocValuesFormat_provided() {
    IndexableFieldDef mockFieldDef = mock(IndexableFieldDef.class);
    when(mockFieldDef.getDocValuesFormat()).thenReturn(new Lucene80DocValuesFormat());
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(serverCodec.getDocValuesFormatForField("field") instanceof Lucene80DocValuesFormat);
    verify(mockFieldDef, times(1)).getDocValuesFormat();
    verifyNoMoreInteractions(mockFieldDef);
  }

  @Test
  public void testDocValuesFormat_notIndexable() {
    FieldDef mockFieldDef = mock(FieldDef.class);
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    try {
      serverCodec.getDocValuesFormatForField("field");
      fail();
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Field \"field\" is not indexable"));
    }
    verifyNoInteractions(mockFieldDef);
  }

  @Test
  public void testDocValuesFormat_internalField() {
    FieldDef mockFieldDef = mock(FieldDef.class);
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(
        serverCodec.getDocValuesFormatForField("internal_field")
            instanceof Lucene90DocValuesFormat);
    verifyNoInteractions(mockFieldDef);
  }

  @Test
  public void testKnnVectorsFormat_default() {
    VectorFieldDef mockFieldDef = mock(VectorFieldDef.class);
    when(mockFieldDef.getVectorsFormat()).thenReturn(null);
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(
        serverCodec.getKnnVectorsFormatForField("field") instanceof Lucene99HnswVectorsFormat);
    verify(mockFieldDef, times(1)).getVectorsFormat();
    verifyNoMoreInteractions(mockFieldDef);
  }

  @Test
  public void testKnnVectorsFormat_provided() {
    VectorFieldDef mockFieldDef = mock(VectorFieldDef.class);
    when(mockFieldDef.getVectorsFormat()).thenReturn(new Lucene90HnswVectorsFormat());
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(
        serverCodec.getKnnVectorsFormatForField("field") instanceof Lucene90HnswVectorsFormat);
    verify(mockFieldDef, times(1)).getVectorsFormat();
    verifyNoMoreInteractions(mockFieldDef);
  }

  @Test
  public void testKnnVectorsFormat_notVectorField() {
    FieldDef mockFieldDef = mock(FieldDef.class);
    IndexStateManager mockStateManager = getManager(mockFieldDef);
    ServerCodec serverCodec = new ServerCodec(mockStateManager);
    assertTrue(
        serverCodec.getKnnVectorsFormatForField("field") instanceof Lucene99HnswVectorsFormat);
    verifyNoInteractions(mockFieldDef);
  }
}
