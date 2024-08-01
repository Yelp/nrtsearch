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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertEquals;

import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.TextDocValuesType;
import com.yelp.nrtsearch.server.luceneserver.similarity.SimilarityCreator;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexOptions;
import org.junit.BeforeClass;
import org.junit.Test;

public class AtomFieldDefTest {

  @BeforeClass
  public static void init() {
    String configStr = "node: node1";
    LuceneServerConfiguration configuration =
        new LuceneServerConfiguration(new ByteArrayInputStream(configStr.getBytes()));
    SimilarityCreator.initialize(configuration, Collections.emptyList());
  }

  private AtomFieldDef createFieldDef(Field field) {
    return new AtomFieldDef("test_field", field);
  }

  @Test
  public void testGetType() {
    AtomFieldDef fieldDef = createFieldDef(Field.newBuilder().build());
    assertEquals("ATOM", fieldDef.getType());
  }

  @Test
  public void testIndexOptions_notSearchable() {
    AtomFieldDef fieldDef = createFieldDef(Field.newBuilder().setSearch(false).build());
    assertEquals(IndexOptions.NONE, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_default() {
    AtomFieldDef fieldDef = createFieldDef(Field.newBuilder().setSearch(true).build());
    assertEquals(IndexOptions.DOCS, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_docs() {
    AtomFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setSearch(true)
                .setIndexOptions(com.yelp.nrtsearch.server.grpc.IndexOptions.DOCS)
                .build());
    assertEquals(IndexOptions.DOCS, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_docsFreq() {
    AtomFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setSearch(true)
                .setIndexOptions(com.yelp.nrtsearch.server.grpc.IndexOptions.DOCS_FREQS)
                .build());
    assertEquals(IndexOptions.DOCS_AND_FREQS, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_docsFreqPos() {
    AtomFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setSearch(true)
                .setIndexOptions(com.yelp.nrtsearch.server.grpc.IndexOptions.DOCS_FREQS_POSITIONS)
                .build());
    assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_docsFreqPosOff() {
    AtomFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setSearch(true)
                .setIndexOptions(
                    com.yelp.nrtsearch.server.grpc.IndexOptions.DOCS_FREQS_POSITIONS_OFFSETS)
                .build());
    assertEquals(
        IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
        fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testDocValueType_none() {
    AtomFieldDef fieldDef = createFieldDef(Field.newBuilder().setStoreDocValues(false).build());
    assertEquals(DocValuesType.NONE, fieldDef.getDocValuesType());
  }

  @Test
  public void testDocValueType_default() {
    AtomFieldDef fieldDef = createFieldDef(Field.newBuilder().setStoreDocValues(true).build());
    assertEquals(DocValuesType.SORTED, fieldDef.getDocValuesType());
  }

  @Test
  public void testDocValueType_sorted() {
    AtomFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setStoreDocValues(true)
                .setTextDocValuesType(TextDocValuesType.TEXT_DOC_VALUES_TYPE_SORTED)
                .build());
    assertEquals(DocValuesType.SORTED, fieldDef.getDocValuesType());
  }

  @Test
  public void testDocValueType_binary() {
    AtomFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setStoreDocValues(true)
                .setTextDocValuesType(TextDocValuesType.TEXT_DOC_VALUES_TYPE_BINARY)
                .build());
    assertEquals(DocValuesType.BINARY, fieldDef.getDocValuesType());
  }
}
