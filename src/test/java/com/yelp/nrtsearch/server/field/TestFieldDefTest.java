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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.similarity.SimilarityCreator;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import org.apache.lucene.index.IndexOptions;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestFieldDefTest {
  @BeforeClass
  public static void init() {
    String configStr = "node: node1";
    NrtsearchConfig configuration =
        new NrtsearchConfig(new ByteArrayInputStream(configStr.getBytes()));
    SimilarityCreator.initialize(configuration, Collections.emptyList());
  }

  private TextFieldDef createFieldDef(Field field) {
    return new TextFieldDef(
        "test_field", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
  }

  @Test
  public void testGetType() {
    TextFieldDef fieldDef = createFieldDef(Field.newBuilder().build());
    assertEquals("TEXT", fieldDef.getType());
  }

  @Test
  public void testIndexOptions_notSearchable() {
    TextFieldDef fieldDef = createFieldDef(Field.newBuilder().setSearch(false).build());
    assertEquals(IndexOptions.NONE, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_default() {
    TextFieldDef fieldDef = createFieldDef(Field.newBuilder().setSearch(true).build());
    assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_docs() {
    TextFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setSearch(true)
                .setIndexOptions(com.yelp.nrtsearch.server.grpc.IndexOptions.DOCS)
                .build());
    assertEquals(IndexOptions.DOCS, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_docsFreq() {
    TextFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setSearch(true)
                .setIndexOptions(com.yelp.nrtsearch.server.grpc.IndexOptions.DOCS_FREQS)
                .build());
    assertEquals(IndexOptions.DOCS_AND_FREQS, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_docsFreqPos() {
    TextFieldDef fieldDef =
        createFieldDef(
            Field.newBuilder()
                .setSearch(true)
                .setIndexOptions(com.yelp.nrtsearch.server.grpc.IndexOptions.DOCS_FREQS_POSITIONS)
                .build());
    assertEquals(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS, fieldDef.getFieldType().indexOptions());
  }

  @Test
  public void testIndexOptions_docsFreqPosOff() {
    TextFieldDef fieldDef =
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
}
