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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.analysis.PrefixWrappedAnalyzer;
import com.yelp.nrtsearch.server.config.NrtsearchConfig;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.IndexPrefixes;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import com.yelp.nrtsearch.server.similarity.SimilarityCreator;
import java.io.ByteArrayInputStream;
import java.util.Collections;
import java.util.Optional;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class PrefixFieldDefTest {

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
  public void testDefaultConfiguration() {
    Field field =
        Field.newBuilder()
            .setSearch(true)
            .setIndexPrefixes(IndexPrefixes.newBuilder().build())
            .build();
    TextFieldDef FieldDef = createFieldDef(field);
    PrefixFieldDef prefixFieldDef = FieldDef.getPrefixFieldDef();
    assertEquals(2, FieldDef.getPrefixFieldDef().getMinChars());
    assertEquals(5, FieldDef.getPrefixFieldDef().getMaxChars());
    assertEquals(IndexOptions.DOCS, prefixFieldDef.fieldType.indexOptions());
    assertTrue(prefixFieldDef.fieldType.omitNorms());
    assertTrue(prefixFieldDef.fieldType.tokenized());
  }

  @Test
  public void testInvalidMinChars() {
    try {
      Field field =
          Field.newBuilder()
              .setSearch(true)
              .setIndexPrefixes(IndexPrefixes.newBuilder().setMinChars(1).setMaxChars(5).build())
              .build();
      createFieldDef(field);
      fail("Should throw IllegalArgumentException for min_chars < 2");
    } catch (IllegalArgumentException e) {
      assertEquals("min_chars [1] must be greater than zero", e.getMessage());
    }
  }

  @Test
  public void testInvalidMaxChars() {
    try {
      Field field =
          Field.newBuilder()
              .setSearch(true)
              .setIndexPrefixes(IndexPrefixes.newBuilder().setMinChars(2).setMaxChars(20).build())
              .build();
      createFieldDef(field);
      fail("Should throw IllegalArgumentException for max_chars >= 20");
    } catch (IllegalArgumentException e) {
      assertEquals("max_chars [20] must be less than 20", e.getMessage());
    }
  }

  @Test
  public void testInvalidMinMaxChars() {
    try {
      Field field =
          Field.newBuilder()
              .setSearch(true)
              .setIndexPrefixes(IndexPrefixes.newBuilder().setMinChars(5).setMaxChars(3).build())
              .build();
      createFieldDef(field);
      fail("Should throw IllegalArgumentException for min_chars > max_chars");
    } catch (IllegalArgumentException e) {
      assertEquals("min_chars [5] must be less than max_chars [3]", e.getMessage());
    }
  }

  @Test
  public void testAcceptLength() {
    Field field =
        Field.newBuilder()
            .setSearch(true)
            .setIndexPrefixes(IndexPrefixes.newBuilder().setMinChars(3).setMaxChars(5).build())
            .build();
    TextFieldDef FieldDef = createFieldDef(field);
    PrefixFieldDef prefixFieldDef = FieldDef.getPrefixFieldDef();
    assertEquals(3, FieldDef.getPrefixFieldDef().getMinChars());
    assertEquals(5, FieldDef.getPrefixFieldDef().getMaxChars());
    assertFalse(prefixFieldDef.accept(0));
    assertFalse(prefixFieldDef.accept(1));
    assertTrue(prefixFieldDef.accept(2));
    assertTrue(prefixFieldDef.accept(5));
    assertFalse(prefixFieldDef.accept(6));
  }

  @Test
  public void testAnalyzer() {
    Field field =
        Field.newBuilder()
            .setSearch(true)
            .setIndexPrefixes(IndexPrefixes.newBuilder().build())
            .build();
    TextFieldDef FieldDef = createFieldDef(field);
    PrefixFieldDef prefixFieldDef = FieldDef.getPrefixFieldDef();
    Optional<Analyzer> analyzer = prefixFieldDef.getIndexAnalyzer();
    assertTrue(analyzer.isPresent());
    assertTrue(analyzer.get() instanceof PrefixWrappedAnalyzer);
  }

  @Test
  public void testPrefixQueryConstantScoreQuery() {
    Field field =
        Field.newBuilder()
            .setSearch(true)
            .setIndexPrefixes(IndexPrefixes.newBuilder().setMinChars(2).setMaxChars(5).build())
            .build();
    TextFieldDef fieldDef = createFieldDef(field);

    PrefixQuery prefixQuery =
        PrefixQuery.newBuilder().setField("test_field").setPrefix("test").build();
    Query query = fieldDef.getPrefixQuery(prefixQuery, null);
    assertNotNull(query);
    assertTrue(query instanceof ConstantScoreQuery);
  }

  @Test
  public void testPrefixQueryBooleanQuery() {
    Field field =
        Field.newBuilder()
            .setSearch(true)
            .setIndexPrefixes(IndexPrefixes.newBuilder().setMinChars(3).setMaxChars(5).build())
            .build();
    TextFieldDef FieldDef = createFieldDef(field);
    PrefixFieldDef prefixFieldDef = FieldDef.getPrefixFieldDef();
    PrefixQuery prefixQuery =
        PrefixQuery.newBuilder().setField("test_field").setPrefix("te").build();
    Query query = prefixFieldDef.getPrefixQuery(prefixQuery);
    assertTrue(query instanceof BooleanQuery);
  }
}
