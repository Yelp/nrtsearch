/*
 * Copyright 2025 Yelp Inc.
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

import com.yelp.nrtsearch.server.ServerTestCase;
import com.yelp.nrtsearch.server.grpc.Field;
import com.yelp.nrtsearch.server.grpc.IndexPrefixes;
import com.yelp.nrtsearch.server.grpc.PrefixQuery;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.junit.Test;

public class PrefixFieldTest extends ServerTestCase {

  private static final String TEST_FIELD = "test_field";

  @Test
  public void testTextFieldWithPrefixConstruction() {
    Field field =
        Field.newBuilder()
            .setName(TEST_FIELD)
            .setSearch(true)
            .setIndexPrefixes(IndexPrefixes.newBuilder().setMinChars(3).setMaxChars(10).build())
            .build();
    TextFieldDef textFieldDef =
        new TextFieldDef(TEST_FIELD, field, new FieldDefCreator.FieldDefCreatorContext(null));

    assertTrue(textFieldDef.hasPrefix());
    PrefixFieldDef prefixFieldDef = textFieldDef.getPrefixFieldDef();
    assertEquals(3, prefixFieldDef.getMinChars());
    assertEquals(10, prefixFieldDef.getMaxChars());
    assertEquals(TEST_FIELD + "._index_prefix", prefixFieldDef.getName());
  }

  @Test
  public void testPrefixQueryGeneration() {
    Field field =
        Field.newBuilder()
            .setName(TEST_FIELD)
            .setSearch(true)
            .setIndexPrefixes(IndexPrefixes.newBuilder().setMinChars(3).build())
            .build();
    TextFieldDef textFieldDef =
        new TextFieldDef(TEST_FIELD, field, new FieldDefCreator.FieldDefCreatorContext(null));

    PrefixQuery prefixQuery = PrefixQuery.newBuilder().setPrefix("te").build();
    Query query = textFieldDef.getPrefixFieldDef().getPrefixQuery(prefixQuery);

    assertTrue(query instanceof BooleanQuery);
  }
}
