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
package com.yelp.nrtsearch.server.field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.yelp.nrtsearch.server.grpc.Field;
import org.junit.Assert;
import org.junit.Test;

public class BooleanFieldDefTest {

  private BooleanFieldDef createFieldDef(Field field) {
    return new BooleanFieldDef(
        "test_field", field, mock(FieldDefCreator.FieldDefCreatorContext.class));
  }

  @Test
  public void testParseTrue() {
    assertTrue(BooleanFieldDef.parseBooleanOrThrow("true"));
    assertTrue(BooleanFieldDef.parseBooleanOrThrow("True"));
    assertTrue(BooleanFieldDef.parseBooleanOrThrow("TRUE"));
    assertTrue(BooleanFieldDef.parseBooleanOrThrow("truE"));
  }

  @Test
  public void testParseFalse() {
    assertFalse(BooleanFieldDef.parseBooleanOrThrow("false"));
    assertFalse(BooleanFieldDef.parseBooleanOrThrow("False"));
    assertFalse(BooleanFieldDef.parseBooleanOrThrow("FALSE"));
    assertFalse(BooleanFieldDef.parseBooleanOrThrow("falsE"));
  }

  @Test
  public void testMalformedStrings() {
    assertMalformedString("ttrue");
    assertMalformedString("ffalse");
    assertMalformedString("yes");
    assertMalformedString("no");
    assertMalformedString("0");
    assertMalformedString("1");
    assertMalformedString("");
  }

  private void assertMalformedString(String booleanStr) {
    try {
      BooleanFieldDef.parseBooleanOrThrow(booleanStr);
      Assert.fail();
    } catch (IllegalArgumentException ignored) {

    }
  }

  @Test
  public void testCreateUpdatedFieldDef() {
    BooleanFieldDef fieldDef =
        createFieldDef(Field.newBuilder().setName("field").setStoreDocValues(true).build());
    FieldDef updatedField =
        fieldDef.createUpdatedFieldDef(
            "field",
            Field.newBuilder().setStoreDocValues(false).build(),
            mock(FieldDefCreator.FieldDefCreatorContext.class));
    assertTrue(updatedField instanceof BooleanFieldDef);
    BooleanFieldDef updatedFieldDef = (BooleanFieldDef) updatedField;

    assertNotSame(fieldDef, updatedFieldDef);
    assertEquals("field", updatedFieldDef.getName());
    assertTrue(fieldDef.hasDocValues());
    assertFalse(updatedFieldDef.hasDocValues());
  }
}
