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
package com.yelp.nrtsearch.server.luceneserver.field;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

public class BooleanFieldDefTest {

  @Test
  public void testParseTrue() {
    assertTrue(BooleanFieldDefTerm.parseBooleanOrThrow("true"));
    assertTrue(BooleanFieldDefTerm.parseBooleanOrThrow("True"));
    assertTrue(BooleanFieldDefTerm.parseBooleanOrThrow("TRUE"));
    assertTrue(BooleanFieldDefTerm.parseBooleanOrThrow("truE"));
  }

  @Test
  public void testParseFalse() {
    assertFalse(BooleanFieldDefTerm.parseBooleanOrThrow("false"));
    assertFalse(BooleanFieldDefTerm.parseBooleanOrThrow("False"));
    assertFalse(BooleanFieldDefTerm.parseBooleanOrThrow("FALSE"));
    assertFalse(BooleanFieldDefTerm.parseBooleanOrThrow("falsE"));
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
      BooleanFieldDefTerm.parseBooleanOrThrow(booleanStr);
      Assert.fail();
    } catch (IllegalArgumentException ignored) {

    }
  }
}
