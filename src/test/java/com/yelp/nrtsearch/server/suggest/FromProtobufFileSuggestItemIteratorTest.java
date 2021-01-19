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
package com.yelp.nrtsearch.server.suggest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.io.Resources;
import com.yelp.nrtsearch.server.luceneserver.suggest.iterator.FromProtobufFileSuggestItemIterator;
import java.io.File;
import java.util.Set;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Test;

public class FromProtobufFileSuggestItemIteratorTest {

  private File indexFile;

  @Before
  public void setUp() throws Exception {
    this.indexFile = new File(Resources.getResource("suggest/test_index.file").toURI());
  }

  @Test
  public void testIteratorWithContextWithPayload() throws Exception {
    FromProtobufFileSuggestItemIterator iterator =
        new FromProtobufFileSuggestItemIterator(indexFile, true, true, true);
    assertEquals("1", iterator.next().utf8ToString());
    assertEquals(10, iterator.weight());
    assertExpectedSet(Set.of("en_CL"), iterator.contexts());
    assertExpectedSet(Set.of("post offices", "offices"), iterator.searchTexts());
    assertNotNull(iterator.payload());

    assertEquals("2", iterator.next().utf8ToString());
    assertEquals(20, iterator.weight());
    assertExpectedSet(Set.of("en_US"), iterator.contexts());
    assertExpectedSet(Set.of("hog island", "island"), iterator.searchTexts());
    assertNotNull(iterator.payload());

    // Reach the end of the source file
    assertNull(iterator.next());
    assertEquals(2, iterator.suggestCount);
  }

  @Test
  public void testIteratorWithoutContextWithoutPayload() throws Exception {
    FromProtobufFileSuggestItemIterator iterator =
        new FromProtobufFileSuggestItemIterator(indexFile, false, false, true);
    assertEquals("1", iterator.next().utf8ToString());
    assertEquals(10, iterator.weight());
    assertExpectedSet(Set.of("post offices", "offices"), iterator.searchTexts());
    assertNull(iterator.contexts());
    assertNull(iterator.payload());

    assertEquals("2", iterator.next().utf8ToString());
    assertEquals(20, iterator.weight());
    assertExpectedSet(Set.of("hog island", "island"), iterator.searchTexts());
    assertNull(iterator.contexts());
    assertNull(iterator.payload());

    // Reach the end of the source file
    assertNull(iterator.next());
    assertEquals(2, iterator.suggestCount);
  }

  private void assertExpectedSet(Set<String> expectedSet, Set<BytesRef> actualSet) {
    assertNotNull(actualSet);
    assertEquals(expectedSet.size(), actualSet.size());
    for (BytesRef bytes : actualSet) {
      assertTrue(expectedSet.contains(bytes.utf8ToString()));
    }
  }
}
