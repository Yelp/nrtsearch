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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.yelp.nrtsearch.server.luceneserver.suggest.FromFileSuggestItemIterator;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FromFileSuggestItemIteratorTest {

  /**
   * This rule ensure the temporary folder which maintains indexes are cleaned up after each test
   */
  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  private static Path tempFile;

  @Before
  public void setUp() throws IOException {
    Path tempDir = folder.newFolder("TestSuggest").toPath();
    tempFile = tempDir.resolve("suggest.in");
  }

  @Test
  public void testSuggestItemWithoutContextsWithoutPayload() throws IOException {
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("home depot\u001fhome depot\u001edepot\u001f1\n");
    out.write(
        "gary danko restaurant\u001fgary danko restaurant\u001edanko restaurant\u001erestaurant\u001f2\n");
    out.write("restaurant\u001frestaurant\u001f3\n");
    out.close();

    List<String> expectedTexts = List.of("home depot", "gary danko restaurant", "restaurant");
    List<Long> expectedWeights = List.of(1L, 2L, 3L);
    List<List<String>> expectedSearchTexts =
        List.of(
            List.of("home depot", "depot"),
            List.of("gary danko restaurant", "danko restaurant", "restaurant"),
            List.of("restaurant"));

    FromFileSuggestItemIterator iterator =
        new FromFileSuggestItemIterator(tempFile.toFile(), false, false);
    int itemIndex = 0;
    for (BytesRef text; (text = iterator.next()) != null; ) {
      assertTrue(itemIndex < expectedTexts.size());

      // Test text is equal
      assertEquals(expectedTexts.get(itemIndex), text.utf8ToString());

      // Test weight is equal
      long expectedWeight = expectedWeights.get(itemIndex);
      long actualWeight = iterator.weight();
      assertEquals(expectedWeight, actualWeight);

      // Test search texts are equal
      assertEquals(expectedSearchTexts.get(itemIndex).size(), iterator.searchTexts().size());
      Set<String> searchTextStringSet = new HashSet<>();
      iterator
          .searchTexts()
          .forEach(searchText -> searchTextStringSet.add(searchText.utf8ToString()));
      expectedSearchTexts
          .get(itemIndex)
          .forEach(searchText -> assertTrue(searchTextStringSet.contains(searchText)));

      // payload and contexts are null
      assertNull(iterator.contexts());
      assertNull(iterator.payload());

      itemIndex++;
    }
  }

  @Test
  public void testSuggestItemWithContextsWithPayload() throws IOException {
    Writer fstream = new OutputStreamWriter(new FileOutputStream(tempFile.toFile()), "UTF-8");
    BufferedWriter out = new BufferedWriter(fstream);
    out.write("home depot\u001fhome depot\u001edepot\u001f1\u001fpayload1\u001fc1\u001ec2\n");
    out.write(
        "gary danko restaurant\u001fgary danko restaurant\u001edanko restaurant\u001erestaurant\u001f2\u001fpayload2\u001fc1\n");
    out.close();

    List<String> expectedTexts = List.of("home depot", "gary danko restaurant");
    List<Long> expectedWeights = List.of(1L, 2L);
    List<List<String>> expectedSearchTexts =
        List.of(
            List.of("home depot", "depot"),
            List.of("gary danko restaurant", "danko restaurant", "restaurant"));
    List<List<String>> expectedContexts = List.of(List.of("c1", "c2"), List.of("c1"));
    List<String> expectedPayload = List.of("payload1", "payload2");

    FromFileSuggestItemIterator iterator =
        new FromFileSuggestItemIterator(tempFile.toFile(), true, true);
    int itemIndex = 0;
    for (BytesRef text; (text = iterator.next()) != null; ) {
      assertTrue(itemIndex < expectedTexts.size());

      // Test text is equal
      assertEquals(expectedTexts.get(itemIndex), text.utf8ToString());

      // Test weight is equal
      long expectedWeight = expectedWeights.get(itemIndex);
      long actualWeight = iterator.weight();
      assertEquals(expectedWeight, actualWeight);

      // Test search texts are equal
      assertEquals(expectedSearchTexts.get(itemIndex).size(), iterator.searchTexts().size());
      Set<String> searchTextStringSet = new HashSet<>();
      iterator
          .searchTexts()
          .forEach(searchText -> searchTextStringSet.add(searchText.utf8ToString()));
      expectedSearchTexts
          .get(itemIndex)
          .forEach(searchText -> assertTrue(searchTextStringSet.contains(searchText)));

      // Test contexts are equals
      assertEquals(expectedContexts.get(itemIndex).size(), iterator.contexts().size());
      Set<String> contextStringSet = new HashSet<>();
      iterator.contexts().forEach(context -> contextStringSet.add(context.utf8ToString()));
      expectedContexts
          .get(itemIndex)
          .forEach(context -> assertTrue(contextStringSet.contains(context)));

      // Test payload are equals
      assertEquals(expectedPayload.get(itemIndex), iterator.payload().utf8ToString());

      itemIndex++;
    }
  }
}
