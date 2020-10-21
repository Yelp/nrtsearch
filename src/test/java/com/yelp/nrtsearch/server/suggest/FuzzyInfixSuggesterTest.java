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

import com.google.common.io.Resources;
import com.yelp.nrtsearch.server.luceneserver.suggest.CompletionInfixSuggester;
import com.yelp.nrtsearch.server.luceneserver.suggest.FromFileSuggestItemIterator;
import com.yelp.nrtsearch.server.luceneserver.suggest.FuzzyInfixSuggester;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.search.suggest.Lookup;
import org.apache.lucene.search.suggest.Lookup.LookupResult;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FuzzyInfixSuggesterTest extends LuceneTestCase {

  private FuzzyInfixSuggester suggester;

  @Before
  public void before() throws Exception {
    Directory dir = newDirectory();
    FromFileSuggestItemIterator iter = createIterator("suggest/fuzzy_suggest_item.file");
    Analyzer analyzer = new StandardAnalyzer();
    // Setup fuzzy infix suggester with default values
    suggester = new FuzzyInfixSuggester(dir, analyzer);
    suggester.build(iter);
  }

  @After
  public void after() throws Exception {
    suggester.close();
  }

  @Test
  public void testCompletionSuggestionWithLongPrefixWithoutContext() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "hom", Set.of(), 9);

    // "iom decoration" text is not returned since non_fuzzy_prefix is 1.
    assertNotNull(actualResults);
    assertEquals(4, actualResults.size());
    assertEquals("hime decoration", actualResults.get(0).key);
    assertEquals("lowe's hot spring", actualResults.get(1).key);
    assertEquals("home decoration", actualResults.get(2).key);
    assertEquals("hot depot", actualResults.get(3).key);
  }

  @Test
  public void testCompletionSuggestionWithShortPrefixWithoutContext() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "hi", Set.of(), 9);

    // No fuzzy match is returned since the length of prefix is less than non_fuzzy_prefix (3 in
    // this case).
    assertNotNull(actualResults);
    assertEquals(1, actualResults.size());
    assertEquals("hime decoration", actualResults.get(0).key);
  }

  @Test
  public void testCompletionSuggestionWithLongPrefixWithContext() throws IOException {
    List<LookupResult> actualResults = lookupHelper(suggester, "hom", Set.of("9q9hfe"), 9);

    // "iom decoration" text is not returned since non_fuzzy_prefix is 1.
    assertNotNull(actualResults);
    assertEquals(2, actualResults.size());
    assertEquals("lowe's hot spring", actualResults.get(0).key);
    assertEquals("hot depot", actualResults.get(1).key);
  }

  @Test(expected = RuntimeException.class)
  public void testSuggesterLookupWithoutValidIndexBuild() throws IOException {
    Directory dir = newDirectory();
    Analyzer analyzer = new StandardAnalyzer();
    CompletionInfixSuggester testSuggester = new CompletionInfixSuggester(dir, analyzer);
    try {
      lookupHelper(testSuggester, "sha", Set.of("9q9hf"), 2);
    } finally {
      testSuggester.close();
    }
  }

  private List<LookupResult> lookupHelper(
      Lookup suggester, String key, Set<String> contexts, int count) throws IOException {
    Set<BytesRef> contextSet = new HashSet<>();
    for (String context : contexts) {
      contextSet.add(new BytesRef(context));
    }

    return suggester.lookup(key, contextSet, true, count);
  }

  private FromFileSuggestItemIterator createIterator(String pathString)
      throws IOException, URISyntaxException {
    File suggestItemFile = new File(Resources.getResource(pathString).toURI());
    return new FromFileSuggestItemIterator(suggestItemFile, true, true);
  }
}
