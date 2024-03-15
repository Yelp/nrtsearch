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
package com.yelp.nrtsearch.server.luceneserver.analysis;

import static org.apache.lucene.util.LuceneTestCase.random;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.runner.RunWith;

@RunWith(RandomizedRunner.class)
public class SynonymV2GraphFilterFactoryTest extends LuceneTestCase {
  public void testNoMappings() throws IOException, ParseException {
    Analyzer analyzer = new MockAnalyzer(random());
    try {
      new SynonymV2GraphFilterFactory(new HashMap<>(), analyzer);
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Synonym mappings must be specified", e.getMessage());
    }
  }

  public void testSingleMapping() throws IOException, ParseException {
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory = getFactory("a, b");
    TokenStream tokenStream =
        new StandardAnalyzer().tokenStream("field", new StringReader("this is a test string"));
    String[] expectedTokens = {"this", "is", "b", "a", "test", "string"};
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  private static void assertTokenStream(
      SynonymV2GraphFilterFactory synonymV2GraphFilterFactory,
      TokenStream tokenStream,
      String[] expectedTokens) {
    try {
      TokenStream output = synonymV2GraphFilterFactory.create(tokenStream);
      CharTermAttribute charTermAtt = output.addAttribute(CharTermAttribute.class);
      int i = 0;
      output.reset();
      while (output.incrementToken()) {
        assertEquals(expectedTokens[i], charTermAtt.toString());
        i += 1;
      }
      output.end();
      output.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private SynonymV2GraphFilterFactory getFactory(String mappings)
      throws IOException, ParseException {
    Analyzer analyzer = new MockAnalyzer(random());
    Map<String, String> params = new HashMap<>();
    params.put(SynonymV2GraphFilterFactory.MAPPINGS, mappings);
    params.put("parserFormat", "nrtsearch");
    return new SynonymV2GraphFilterFactory(params, analyzer);
  }
}
