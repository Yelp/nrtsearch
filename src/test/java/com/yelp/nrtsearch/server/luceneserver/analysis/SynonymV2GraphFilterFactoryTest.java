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

import com.carrotsearch.randomizedtesting.RandomizedRunner;
import com.yelp.nrtsearch.server.config.LuceneServerConfiguration;
import com.yelp.nrtsearch.server.plugins.Plugin;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.standard.ClassicAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.LuceneTestCase;
import org.junit.Before;
import org.junit.runner.RunWith;

@RunWith(RandomizedRunner.class)
public class SynonymV2GraphFilterFactoryTest extends LuceneTestCase {

  private static final String STANDARD_ANALYZER = "standard";

  @Before
  public void init() {
    init(Collections.emptyList());
  }

  private void init(List<Plugin> plugins) {
    AnalyzerCreator.initialize(getEmptyConfig(), plugins);
  }

  private LuceneServerConfiguration getEmptyConfig() {
    String config = "nodeName: \"lucene_server_foo\"";
    return new LuceneServerConfiguration(new ByteArrayInputStream(config.getBytes()));
  }

  public void testNoSynonymMappings() throws IOException, ParseException {
    try {
      new SynonymV2GraphFilterFactory(new HashMap<>());
      fail();
    } catch (IllegalArgumentException e) {
      assertEquals("Synonym mappings must be specified", e.getMessage());
    }
  }

  public void testNoSynonymsReturnsInputTokenStream() throws IOException, ParseException {
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory = getFactory("a,b");
    TokenStream tokenStream =
        new StandardAnalyzer().tokenStream("field", new StringReader("this is test string"));
    String[] expectedTokens = {"this", "is", "test", "string"};
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  public void testSingleMappingWithDefaultAnalyzer() throws IOException, ParseException {
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory = getFactory("a, b");
    TokenStream tokenStream =
        new StandardAnalyzer().tokenStream("field", new StringReader("this is a test string"));
    String[] expectedTokens = {"this", "is", "b", "a", "test", "string"};
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  public void testMultipleMappingsWithDefaultAnalyzer() throws IOException, ParseException {
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory =
        getFactory("#, ste|a, b|c/, calle|plaza, plaça|p.o, po| v, väg");
    TokenStream tokenStream =
        new StandardAnalyzer()
            .tokenStream("field", new StringReader("# a b calle plaça p.o väg v"));
    String[] expectedTokens = {
      "b", "a", "a", "b", "c/", "calle", "plaza", "plaça", "po", "p.o", "v", "väg", "väg", "v"
    };
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  public void testMultipleMappingsWithStandardAnalyzer() throws IOException, ParseException {
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory =
        getFactory("a, b|c/, calle|plaza, plaça|p.o, po| v, väg", STANDARD_ANALYZER);
    TokenStream tokenStream =
        new StandardAnalyzer().tokenStream("field", new StringReader("a b calle plaça p.o väg v"));
    String[] expectedTokens = {
      "b", "a", "a", "b", "c", "calle", "plaza", "plaça", "po", "p.o", "v", "väg", "väg", "v"
    };
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  public void testMultipleMappingsWithClassicAnalyzer() throws IOException, ParseException {
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory =
        getFactory("c/, calle|plaza, plaça", "classic");
    TokenStream tokenStream =
        new ClassicAnalyzer()
            .tokenStream("field", new StringReader("this is a test for calle and plaça"));
    String[] expectedTokens = {"test", "c", "calle", "plaza", "plaça"};
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  public void testInvalidAnalyzer() throws IOException, ParseException {
    try {
      Map<String, String> params = new HashMap<>();
      params.put(SynonymV2GraphFilterFactory.SYNONYMS, "a, b");
      params.put("analyzerName", "invalid");
      new SynonymV2GraphFilterFactory(params);
      fail();
    } catch (RuntimeException e) {
      assertEquals("Unable to find predefined analyzer: invalid", e.getMessage());
    }
  }

  public void testNrtsearchParserFormat() throws IOException, ParseException {
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory =
        getFactory("a, b", STANDARD_ANALYZER, "nrtsearch");
    TokenStream tokenStream =
        new StandardAnalyzer().tokenStream("field", new StringReader("this is a test string"));
    String[] expectedTokens = {"this", "is", "b", "a", "test", "string"};
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  public void testInvalidParserFormat() throws IOException, ParseException {
    try {
      Map<String, String> params = new HashMap<>();
      params.put(SynonymV2GraphFilterFactory.SYNONYMS, "a, b");
      params.put("parserFormat", "invalid");
      new SynonymV2GraphFilterFactory(params);
      fail();
    } catch (RuntimeException e) {
      assertEquals(
          "The parser format: invalid is not valid. It should be nrtsearch", e.getMessage());
    }
  }

  public void testSingleMappingWithExpandFalse() throws IOException, ParseException {
    Map<String, String> params = new HashMap<>();
    params.put(SynonymV2GraphFilterFactory.SYNONYMS, "a, b");
    params.put("expand", "false");
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory =
        new SynonymV2GraphFilterFactory(params);
    TokenStream tokenStream =
        new StandardAnalyzer().tokenStream("field", new StringReader("this is a test string"));
    String[] expectedTokens = {"this", "is", "a", "test", "string"};
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  public void testMultipleMappingsWithCustomSeparator() throws IOException, ParseException {
    Map<String, String> params = new HashMap<>();
    params.put(
        SynonymV2GraphFilterFactory.SYNONYMS,
        "#, ste=>a, b=>c/, calle=>plaza, plaça=>p.o, po=> v, väg");
    params.put("separator_pattern", "\\s*\\=>\\s*");
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory =
        new SynonymV2GraphFilterFactory(params);
    TokenStream tokenStream =
        new StandardAnalyzer()
            .tokenStream("field", new StringReader("# a b calle plaça p.o väg v"));
    String[] expectedTokens = {
      "b", "a", "a", "b", "c/", "calle", "plaza", "plaça", "po", "p.o", "v", "väg", "väg", "v"
    };
    assertTokenStream(synonymV2GraphFilterFactory, tokenStream, expectedTokens);
  }

  public void testSingleMappingIgnoreCase() throws IOException, ParseException {
    Map<String, String> params = new HashMap<>();
    params.put(SynonymV2GraphFilterFactory.SYNONYMS, "A, B");
    params.put("ignoreCase", "true");
    SynonymV2GraphFilterFactory synonymV2GraphFilterFactory =
        new SynonymV2GraphFilterFactory(params);
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

  private SynonymV2GraphFilterFactory getFactory(String synonyms)
      throws IOException, ParseException {
    Map<String, String> params = new HashMap<>();
    params.put(SynonymV2GraphFilterFactory.SYNONYMS, synonyms);
    return new SynonymV2GraphFilterFactory(params);
  }

  private SynonymV2GraphFilterFactory getFactory(String synonyms, String analyzerName)
      throws IOException, ParseException {
    Map<String, String> params = new HashMap<>();
    params.put(SynonymV2GraphFilterFactory.SYNONYMS, synonyms);
    params.put("analyzerName", analyzerName);
    return new SynonymV2GraphFilterFactory(params);
  }

  private SynonymV2GraphFilterFactory getFactory(
      String synonyms, String analyzerName, String parserFormat)
      throws IOException, ParseException {
    Map<String, String> params = new HashMap<>();
    params.put(SynonymV2GraphFilterFactory.SYNONYMS, synonyms);
    params.put("analyzerName", analyzerName);
    params.put("parserFormat", parserFormat);
    return new SynonymV2GraphFilterFactory(params);
  }
}
