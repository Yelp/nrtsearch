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
package com.yelp.nrtsearch.server.analysis;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.junit.Assert;
import org.junit.Test;

public class NrtsearchSynonymParserTest {
  public final String DEFAULT_SEPARATOR_PATTERN = "\\s*\\|\\s*";

  @Test
  public void testParse() throws IOException, ParseException {
    Analyzer analyzer = new WhitespaceAnalyzer();
    NrtsearchSynonymParser parser =
        new NrtsearchSynonymParser(DEFAULT_SEPARATOR_PATTERN, Boolean.TRUE, Boolean.TRUE, analyzer);
    String synonyms =
        "a         , b|ix,pie-ix|plaza, pla|plaza, plz|str,         strada      |str, strasse|str, straße|village ,vlg";
    parser.parse(new StringReader(synonyms));
    final SynonymMap map = parser.build();
    analyzer.close();
    analyzer = getAnalyzer(map);

    assertAnalyzesTo(analyzer, "a", new String[] {"b", "a"}, new int[] {1, 0});
    assertAnalyzesTo(analyzer, "pie-ix", new String[] {"ix", "pie-ix"}, new int[] {1, 0, 1});
    assertAnalyzesTo(analyzer, "plaza", new String[] {"pla", "plz", "plaza"}, new int[] {1, 0, 0});
    assertAnalyzesTo(
        analyzer,
        "str",
        new String[] {"strada", "strasse", "straße", "str"},
        new int[] {1, 0, 0, 0});

    assertAnalyzesTo(analyzer, "vlg", new String[] {"village", "vlg"}, new int[] {1, 0});
    analyzer.close();
  }

  @Test
  public void testParseDedupFalse() throws IOException, ParseException {
    Analyzer analyzer = new WhitespaceAnalyzer();
    NrtsearchSynonymParser parser =
        new NrtsearchSynonymParser(
            DEFAULT_SEPARATOR_PATTERN, Boolean.FALSE, Boolean.TRUE, analyzer);
    String synonyms = "a         , b|a,b";
    parser.parse(new StringReader(synonyms));
    final SynonymMap map = parser.build();
    analyzer.close();
    analyzer = getAnalyzer(map);
    assertAnalyzesTo(analyzer, "a", new String[] {"b", "b", "a"}, new int[] {1, 0, 0});
    analyzer.close();
  }

  @Test
  public void testParseExpandFalse() throws IOException, ParseException {
    Analyzer analyzer = new WhitespaceAnalyzer();
    NrtsearchSynonymParser parser =
        new NrtsearchSynonymParser(
            DEFAULT_SEPARATOR_PATTERN, Boolean.TRUE, Boolean.FALSE, analyzer);
    String synonyms = "a         , b";
    parser.parse(new StringReader(synonyms));
    final SynonymMap map = parser.build();
    analyzer.close();
    analyzer = getAnalyzer(map);
    assertAnalyzesTo(analyzer, "a", new String[] {"a"}, new int[] {1});
    assertAnalyzesTo(analyzer, "b", new String[] {"a"}, new int[] {1});
    analyzer.close();
  }

  @Test
  public void testInvalidMappings() {
    Analyzer analyzer = new WhitespaceAnalyzer();
    NrtsearchSynonymParser parser =
        new NrtsearchSynonymParser(DEFAULT_SEPARATOR_PATTERN, Boolean.TRUE, Boolean.TRUE, analyzer);
    String synonyms = "a, b, c, d, e";
    Assert.assertThrows(
        IllegalArgumentException.class,
        () -> {
          parser.parse(new StringReader(synonyms));
        });
    analyzer.close();
  }

  @Test
  public void testParseCustomSeparator() throws IOException, ParseException {
    Analyzer analyzer = new WhitespaceAnalyzer();
    NrtsearchSynonymParser parser =
        new NrtsearchSynonymParser("\\s*\\$\\s*", Boolean.TRUE, Boolean.TRUE, analyzer);
    String synonyms = "a         , b$ix,pie-ix";
    parser.parse(new StringReader(synonyms));
    final SynonymMap map = parser.build();
    analyzer.close();
    analyzer = getAnalyzer(map);

    assertAnalyzesTo(analyzer, "a", new String[] {"b", "a"}, new int[] {1, 0});
    assertAnalyzesTo(analyzer, "pie-ix", new String[] {"ix", "pie-ix"}, new int[] {1, 0, 1});
    analyzer.close();
  }

  @Test
  public void testParseUnescape() throws IOException, ParseException {
    Analyzer analyzer = new WhitespaceAnalyzer();
    NrtsearchSynonymParser parser =
        new NrtsearchSynonymParser(DEFAULT_SEPARATOR_PATTERN, Boolean.TRUE, Boolean.TRUE, analyzer);
    String synonyms = "a         , \\b";
    parser.parse(new StringReader(synonyms));
    final SynonymMap map = parser.build();
    analyzer.close();
    analyzer = getAnalyzer(map);
    assertAnalyzesTo(analyzer, "a", new String[] {"b", "a"}, new int[] {1, 0});
    analyzer.close();
  }

  private Analyzer getAnalyzer(SynonymMap map) {
    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer tokenizer = new WhitespaceTokenizer();
        return new TokenStreamComponents(tokenizer, new SynonymGraphFilter(tokenizer, map, true));
      }
    };
  }

  private static void assertAnalyzesTo(
      Analyzer analyzer, String input, String[] expectedTerms, int[] expectedPositions)
      throws IOException {
    try (TokenStream ts = analyzer.tokenStream("", input)) {
      CharTermAttribute termAttr = ts.addAttribute(CharTermAttribute.class);
      PositionIncrementAttribute posAttr = ts.addAttribute(PositionIncrementAttribute.class);
      ts.reset();
      List<String> actualTerms = new ArrayList<>();
      List<Integer> actualPositions = new ArrayList<>();
      while (ts.incrementToken()) {
        actualTerms.add(termAttr.toString());
        actualPositions.add(posAttr.getPositionIncrement());
      }
      ts.end();
      Assert.assertArrayEquals(
          "Terms mismatch for input: " + input, expectedTerms, actualTerms.toArray(new String[0]));
      int[] actualPosArray = new int[actualPositions.size()];
      for (int i = 0; i < actualPositions.size(); i++) {
        actualPosArray[i] = actualPositions.get(i);
      }
      Assert.assertArrayEquals(
          "Position increments mismatch for input: " + input, expectedPositions, actualPosArray);
    }
  }
}
