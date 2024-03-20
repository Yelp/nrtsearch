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

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.util.TokenFilterFactory;

/**
 * Implementation of a {@link TokenFilterFactory} that allows for loading synonym mappings. Unlike
 * the lucene provided {@link org.apache.lucene.analysis.synonym.SynonymGraphFilterFactory}, this
 * one lets you specify the synonyms inline as a parameter (instead of within a file).
 *
 * <p>Synonyms must be specified in the 'synonyms' parameter string. This value is separated into
 * multiple synonym mappings that are comma separated by splitting on a pattern, which defaults to
 * '|'. This pattern may be changed by giving a 'separator_pattern' param.
 *
 * <p>Synonyms must be of the form synonym_from,synonym_to for example:
 *
 * <p>a,b
 *
 * <p>a,b|c,d
 */
public class SynonymV2GraphFilterFactory extends TokenFilterFactory {
  /** SPI name */
  public static final String NAME = "synonymV2";

  public static final String SYNONYMS = "synonyms";
  public static final String SYNONYM_SEPARATOR_PATTERN = "separator_pattern";
  public static final String DEFAULT_SYNONYM_SEPARATOR_PATTERN = "\\s*\\|\\s*";
  public final boolean ignoreCase;
  protected SynonymMap synonymMap;

  public SynonymV2GraphFilterFactory(Map<String, String> args) throws IOException, ParseException {
    super(args);
    String synonymMappings = args.get(SYNONYMS);
    String separatorPattern =
        args.getOrDefault(SYNONYM_SEPARATOR_PATTERN, DEFAULT_SYNONYM_SEPARATOR_PATTERN);

    this.ignoreCase = getBoolean(args, "ignoreCase", false);
    boolean expand = getBoolean(args, "expand", true);
    boolean dedup = getBoolean(args, "dedup", true);
    String parserFormat = args.getOrDefault("parserFormat", "nrtsearch");
    String analyzerName = args.get("analyzerName");

    if (synonymMappings == null) {
      throw new IllegalArgumentException("Synonym mappings must be specified");
    }

    if (!parserFormat.equals("nrtsearch")) {
      throw new IllegalArgumentException(
          "The parser format: " + parserFormat + " is not valid. It should be nrtsearch");
    }

    Analyzer analyzer;
    if (analyzerName == null) {
      analyzer =
          new Analyzer() {
            @Override
            protected TokenStreamComponents createComponents(String fieldName) {
              Tokenizer tokenizer = new WhitespaceTokenizer();
              TokenStream stream = ignoreCase ? new LowerCaseFilter(tokenizer) : tokenizer;
              return new TokenStreamComponents(tokenizer, stream);
            }
          };
    } else {
      analyzer = AnalyzerCreator.getInstance().getAnalyzer(getPredefinedAnalyzer(analyzerName));
    }

    SynonymMap.Parser parser =
        new NrtsearchSynonymParser(separatorPattern, dedup, expand, analyzer);
    parser.parse(new StringReader(synonymMappings));
    synonymMap = parser.build();
  }

  @Override
  public TokenStream create(TokenStream input) {
    return (this.synonymMap.fst == null
        ? input
        : new SynonymGraphFilter(input, synonymMap, ignoreCase));
  }

  private com.yelp.nrtsearch.server.grpc.Analyzer getPredefinedAnalyzer(String analyzerName) {
    return com.yelp.nrtsearch.server.grpc.Analyzer.newBuilder().setPredefined(analyzerName).build();
  }
}
