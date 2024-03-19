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
import java.lang.reflect.InvocationTargetException;
import java.text.MessageFormat;
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

public class SynonymV2GraphFilterFactory extends TokenFilterFactory {
  /** SPI name */
  public static final String NAME = "synonymV2";

  public static final String MAPPINGS = "mappings";
  private static final String LUCENE_ANALYZER_PATH =
      "org.apache.lucene.analysis.standard.{0}Analyzer";
  public static final String SYNONYM_SEPARATOR_PATTERN = "separator_pattern";
  public static final String DEFAULT_SYNONYM_SEPARATOR_PATTERN = "\\s*\\|\\s*";
  public final boolean ignoreCase;
  protected SynonymMap synonymMap;

  public SynonymV2GraphFilterFactory(Map<String, String> args) throws IOException, ParseException {
    super(args);
    String synonymMappings = args.get(MAPPINGS);
    String separatorPattern =
        args.getOrDefault(SYNONYM_SEPARATOR_PATTERN, DEFAULT_SYNONYM_SEPARATOR_PATTERN);

    this.ignoreCase = getBoolean(args, "ignoreCase", false);
    boolean expand = getBoolean(args, "expand", true);
    String parserFormat = args.getOrDefault("parserFormat", "nrtsearch");
    String analyzerName = args.get("analyzerName");

    if (synonymMappings == null) {
      throw new IllegalArgumentException("Synonym mappings must be specified");
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
      analyzer = loadAnalyzer(analyzerName);
    }
    synonymMap =
        loadSynonymsFromString(separatorPattern, synonymMappings, parserFormat, expand, analyzer);
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new SynonymGraphFilter(input, synonymMap, ignoreCase);
  }

  public SynonymMap loadSynonymsFromString(
      String separatorPattern,
      String synonymMappings,
      String parserFormat,
      boolean expand,
      Analyzer analyzer)
      throws IOException, ParseException {
    SynonymMap.Parser parser;

    if (parserFormat.equals("nrtsearch")) {
      parser = new NrtsearchSynonymParser(separatorPattern, true, expand, analyzer);
    } else {
      throw new IllegalArgumentException(
          "The parser format: " + parserFormat + " is not valid. It should be nrtsearch");
    }
    parser.parse(new StringReader(synonymMappings));
    return parser.build();
  }

  private Analyzer loadAnalyzer(String analyzerName) {
    Analyzer analyzer;
    String analyzerClassName = MessageFormat.format(LUCENE_ANALYZER_PATH, analyzerName);
    try {
      analyzer =
          (Analyzer)
              Analyzer.class
                  .getClassLoader()
                  .loadClass(analyzerClassName)
                  .getDeclaredConstructor()
                  .newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | NoSuchMethodException
        | ClassNotFoundException
        | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    return analyzer;
  }
}
