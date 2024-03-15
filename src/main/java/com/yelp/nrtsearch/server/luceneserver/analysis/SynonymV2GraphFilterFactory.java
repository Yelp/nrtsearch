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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.analysis.synonym.WordnetSynonymParser;
import org.apache.lucene.analysis.util.TokenFilterFactory;

public class SynonymV2GraphFilterFactory extends TokenFilterFactory {
  public static final String MAPPINGS = "mappings";
  public final boolean ignoreCase;
  protected SynonymMap synonymMap;

  public SynonymV2GraphFilterFactory(Map<String, String> args, Analyzer analyzer)
      throws IOException, ParseException {
    super(args);
    this.ignoreCase = getBoolean(args, "ignoreCase", false);
    boolean expand = getBoolean(args, "expand", true);
    String parserFormat = args.get("parserFormat");
    String synonymMappings = args.get(MAPPINGS);
    if (synonymMappings == null) {
      throw new IllegalArgumentException("Synonym mappings must be specified");
    }
    if (parserFormat == null) {
      throw new IllegalArgumentException("Parser format must be specified");
    }
    synonymMap = loadSynonymsFromString(synonymMappings, parserFormat, expand, analyzer);
  }

  @Override
  public TokenStream create(TokenStream input) {
    return new SynonymGraphFilter(input, synonymMap, ignoreCase);
  }

  public SynonymMap loadSynonymsFromString(
      String synonymMappings, String parserFormat, boolean expand, Analyzer analyzer)
      throws IOException, ParseException {
    SynonymMap.Parser parser;

    if (parserFormat.equals("solr")) {
      parser = new SolrSynonymParser(true, expand, analyzer);
    } else if (parserFormat.equals("wordnet")) {
      parser = new WordnetSynonymParser(true, expand, analyzer);
    } else if (parserFormat.equals("nrtsearch")) {
      parser = new NrtsearchSynonymParser(true, expand, analyzer);
    } else {
      throw new IllegalArgumentException(
          "The parser format: "
              + parserFormat
              + " is not valid. It should be solr, wordnet or nrtsearch");
    }
    parser.parse(new StringReader(synonymMappings));
    return parser.build();
  }
}
