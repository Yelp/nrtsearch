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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.text.ParseException;
import java.util.ArrayList;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.CharsRefBuilder;

class NrtsearchSynonymParser extends SynonymMap.Parser {
  private final boolean expand;
  private final String synonymsSeparator;
  private static final String SYNONYM_MAPPING_SEPARATOR = ",";

  /**
   * This is a nrtsearch parser that extends SynonymMap.Parser to parse synonyms provided inline in
   * a string instead of a file
   *
   * @param synonymsSeparator pattern used to split the synonym mappings
   * @param dedup set to true to dedup duplicate synonym mappings
   * @param expand set to true to map synonyms both ways
   * @param analyzer analyzer for the synonyms
   */
  public NrtsearchSynonymParser(
      String synonymsSeparator, boolean dedup, boolean expand, Analyzer analyzer) {
    super(dedup, analyzer);
    this.expand = expand;
    this.synonymsSeparator = synonymsSeparator;
  }

  @Override
  public void parse(Reader mappings) throws IOException, ParseException {
    BufferedReader bufferedReader = new BufferedReader(mappings);
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      String[] synonyms = line.split(synonymsSeparator);
      this.addInternal(synonyms);
    }
  }

  public void addInternal(String[] synonyms) throws IOException {
    String[] inputStrings;
    CharsRef[] inputs;
    int i;

    for (String synonym : synonyms) {
      inputStrings = split(synonym, SYNONYM_MAPPING_SEPARATOR);

      if (inputStrings.length != 2) {
        throw new IllegalArgumentException("synonym mapping is invalid for " + synonym);
      }
      inputs = new CharsRef[inputStrings.length];

      for (i = 0; i < inputs.length; ++i) {
        inputs[i] = this.analyze(this.unescape(inputStrings[i]).trim(), new CharsRefBuilder());
      }

      if (!this.expand) {
        for (i = 0; i < inputs.length; ++i) {
          this.add(inputs[i], inputs[0], false);
        }
      } else {
        for (i = 0; i < inputs.length; ++i) {
          for (int j = 0; j < inputs.length; ++j) {
            if (i != j) {
              this.add(inputs[i], inputs[j], true);
            }
          }
        }
      }
    }
  }

  private static String[] split(String s, String separator) {
    ArrayList<String> list = new ArrayList(2);
    StringBuilder sb = new StringBuilder();
    int pos = 0;
    int end = s.length();

    while (pos < end) {
      if (s.startsWith(separator, pos)) {
        if (sb.length() > 0) {
          list.add(sb.toString());
          sb = new StringBuilder();
        }

        pos += separator.length();
      } else {
        char ch = s.charAt(pos++);
        if (ch == '\\') {
          sb.append(ch);
          if (pos >= end) {
            break;
          }

          ch = s.charAt(pos++);
        }

        sb.append(ch);
      }
    }

    if (sb.length() > 0) {
      list.add(sb.toString());
    }

    return list.toArray(new String[list.size()]);
  }

  private String unescape(String s) {
    if (s.indexOf("\\") < 0) {
      return s;
    } else {
      StringBuilder sb = new StringBuilder();

      for (int i = 0; i < s.length(); ++i) {
        char ch = s.charAt(i);
        if (ch == '\\' && i < s.length() - 1) {
          ++i;
          sb.append(s.charAt(i));
        } else {
          sb.append(ch);
        }
      }

      return sb.toString();
    }
  }
}
