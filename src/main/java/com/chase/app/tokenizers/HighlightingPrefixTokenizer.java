/*
 * Copyright 2021 Yelp Inc.
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
package com.chase.app.tokenizers;

import org.apache.lucene.analysis.ngram.EdgeNGramTokenizer;
import org.apache.lucene.util.AttributeFactory;

// like
// https://github.com/chaseappio/backend-resources-service/blob/dev/src/Chase.Resources.Elastic/Template/Tokenizers/PathTokenizer.cs
public class HighlightingPrefixTokenizer extends EdgeNGramTokenizer {

  private static final int MINGRAM_SIZE = 3;
  private static final int MAXGRAM_SIZE = 20;

  public HighlightingPrefixTokenizer() {
    super(MINGRAM_SIZE, MAXGRAM_SIZE);
  }

  public HighlightingPrefixTokenizer(AttributeFactory factory) {
    super(factory, MINGRAM_SIZE, MAXGRAM_SIZE);
  }

  @Override
  protected boolean isTokenChar(int c) {
    // Support letters, numbers, punctuations or symbols. According to
    // https://github.com/elastic/elasticsearch/blob/a92a647b9f17d1bddf5c707490a19482c273eda3/modules/analysis-common/src/main/java/org/elasticsearch/analysis/common/CharMatcher.java

    if (Character.isLetter(c) || Character.isDigit(c)) return true;

    int ctype = Character.getType(c);
    return ctype == Character.START_PUNCTUATION
        || ctype == Character.END_PUNCTUATION
        || ctype == Character.OTHER_PUNCTUATION
        || ctype == Character.CONNECTOR_PUNCTUATION
        || ctype == Character.DASH_PUNCTUATION
        || ctype == Character.INITIAL_QUOTE_PUNCTUATION
        || ctype == Character.FINAL_QUOTE_PUNCTUATION
        || ctype == Character.CURRENCY_SYMBOL
        || ctype == Character.MATH_SYMBOL
        || ctype == Character.OTHER_SYMBOL
        || ctype == Character.MODIFIER_SYMBOL;
  }
}
