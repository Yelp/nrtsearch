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
package com.chase.app.analyzers;

import com.chase.app.tokenFilters.ShingleFilterHelper;
import com.chase.app.tokenFilters.WordDelimiterGraphFilterHelper;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

public class TextAggressiveDelimitionAnalyzer extends Analyzer {
  @Override
  protected TokenStreamComponents createComponents(String fieldName) {
    final Tokenizer src = new WhitespaceTokenizer();

    TokenStream res = WordDelimiterGraphFilterHelper.UseDefaultWordDelimiterGraphSettings(src);
    res = new LowerCaseFilter(res);
    res = new ASCIIFoldingFilter(res);
    res = ShingleFilterHelper.UseTrigramFilter(res);
    // TODO why did we decide to use trigram in aggressive delimiter? isnt is enough to prefix on
    // basic (whitespace + "-") analyzer
    // anyway this can create strange behavior (e.g. "bla.com/path+level+1" and someone looking for
    // "compa"..)

    return new TokenStreamComponents(src, res);
  }
}
