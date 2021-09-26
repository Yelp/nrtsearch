/*
 * Copyright 2020 Chase Labs Inc.
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
package com.chase.app.tokenFilters;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;

public final class WordDelimiterGraphFilterHelper {

    final private static int _configurationFlags = WordDelimiterGraphFilter.GENERATE_NUMBER_PARTS |
      WordDelimiterGraphFilter.GENERATE_WORD_PARTS |
      WordDelimiterGraphFilter.SPLIT_ON_CASE_CHANGE |
      WordDelimiterGraphFilter.SPLIT_ON_NUMERICS |
      WordDelimiterGraphFilter.STEM_ENGLISH_POSSESSIVE;
    public static TokenStream UseDefaultWordDelimiterGraphSettings(TokenStream in)
    {
      return new WordDelimiterGraphFilter(in, _configurationFlags, null);
    }
  }