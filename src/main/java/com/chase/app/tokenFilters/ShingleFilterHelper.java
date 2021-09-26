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
import org.apache.lucene.analysis.miscellaneous.LengthFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;

public final class ShingleFilterHelper {
  public static TokenStream UseTrigramFilter(TokenStream in)
    {
      final ShingleFilter sf = new ShingleFilter(in, 3);
      sf.setTokenSeparator("");
      sf.setOutputUnigrams(true);
      return sf;
    }

    public static TokenStream UseTrigramLengthLimitFilter(TokenStream in)
    {
      return new LengthFilter(in, 1, 20);
    }
}