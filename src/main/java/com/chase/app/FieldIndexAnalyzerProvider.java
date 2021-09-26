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
package com.chase.app;

import com.chase.app.analyzers.HighlightingPrefixAnalyzer;
import com.chase.app.analyzers.PathLiteDelimitionAnalyzer;
import com.chase.app.analyzers.PathLiteDelimitionPrefixAnalyzer;
import com.chase.app.analyzers.TextAggressiveDelimitionAnalyzer;
import com.chase.app.analyzers.TextAggressiveDelimitionPrefixAnalyzer;
import com.chase.app.analyzers.TextBasicAnalyzer;
import com.chase.app.analyzers.TextLiteDelimitionAnalyzer;
import com.chase.app.analyzers.TextPrefixAnalyzer;
import com.chase.app.analyzers.TextShingleAnalyzer;
import com.chase.app.analyzers.TextStemmingAnalyzer;
import com.chase.app.analyzers.TypeAggressiveDelimitionAnalyzer;
import com.chase.app.analyzers.TypeLiteDelimitionAnalyzer;
import com.chase.app.analyzers.TypePrefixAnalyzer;
import java.util.Arrays;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class FieldIndexAnalyzerProvider {
  public static Analyzer GetIndexAnalyzer(String key) {
    return GetIndexAnalyzer(key, null);
  }

  public static Analyzer GetIndexAnalyzer(String key, String extension) {
    switch (key) {
      case IndexedFieldsNames.TYPE:
        switch (extension) {
          case IndexedFieldsExtensionsNames.LITE_DELIMITION:
            return new TypeLiteDelimitionAnalyzer();
          case IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION:
            return new TypeAggressiveDelimitionAnalyzer();
          case IndexedFieldsExtensionsNames.PREFIXES:
            return new TypePrefixAnalyzer();
        }
        break;

      case IndexedFieldsNames.CONTENT:
        if (extension == null) {
          return new StandardAnalyzer();
        }
        switch (extension) {
          case IndexedFieldsExtensionsNames.STEMMING:
            return new TextStemmingAnalyzer();
        }
        break;

      case IndexedFieldsNames.NAME:
      case IndexedFieldsNames.TEXT:
        if (extension == null) {
          return new TextBasicAnalyzer();
        }
        switch (extension) {
          case IndexedFieldsExtensionsNames.LITE_DELIMITION:
            return new TextLiteDelimitionAnalyzer();
          case IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION:
            return new TextAggressiveDelimitionAnalyzer();
          case IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX:
            return new TextAggressiveDelimitionPrefixAnalyzer();
          case IndexedFieldsExtensionsNames.SHINGLES:
            return new TextShingleAnalyzer();
          case IndexedFieldsExtensionsNames.PREFIXES:
            return new TextPrefixAnalyzer();
          case IndexedFieldsExtensionsNames.STEMMING:
            return new TextStemmingAnalyzer();
          case IndexedFieldsExtensionsNames.KEYWORD:
            return new KeywordAnalyzer();
        }
        break;

      case IndexedFieldsNames.HIGHLIGHT:
        return new HighlightingPrefixAnalyzer();

      case IndexedFieldsNames.PATHS:
        if (extension == null) {
          return new KeywordAnalyzer();
        }
        switch (extension) {
          case IndexedFieldsExtensionsNames.LITE_DELIMITION:
            return new PathLiteDelimitionAnalyzer();
          case IndexedFieldsExtensionsNames.LITE_DELIMITION_PREFIX:
            return new PathLiteDelimitionPrefixAnalyzer();
          case IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION:
            return new TextAggressiveDelimitionAnalyzer();
          case IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX:
            return new TextAggressiveDelimitionPrefixAnalyzer();
        }
        break;

        // TODO analyzedTraits should get an analyzer

      default:
        return null; // fields that are not analyzed at all
    }
    return null;
  }

  public static void SetAnalyzer(String key, Map<String, Analyzer> map) throws Exception {
    switch (key) {
      case IndexedFieldsNames.APP_ID:
        // TODO how to set normalizer + split on white space in order to lookup on this?
        break;

      case IndexedFieldsNames
          .TYPE: // TODO how to set normalizer + split on white space in order to lookup on this?
        Arrays.asList(
                IndexedFieldsExtensionsNames.LITE_DELIMITION,
                IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION,
                IndexedFieldsExtensionsNames.PREFIXES)
            .forEach(ext -> map.put(String.format("%s.%s", key, ext), GetIndexAnalyzer(key, ext)));

        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION), new
        // TypeLiteDelimitionAnalyzer());
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION),
        // new TypeAggressiveDelimitionAnalyzer());
        // // ---> todo // map.put(String.format("%s.%s", key,
        // IndexedFieldsExtensionsNames.PREFIXES), new TypePrefixAnalyzer());
        break;

      case IndexedFieldsNames.CONTENT:
        map.put(key, GetIndexAnalyzer(key));
        Arrays.asList(IndexedFieldsExtensionsNames.STEMMING)
            .forEach(ext -> map.put(String.format("%s.%s", key, ext), GetIndexAnalyzer(key, ext)));

        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.STEMMING), new
        // TextStemmingAnalyzer());
        break;

      case IndexedFieldsNames.NAME:
        map.put(key, GetIndexAnalyzer(key));

        Arrays.asList(
                IndexedFieldsExtensionsNames.LITE_DELIMITION,
                IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION,
                IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX,
                IndexedFieldsExtensionsNames.SHINGLES,
                IndexedFieldsExtensionsNames.PREFIXES,
                IndexedFieldsExtensionsNames.STEMMING,
                IndexedFieldsExtensionsNames.KEYWORD)
            .forEach(ext -> map.put(String.format("%s.%s", key, ext), GetIndexAnalyzer(key, ext)));

        // map.put(key, new TextBasicAnalyzer());
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION), new
        // TextLiteDelimitionAnalyzer());
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION),
        // new TextAggressiveDelimitionAnalyzer());
        // // ---> todo //map.put(String.format("%s.%s", key,
        // IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX), new
        // TextAggressiveDelimitionPrefixAnalyzer());  // TODO how to configure search analyzer?
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.SHINGLES), new
        // TextShingleAnalyzer());
        // // ---> todo //map.put(String.format("%s.%s", key,
        // IndexedFieldsExtensionsNames.PREFIXES), new TextPrefixAnalyzer()); // TODO search
        // analyzer?
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.STEMMING), new
        // TextStemmingAnalyzer());
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.KEYWORD), new
        // KeywordAnalyzer());
        break;
      case IndexedFieldsNames.TEXT:
        map.put(key, GetIndexAnalyzer(key));

        Arrays.asList(
                IndexedFieldsExtensionsNames.LITE_DELIMITION,
                IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION,
                IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX,
                IndexedFieldsExtensionsNames.SHINGLES,
                IndexedFieldsExtensionsNames.PREFIXES,
                IndexedFieldsExtensionsNames.STEMMING)
            .forEach(ext -> map.put(String.format("%s.%s", key, ext), GetIndexAnalyzer(key, ext)));

        // map.put(key, new TextBasicAnalyzer());
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION), new
        // TextLiteDelimitionAnalyzer());
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION),
        // new TextAggressiveDelimitionAnalyzer());
        // // ---> todo //map.put(String.format("%s.%s", key,
        // IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX), new
        // TextAggressiveDelimitionPrefixAnalyzer());  // TODO how to configure search analyzer?
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.SHINGLES), new
        // TextShingleAnalyzer());
        // // ---> todo //map.put(String.format("%s.%s", key,
        // IndexedFieldsExtensionsNames.PREFIXES), new TextPrefixAnalyzer()); // TODO search
        // analyzer?
        // map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.STEMMING), new
        // TextStemmingAnalyzer());
        break;

      case IndexedFieldsNames.HIGHLIGHT:
        map.put(
            key,
            GetIndexAnalyzer(
                key)); // TODO removed as it is a prefix analyzer and we can search without it

        //// ---> todo //map.put(key, new HighlightingPrefixAnalyzer()); // TODO search analyzer?
        break;

      case IndexedFieldsNames.PATHS:
        map.put(key, GetIndexAnalyzer(key));

        Arrays.asList(
                IndexedFieldsExtensionsNames.LITE_DELIMITION,
                IndexedFieldsExtensionsNames.LITE_DELIMITION_PREFIX,
                IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION,
                IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX)
            .forEach(ext -> map.put(String.format("%s.%s", key, ext), GetIndexAnalyzer(key, ext)));

        //     map.put(String.format("%s.%s", key, IndexedFieldsExtensionsNames.LITE_DELIMITION),
        // new PathLiteDelimitionAnalyzer());
        //     // ---> todo //map.put(String.format("%s.%s", key,
        // IndexedFieldsExtensionsNames.LITE_DELIMITION_PREFIX), new
        // PathLiteDelimitionPrefixAnalyzer()); // TODO search analyzer
        //     map.put(String.format("%s.%s", key,
        // IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION), new
        // TextAggressiveDelimitionAnalyzer());
        //    // ---> todo // map.put(String.format("%s.%s", key,
        // IndexedFieldsExtensionsNames.AGGRESSIVE_DELIMITION_PREFIX), new
        // TextAggressiveDelimitionPrefixAnalyzer()); // TODO search analyzer
        break;

        // TODO analyzedTraits should get an analyzer
      default:
        throw new Exception(String.format("Unsupported field key: %s", key));
    }
  }
}
