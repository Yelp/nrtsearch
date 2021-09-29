Analyzers
==========================

Proto definition:

.. code-block::

  message NameAndParams {
      string name = 1;
      map<string, string> params = 2;
  }

  message ConditionalTokenFilter {
      NameAndParams condition = 1;
      repeated NameAndParams tokenFilters = 2;
  }

  // Used to be able to check if a value was set
  message IntObject {
      int32 int = 1;
  }

  message CustomAnalyzer {
      repeated NameAndParams charFilters = 1; // Available char filters as of Lucene 8.2.0: htmlstrip, mapping, persian, patternreplace
      NameAndParams tokenizer = 2; // Specify a Lucene tokenizer (https://lucene.apache.org/core/8_2_0/core/org/apache/lucene/analysis/Tokenizer.html). Possible options as of Lucene 8.2.0: keyword, letter, whitespace, edgengram, pathhierarchy, pattern, simplepatternsplit, classic, standard, uax29urlemail, thai, wikipedia.
      repeated NameAndParams tokenFilters = 3; // Specify a Lucene token filter (https://lucene.apache.org/core/8_2_0/core/org/apache/lucene/analysis/TokenFilter.html). The possible options can be seen at https://lucene.apache.org/core/8_2_0/analyzers-common/org/apache/lucene/analysis/util/TokenFilterFactory.html or by calling TokenFilterFactory.availableTokenFilters().
      repeated ConditionalTokenFilter conditionalTokenFilters = 4; // TODO: this is not properly supported yet, the only impl requires a protected terms file. Can support this properly later if needed
      string defaultMatchVersion = 5; // Lucene version as LUCENE_X_Y_Z or X.Y.Z, LATEST by default
      IntObject positionIncrementGap = 6;
      IntObject offsetGap = 7;
  }

  message Analyzer {
      oneof AnalyzerType {
          string predefined = 1; // Analyzers predefined in Lucene, apart from standard and classic there are en.English, bn.Bengali, eu.Basque, etc. (names derived from Lucene's analyzer class names)
          CustomAnalyzer custom = 2;
      }
  }