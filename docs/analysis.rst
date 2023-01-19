Analyzers
==========================

Nrtsearch provides some default analyzers, supports specifying all analyzers in Lucene and also supports building custom analyzers.

Proto definition for Analyzer:

.. code-block::

  message Analyzer {
      oneof AnalyzerType {
          string predefined = 1; // Analyzers predefined in Lucene, apart from standard and classic there are en.English, bn.Bengali, eu.Basque, etc. (names derived from Lucene's analyzer class names)
          CustomAnalyzer custom = 2;
      }
  }

To use a predefined analyzer you need to provide the name of the analyzer in ``predefined``. To create a custom analyzer you need to provide your analyzer in the ``custom`` field.

Predefined Analyzers
-----------------------------

Following are the predefined analyzers available in Nrtsearch. These are derived from the analyzer classes available in `Lucene <https://lucene.apache.org/core/8_4_0/analyzers-common/index.html>`_.

  * ar.Arabic

  * hy.Armenian

  * eu.Basque

  * bn.Bengali

  * br.Brazilian

  * bg.Bulgarian

  * ca.Catalan

  * cjk.CJK

  * standard.Classic

  * cz.Czech

  * da.Danish

  * nl.Dutch

  * en.English

  * et.Estonian

  * fi.Finnish

  * fr.French

  * gl.Galician

  * de.German

  * el.Greek

  * hi.Hindi

  * hu.Hungarian

  * id.Indonesian

  * ga.Irish

  * it.Italian

  * lv.Latvian

  * lt.Lithuanian

  * no.Norwegian

  * fa.Persian

  * pt.Portuguese

  * ro.Romanian

  * ru.Russian

  * ckb.Sorani

  * es.Spanish

  * core.Stop

  * sv.Swedish

  * th.Thai

  * tr.Turkish

  * standard.UAX29URLEmail

  * core.UnicodeWhitespace

  * core.Whitespace

  * query.QueryAutoStopWord

  * miscellaneous.LimitTokenCount


Building Custom Analyzers
-----------------------------

This is the proto definition for a ``CustomAnalyzer``:

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
      IntObject positionIncrementGap = 6; // Must be >= 0
      IntObject offsetGap = 7; // Must be >= 0
  }

A custom analyzer is created by combining different character filters, tokenizer and token filters. Each of these perform different functions in analysis:

  * Character filter: change or remove characters from input text. Executed first during analysis.

  * Tokenizer: consume a stream of characters and output tokens. Executed after all character filters.

  * Token filter: change or remove tokens from token stream. Executed after tokenizer.

The API also lets you provide ``map<string, string> params`` for every character filter, tokenizer or token filter which can be used to override some default parameters for them.

Available character filters:

  * htmlstrip

  * mapping

  * persian

  * patternreplace

Available tokenizers:

  * keyword

  * letter

  * whitespace

  * edgengram

  * ngram

  * pathhierarchy

  * pattern

  * simplepatternsplit

  * simplepattern

  * classic

  * standard

  * uax29urlemail

  * thai

  * wikipedia.

Available token filters:

  * suggestStop

  * apostrophe

  * arabicNormalization

  * arabicStem

  * bulgarianStem

  * bengaliNormalization

  * bengaliStem

  * brazilianStem

  * cjkBigram

  * cjkWidth

  * soraniNormalization

  * soraniStem

  * commonGrams

  * commonGramsQuery

  * dictionaryCompoundWord

  * hyphenationCompoundWord

  * decimalDigit

  * lowercase

  * stop

  * type

  * uppercase

  * czechStem

  * germanLightStem

  * germanMinimalStem

  * germanNormalization

  * germanStem

  * greekLowercase

  * greekStem

  * englishMinimalStem

  * englishPossessive

  * kStem

  * porterStem

  * spanishLightStem

  * spanishMinimalStem

  * persianNormalization

  * finnishLightStem

  * frenchLightStem

  * frenchMinimalStem

  * irishLowercase

  * galicianMinimalStem

  * galicianStem

  * hindiNormalization

  * hindiStem

  * hungarianLightStem

  * hunspellStem

  * indonesianStem

  * indicNormalization

  * italianLightStem

  * latvianStem

  * minHash

  * asciiFolding

  * capitalization

  * codepointCount

  * concatenateGraph

  * dateRecognizer

  * delimitedTermFrequency

  * fingerprint

  * fixBrokenOffsets

  * hyphenatedWords

  * keepWord

  * keywordMarker

  * keywordRepeat

  * length

  * limitTokenCount

  * limitTokenOffset

  * limitTokenPosition

  * removeDuplicates

  * stemmerOverride

  * protectedTerm

  * trim

  * truncate

  * typeAsSynonym

  * wordDelimiter

  * wordDelimiterGraph

  * scandinavianFolding

  * scandinavianNormalization

  * edgeNGram

  * nGram

  * norwegianLightStem

  * norwegianMinimalStem

  * patternReplace

  * patternCaptureGroup

  * delimitedPayload

  * numericPayload

  * tokenOffsetPayload

  * typeAsPayload

  * portugueseLightStem

  * portugueseMinimalStem

  * portugueseStem

  * reverseString

  * russianLightStem

  * shingle

  * fixedShingle

  * snowballPorter

  * serbianNormalization

  * classic

  * swedishLightStem

  * synonym

  * synonymGraph

  * flattenGraph

  * turkishLowercase

  * elision


