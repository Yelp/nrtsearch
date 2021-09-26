package com.chase.app.analyzers;

import com.chase.app.tokenFilters.EdgeNgramFilterHelper;
import com.chase.app.tokenizers.PathsTokenizer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.Analyzer.TokenStreamComponents;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

public class PathLiteDelimitionPrefixAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new PathsTokenizer();
      
      TokenStream res = new LowerCaseFilter(src);
      res = new ASCIIFoldingFilter(res);
      res = EdgeNgramFilterHelper.UsePrefixEdgeNgramFilter(res);
      
      return new TokenStreamComponents(src, res);
    }
  }