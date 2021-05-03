package com.chase.app.analyzers;

import com.chase.app.tokenizers.HighlightingPrefixTokenizer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;

public class HighlightingPrefixAnalyzer extends Analyzer {
    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
      final Tokenizer src = new HighlightingPrefixTokenizer();
      
      TokenStream res = new LowerCaseFilter(src);
      res = new ASCIIFoldingFilter(res);
      
      return new TokenStreamComponents(src, res);
    }
  }
